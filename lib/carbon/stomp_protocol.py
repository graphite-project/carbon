from twisted.internet import defer, reactor, task
from twisted.internet.protocol import Protocol
from stompest.protocol import StompFailoverTransport, StompParser
from stompest.error import StompConnectionError, StompFrameError, StompCancelledError, StompProtocolError
from stompest.async.util import WaitingDeferred, InFlightOperations, sendToErrorDestination
from stompest.protocol import StompSession, StompSpec
from stompest.async import util
from stompest.config import StompConfig
from stompest.util import checkattr
from carbon.conf import settings
from carbon import stomp_listener
from carbon import log, instrumentation


SEND_QUEUE_LOW_WATERMARK = settings.MAX_QUEUE_SIZE * 0.8
activeConnection = checkattr('connected')

class StompClientProtocol(Protocol):
  @property
  def connected(self):
    try:
      connected = self.__connected
    except AttributeError:
      self.connected = False
      return self.connected
    if not connected:
      raise StompConnectionError('Not connected')
    return connected

  @connected.setter
  def connected(self, connected):
    self.__connected = connected

  def __init__(self, factory):
    self.factory = factory
    self.paused = False
    # Define internal metric names
    self.destinationName = self.factory.destinationName
    self.queuedUntilReady = 'destinations.%s.queuedUntilReady' % self.destinationName
    self.sent = 'destinations.%s.sent' % self.destinationName
    self.relayMaxQueueLength = 'destinations.%s.relayMaxQueueLength' % self.destinationName
    # Define stomp related variables
    self.parser = StompParser()
    self.config = StompConfig(self.factory.uri)
    self.session = StompSession(self.config.version, self.config.check)
    self.listenersFactory = stomp_listener.defaultListeners
    self.listeners = []
    self.handlers = {'MESSAGE': self._onMessage, 'CONNECTED': self._onConnected, 'ERROR': self._onError, 'RECEIPT': self._onReceipt}
 
  def pauseProducing(self):
    self.paused = True

  def resumeProducing(self):
    self.paused = False
    self.sendQueued()

  def stopProducing(self):
    self.disconnect()

  @util.exclusive
  @defer.inlineCallbacks
  def connect(self, headers=None, versions=None, host=None, heartBeats=None, connectTimeout=None, connectedTimeout=None):
    frame = self.session.connect(self.factory.username, self.factory.password, headers, versions, host, heartBeats)

    for listener in self.listenersFactory():
      self.add(listener)

    try:
      self._sendFrame(frame)
      yield self._notify(lambda l: l.onConnect(self, frame, connectedTimeout))
    except Exception as e:
      self.disconnect(failure=e)

    yield self._replay()
    defer.returnValue(self)

  def connectionMade(self):
    log.clients("%s::connectionMade" % self)
    self.transport.registerProducer(self, streaming=True)
    self.connect()

  def connectedQueue(self):
    self.connected = True
    self.add(stomp_listener.ReceiptListener())

    self.factory.connectionMade.callback(self)
    self.factory.connectionMade = defer.Deferred()
    self.sendQueued()
 
  def dataReceived(self, data):
    log.debug('Received data: %s' % repr(data))
    self.parser.add(data)
    for frame in iter(self.parser.get, self.parser.SENTINEL):
      log.debug('Received %s' % frame.info())
      try:
        self._onFrame(frame)
      except Exception as e:
        log.clients("%s::unhandledError Frame handler: %s" % (self, e))

  @defer.inlineCallbacks
  def sendDatapoint(self, metric, datapoint):
    frame = self.session.send(self.factory.destination['QUEUE'], "{metric:%s, datapoint:%s}" % (metric, datapoint), None, None)
    instrumentation.max(self.relayMaxQueueLength, len(self.factory.queue))
    if self.paused:
      self.factory.enqueue(metric, datapoint)
      instrumentation.increment(self.queuedUntilReady)
    elif self.factory.hasQueuedDatapoints():
      self.factory.enqueue(metric, datapoint)
      self.sendQueued()
    else:
      yield self._sendFrame(frame)

  def sendDatapoints(self, datapoints):
    for point in datapoints:
      metric, datapoint = point
      self.sendDatapoint(metric, datapoint)

  def sendQueued(self):
    while (not self.paused) and self.factory.hasQueuedDatapoints():
      datapoints = self.factory.takeSomeFromQueue()
      self.sendDatapoints(datapoints)
      queueSize = self.factory.queueSize
      if (self.factory.queueFull.called and
          queueSize < SEND_QUEUE_LOW_WATERMARK):
        self.factory.queueHasSpace.callback(queueSize)

  @defer.inlineCallbacks
  def _sendFrame(self, frame):
    self._send(frame)
    yield self._notify(lambda l: l.onSend(self, frame))

  @activeConnection
  def _send(self, frame):
    log.debug('Sending %s' % frame.info())
    self.transport.write(str(frame))

  @activeConnection
  @defer.inlineCallbacks
  def disconnect(self, receipt=None, failure=None, timeout=None):
    self.connected = False
    try:
      yield self._notify(lambda l: l.onDisconnect(self, failure, timeout))
    except Exception as e:
      self.disconnect(failure=e)

    try:
      if (self.session.state == self.session.CONNECTED):
        yield self._sendFrame(self.session.disconnect(receipt))
    except Exception as e:
      self.disconnect(failure=e)
    finally:
      self.loseConnection()

  def loseConnection(self):
    self.transport.loseConnection()

  def stopProducing(self):
    self.disconnect()
  
  def connectionLost(self, reason):
    self.connected = False
    log.clients("%s::connectionLost %s" % (self, reason.getErrorMessage()))
    try:
      self._onConnectionLost(reason)
    finally:
      Protocol.connectionLost(self, reason)

  def add(self, listener):
    if listener not in self.listeners:
      log.debug('adding listener: %s' % listener)
      self.listeners.append(listener)
      listener.onAdd(self)

  def remove(self, listener):
    log.debug('removing listener: %s' % listener)
    self.listeners.remove(listener)

  def setVersion(self, version):
    self.parser.version = version

  @activeConnection
  @defer.inlineCallbacks
  def ack(self, frame, receipt=None):
    frame = self.session.ack(frame, receipt)
    yield self._sendFrame(frame)

  @activeConnection
  @defer.inlineCallbacks
  def nack(self, frame, receipt=None):
    frame = self.session.nack(frame, receipt)
    yield self._sendFrame(frame)

  @activeConnection
  @defer.inlineCallbacks
  def begin(self, transaction=None, receipt=None):
    frame = self.session.begin(transaction, receipt)
    yield self._sendFrame(frame)

  @activeConnection
  @defer.inlineCallbacks
  def abort(self, transaction=None, receipt=None):
    frame = self.session.abort(transaction, receipt)
    yield self._sendFrame(frame)

  @activeConnection
  @defer.inlineCallbacks
  def commit(self, transaction=None, receipt=None):
    frame = self.session.commit(transaction, receipt)
    yield self._sendFrame(frame)

  @activeConnection
  @defer.inlineCallbacks
  def subscribe(self, destination, headers=None, receipt=None, listener=None):
    frame, token = self.session.subscribe(destination, headers, receipt, listener)
    if listener:
      self.add(listener)
    yield self._notify(lambda l: l.onSubscribe(self, frame, l))
    yield self._sendFrame(frame)
    defer.returnValue(token)

  @activeConnection
  @defer.inlineCallbacks
  def unsubscribe(self, token, receipt=None):
    context = self.session.subscription(token)
    frame = self.session.unsubscribe(token, receipt)
    yield self._sendFrame(frame)
    yield self._notify(lambda l: l.onUnsubscribe(self, frame, context))

  @defer.inlineCallbacks
  def _onFrame(self, frame):
    yield self._notify(lambda l: l.onFrame(self, frame))
    if not frame:
      return
    try:
      handler = self.handlers[frame.command]
    except KeyError:
      raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
    yield handler(frame)

  @defer.inlineCallbacks
  def _onConnected(self, frame):
    self.connectedQueue
    self.session.connected(frame)
    log.clients("%s::queueConnected Stomp broker [session=%s, version=%s]" % (self, self.session.id, self.session.version))
    self.setVersion(self.session.version)
    yield self._notify(lambda l: l.onConnected(self, frame))

  @defer.inlineCallbacks
  def _onError(self, frame):
    yield self._notify(lambda l: l.onError(self, frame))

  @defer.inlineCallbacks
  def _onMessage(self, frame):
    headers = frame.headers
    messageId = headers[StompSpec.MESSAGE_ID_HEADER]

    try:
      token = self.session.message(frame)
    except:
      log.debug('Ignoring message (no handler found): %s [%s]' % (messageId, frame.info()))
      defer.returnValue(None)
    context = self.session.subscription(token)

    try:
      yield self._notify(lambda l: l.onMessage(self, frame, context))
    except Exception as e:
      log.clients('%s::disconnecting (error in message handler): %s [%s]' % (self, messageId, frame.info()))
      self.disconnect(failure=e)

  @defer.inlineCallbacks
  def _onReceipt(self, frame):
    receipt = self.session.receipt(frame)
    yield self._notify(lambda l: l.onReceipt(self, frame, receipt))

  def _notify(self, notify):
    return task.cooperate(notify(listener) for listener in list(self.listeners)).whenDone()

  @defer.inlineCallbacks
  def _onConnectionLost(self, reason):
    self.connected = False
    try:
      yield self._notify(lambda l: l.onConnectionLost(self, reason))
    finally:
      yield self._notify(lambda l: l.onCleanup(self))

  def _replay(self):
    def replay():
      for (destination, headers, receipt, context) in self.session.replay():
        log.clients('%s::replaying Subscription: %s' % (self, headers))
        yield self.subscribe(destination, headers=headers, receipt=receipt, listener=context)
    return task.cooperate(replay()).whenDone()

  def __str__(self):
    return 'StompClientProtocol(%s:%d:%s)' % (self.factory.destination['ADDRESS'])
  __repr__ = __str__
