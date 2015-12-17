from collections import deque
from time import time

from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.basic import Int32StringReceiver
from carbon.conf import settings
from carbon.util import pickle
from carbon import instrumentation, log, pipeline, state

try:
    import signal
except ImportError:
    log.debug("Couldn't import signal module")


SEND_QUEUE_LOW_WATERMARK = settings.MAX_QUEUE_SIZE * settings.QUEUE_LOW_WATERMARK_PCT


class CarbonClientProtocol(Int32StringReceiver):
  def connectionMade(self):
    log.clients("%s::connectionMade" % self)
    self.paused = False
    self.connected = True
    self.transport.registerProducer(self, streaming=True)
    # Define internal metric names
    self.lastResetTime = time()
    self.destinationName = self.factory.destinationName
    self.queuedUntilReady = 'destinations.%s.queuedUntilReady' % self.destinationName
    self.sent = 'destinations.%s.sent' % self.destinationName
    self.batchesSent = 'destinations.%s.batchesSent' % self.destinationName

    self.slowConnectionReset = 'destinations.%s.slowConnectionReset' % self.destinationName

    self.factory.connectionMade.callback(self)
    self.factory.connectionMade = Deferred()
    self.sendQueued()

  def connectionLost(self, reason):
    log.clients("%s::connectionLost %s" % (self, reason.getErrorMessage()))
    self.connected = False

  def pauseProducing(self):
    self.paused = True

  def resumeProducing(self):
    self.paused = False
    self.sendQueued()

  def stopProducing(self):
    self.disconnect()

  def disconnect(self):
    if self.connected:
      self.transport.unregisterProducer()
      self.transport.loseConnection()
      self.connected = False

  def sendDatapoint(self, metric, datapoint):
    self.factory.enqueue(metric, datapoint)
    self.factory.scheduleSend()

  def _sendDatapoints(self, datapoints):
      self.sendString(pickle.dumps(datapoints, protocol=-1))
      instrumentation.increment(self.sent, len(datapoints))
      instrumentation.increment(self.batchesSent)
      self.factory.checkQueue()

  def sendQueued(self):
    """This should be the only method that will be used to send stats.
    In order to not hold the event loop and prevent stats from flowing
    in while we send them out, this will process
    settings.MAX_DATAPOINTS_PER_MESSAGE stats, send them, and if there
    are still items in the queue, this will invoke reactor.callLater
    to schedule another run of sendQueued after a reasonable enough time
    for the destination to process what it has just received.

    Given a queue size of one million stats, and using a
    chained_invocation_delay of 0.0001 seconds, you'd get 1,000
    sendQueued() invocations/second max.  With a
    settings.MAX_DATAPOINTS_PER_MESSAGE of 100, the rate of stats being
    sent could theoretically be as high as 100,000 stats/sec, or
    6,000,000 stats/minute.  This is probably too high for a typical
    receiver to handle.

    In practice this theoretical max shouldn't be reached because
    network delays should add an extra delay - probably on the order
    of 10ms per send, so the queue should drain with an order of
    minutes, which seems more realistic.
    """
    queueSize = self.factory.queueSize

    if self.paused:
      instrumentation.max(self.queuedUntilReady, queueSize)
      return
    if not self.factory.hasQueuedDatapoints():
      return

    if settings.USE_RATIO_RESET is True:
      if not self.connectionQualityMonitor():
        self.resetConnectionForQualityReasons("Sent: {0}, Received: {1}".format(
          instrumentation.prior_stats.get(self.sent, 0),
          instrumentation.prior_stats.get('metricsReceived', 0)))

    self._sendDatapoints(self.factory.takeSomeFromQueue())
    if (self.factory.queueFull.called and
        queueSize < SEND_QUEUE_LOW_WATERMARK):
      if not self.factory.queueHasSpace.called:
        self.factory.queueHasSpace.callback(queueSize)
    if self.factory.hasQueuedDatapoints():
      self.factory.scheduleSend()


  def connectionQualityMonitor(self):
    """Checks to see if the connection for this factory appears to
    be delivering stats at a speed close to what we're receiving
    them at.

    This is open to other measures of connection quality.

    Returns a Bool

    True means that quality is good, OR
    True means that the total received is less than settings.MIN_RESET_STAT_FLOW

    False means that quality is bad

    """
    destination_sent = float(instrumentation.prior_stats.get(self.sent, 0))
    total_received = float(instrumentation.prior_stats.get('metricsReceived', 0))
    instrumentation.increment(self.slowConnectionReset, 0)
    if total_received < settings.MIN_RESET_STAT_FLOW:
      return True

    if (destination_sent / total_received) < settings.MIN_RESET_RATIO:
      return False
    else:
      return True

  def resetConnectionForQualityReasons(self, reason):
    """Only re-sets the connection if it's been
    settings.MIN_RESET_INTERVAL seconds since the last re-set.

    Reason should be a string containing the quality info that led to
    a re-set.
    """
    if (time() - self.lastResetTime) < float(settings.MIN_RESET_INTERVAL):
      return
    else:
      self.factory.connectedProtocol.disconnect()
      self.lastResetTime = time()
      instrumentation.increment(self.slowConnectionReset)
      log.clients("%s:: resetConnectionForQualityReasons: %s" % (self, reason))

  def __str__(self):
    return 'CarbonClientProtocol(%s:%d:%s)' % (self.factory.destination)
  __repr__ = __str__


class CarbonClientFactory(ReconnectingClientFactory):
  maxDelay = 5

  def __init__(self, destination):
    self.destination = destination
    self.destinationName = ('%s:%d:%s' % destination).replace('.', '_')
    self.host, self.port, self.carbon_instance = destination
    self.addr = (self.host, self.port)
    self.started = False
    # This factory maintains protocol state across reconnects
    self.queue = deque() # Change to make this the sole source of metrics to be sent.
    self.connectedProtocol = None
    self.queueEmpty = Deferred()
    self.queueFull = Deferred()
    self.queueFull.addCallback(self.queueFullCallback)
    self.queueHasSpace = Deferred()
    self.queueHasSpace.addCallback(self.queueSpaceCallback)
    self.connectFailed = Deferred()
    self.connectionMade = Deferred()
    self.connectionLost = Deferred()
    self.deferSendPending = None
    # Define internal metric names
    self.attemptedRelays = 'destinations.%s.attemptedRelays' % self.destinationName
    self.fullQueueDrops = 'destinations.%s.fullQueueDrops' % self.destinationName
    self.queuedUntilConnected = 'destinations.%s.queuedUntilConnected' % self.destinationName
    self.relayMaxQueueLength = 'destinations.%s.relayMaxQueueLength' % self.destinationName

  def scheduleSend(self):
    if self.deferSendPending and self.deferSendPending.active():
      return
    self.deferSendPending = reactor.callLater(settings.TIME_TO_DEFER_SENDING, self.sendQueued)

  def sendQueued(self):
    if self.connectedProtocol:
      self.connectedProtocol.sendQueued()

  def queueFullCallback(self, result):
    state.events.cacheFull()
    log.clients('%s send queue is full (%d datapoints)' % (self, result))

  def queueSpaceCallback(self, result):
    if self.queueFull.called:
      log.clients('%s send queue has space available' % self.connectedProtocol)
      self.queueFull = Deferred()
      self.queueFull.addCallback(self.queueFullCallback)
      state.events.cacheSpaceAvailable()
    self.queueHasSpace = Deferred()
    self.queueHasSpace.addCallback(self.queueSpaceCallback)

  def buildProtocol(self, addr):
    self.connectedProtocol = CarbonClientProtocol()
    self.connectedProtocol.factory = self
    return self.connectedProtocol

  def startConnecting(self): # calling this startFactory yields recursion problems
    self.started = True
    self.connector = reactor.connectTCP(self.host, self.port, self)

  def stopConnecting(self):
    self.started = False
    self.stopTrying()
    if self.connectedProtocol and self.connectedProtocol.connected:
      return self.connectedProtocol.disconnect()

  @property
  def queueSize(self):
    return len(self.queue)

  def hasQueuedDatapoints(self):
    return bool(self.queue)

  def takeSomeFromQueue(self):
    """Use self.queue, which is a collections.deque, to pop up to
    settings.MAX_DATAPOINTS_PER_MESSAGE items from the left of the
    queue.
    """
    def yield_max_datapoints():
      for count in range(settings.MAX_DATAPOINTS_PER_MESSAGE):
        try:
          yield self.queue.popleft()
        except IndexError:
          raise StopIteration
    return list(yield_max_datapoints())

  def checkQueue(self):
    """Check if the queue is empty. If the queue isn't empty or
    doesn't exist yet, then this will invoke the callback chain on the
    self.queryEmpty Deferred chain with the argument 0, and will
    re-set the queueEmpty callback chain with a new Deferred
    object.
    """
    if not self.queue:
      self.queueEmpty.callback(0)
      self.queueEmpty = Deferred()

  def enqueue(self, metric, datapoint):
    self.queue.append((metric, datapoint))

  def enqueue_from_left(self, metric, datapoint):
    self.queue.appendleft((metric, datapoint))

  def sendDatapoint(self, metric, datapoint):
    instrumentation.increment(self.attemptedRelays)
    instrumentation.max(self.relayMaxQueueLength, self.queueSize)
    if self.queueSize >= settings.MAX_QUEUE_SIZE:
      if not self.queueFull.called:
        self.queueFull.callback(self.queueSize)
      instrumentation.increment(self.fullQueueDrops)
    else:
      self.enqueue(metric, datapoint)

    if self.connectedProtocol:
      self.scheduleSend()
    else:
      instrumentation.increment(self.queuedUntilConnected)

  def sendHighPriorityDatapoint(self, metric, datapoint):
    """The high priority datapoint is one relating to the carbon
    daemon itself.  It puts the datapoint on the left of the deque,
    ahead of other stats, so that when the carbon-relay, specifically,
    is overwhelmed its stats are more likely to make it through and
    expose the issue at hand.

    In addition, these stats go on the deque even when the max stats
    capacity has been reached.  This relies on not creating the deque
    with a fixed max size.
    """
    instrumentation.increment(self.attemptedRelays)
    self.enqueue_from_left(metric, datapoint)

    if self.connectedProtocol:
      self.scheduleSend()
    else:
      instrumentation.increment(self.queuedUntilConnected)

  def startedConnecting(self, connector):
    log.clients("%s::startedConnecting (%s:%d)" % (self, connector.host, connector.port))

  def clientConnectionLost(self, connector, reason):
    ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    log.clients("%s::clientConnectionLost (%s:%d) %s" % (self, connector.host, connector.port, reason.getErrorMessage()))
    self.connectedProtocol = None
    self.connectionLost.callback(0)
    self.connectionLost = Deferred()

  def clientConnectionFailed(self, connector, reason):
    ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    log.clients("%s::clientConnectionFailed (%s:%d) %s" % (self, connector.host, connector.port, reason.getErrorMessage()))
    self.connectFailed.callback(dict(connector=connector, reason=reason))
    self.connectFailed = Deferred()

  def disconnect(self):
    self.queueEmpty.addCallback(lambda result: self.stopConnecting())
    readyToStop = DeferredList(
      [self.connectionLost, self.connectFailed],
      fireOnOneCallback=True,
      fireOnOneErrback=True)
    self.checkQueue()

    # This can happen if the client is stopped before a connection is ever made
    if (not readyToStop.called) and (not self.started):
      readyToStop.callback(None)

    return readyToStop

  def __str__(self):
    return 'CarbonClientFactory(%s:%d:%s)' % self.destination
  __repr__ = __str__


class CarbonClientManager(Service):
  def __init__(self, router):
    self.router = router
    self.client_factories = {} # { destination : CarbonClientFactory() }

  def startService(self):
    if 'signal' in globals().keys():
      log.debug("Installing SIG_IGN for SIGHUP")
      signal.signal(signal.SIGHUP, signal.SIG_IGN)
    Service.startService(self)
    for factory in self.client_factories.values():
      if not factory.started:
        factory.startConnecting()

  def stopService(self):
    Service.stopService(self)
    return self.stopAllClients()

  def startClient(self, destination):
    if destination in self.client_factories:
      return

    log.clients("connecting to carbon daemon at %s:%d:%s" % destination)
    self.router.addDestination(destination)
    factory = self.client_factories[destination] = CarbonClientFactory(destination)
    connectAttempted = DeferredList(
        [factory.connectionMade, factory.connectFailed],
        fireOnOneCallback=True,
        fireOnOneErrback=True)
    if self.running:
      factory.startConnecting() # this can trigger & replace connectFailed

    return connectAttempted

  def stopClient(self, destination):
    factory = self.client_factories.get(destination)
    if factory is None:
      return

    self.router.removeDestination(destination)
    stopCompleted = factory.disconnect()
    stopCompleted.addCallback(lambda result: self.disconnectClient(destination))
    return stopCompleted

  def disconnectClient(self, destination):
    factory = self.client_factories.pop(destination)
    c = factory.connector
    if c and c.state == 'connecting' and not factory.hasQueuedDatapoints():
      c.stopConnecting()

  def stopAllClients(self):
    deferreds = []
    for destination in list(self.client_factories):
      deferreds.append( self.stopClient(destination) )
    return DeferredList(deferreds)

  def sendDatapoint(self, metric, datapoint):
    for destination in self.router.getDestinations(metric):
      self.client_factories[destination].sendDatapoint(metric, datapoint)

  def sendHighPriorityDatapoint(self, metric, datapoint):
    for destination in self.router.getDestinations(metric):
      self.client_factories[destination].sendHighPriorityDatapoint(metric, datapoint)

  def __str__(self):
    return "<%s[%x]>" % (self.__class__.__name__, id(self))


class RelayProcessor(pipeline.Processor):
  plugin_name = 'relay'

  def process(self, metric, datapoint):
    state.client_manager.sendDatapoint(metric, datapoint)
    return pipeline.Processor.NO_OUTPUT
