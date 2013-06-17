from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.protocol import ReconnectingClientFactory
from carbon.conf import settings
from carbon import log, state, instrumentation


class ClientFactory(ReconnectingClientFactory):
  maxDelay = 5

  def __init__(self, destination):
    self.destination = destination
    self.destinationName = ('%s:%s:%d:%s' % ((self.destination['PROTOCOL'],) + self.destination['ADDRESS'])).replace('.', '_')
    self.host, self.port, self.instance = self.destination['ADDRESS']
    self.uri = 'tcp://%s:%s' % (self.host, self.port)
    self.username, self.password = self.destination['CREDENTIALS']
    self.messagequeue = self.destination['QUEUE']
    self.started = False
    # This factory maintains protocol state across reconnects
    self.queue = [] # including datapoints that still need to be sent
    self.connectedProtocol = None
    self.queueEmpty = Deferred()
    self.queueFull = Deferred()
    self.queueFull.addCallback(self.queueFullCallback)
    self.queueHasSpace = Deferred()
    self.queueHasSpace.addCallback(self.queueSpaceCallback)
    self.connectFailed = Deferred()
    self.connectionMade = Deferred()
    self.connectionLost = Deferred()
    # Define internal metric names
    self.attemptedRelays = 'destinations.%s.attemptedRelays' % self.destinationName
    self.fullQueueDrops = 'destinations.%s.fullQueueDrops' % self.destinationName
    self.queuedUntilConnected = 'destinations.%s.queuedUntilConnected' % self.destinationName

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
    if self.destination['PROTOCOL'] == 'tcp':
      from carbon.protocols import CarbonClientProtocol
      self.connectedProtocol = CarbonClientProtocol()
      self.connectedProtocol.factory = self
    elif self.destination['PROTOCOL'] == 'stomp':
      from carbon.stomp_protocol import StompClientProtocol
      self.connectedProtocol = StompClientProtocol(self)
    else:
      raise ValueError("Invalid protocol in destination string \"%s\"" % self.destination['PROTOCOL'])
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
    datapoints = self.queue[:settings.MAX_DATAPOINTS_PER_MESSAGE]
    self.queue = self.queue[settings.MAX_DATAPOINTS_PER_MESSAGE:]
    return datapoints

  def checkQueue(self):
    if not self.queue:
      self.queueEmpty.callback(0)
      self.queueEmpty = Deferred()

  def enqueue(self, metric, datapoint):
    self.queue.append((metric, datapoint))

  def sendDatapoint(self, metric, datapoint):
    instrumentation.increment(self.attemptedRelays)
    queueSize = self.queueSize
    if queueSize >= settings.MAX_QUEUE_SIZE:
      if not self.queueFull.called:
        self.queueFull.callback(queueSize)
      instrumentation.increment(self.fullQueueDrops)
    elif self.connectedProtocol:
      self.connectedProtocol.sendDatapoint(metric, datapoint)
    else:
      self.enqueue(metric, datapoint)
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
    return 'ClientFactory(%s:%s:%d:%s)' % ((self.destination['PROTOCOL'],) + self.destination['ADDRESS'])
  __repr__ = __str__


class ClientManager(Service):
  def __init__(self, router):
    self.router = router
    self.client_factories = {} # { destination['ADDRESS'] : ClientFactory() }

  def startService(self):
    Service.startService(self)
    for factory in self.client_factories.values():
      if not factory.started:
        factory.startConnecting()

  def stopService(self):
    Service.stopService(self)
    self.stopAllClients()

  def startClient(self, destination):
    if destination['ADDRESS'] in self.client_factories:
      return

    if destination['PROTOCOL'] == 'tcp':
      log.clients("connecting to carbon daemon at %s:%d:%s" % destination['ADDRESS'])
    elif destination['PROTOCOL'] == 'stomp':
      log.clients("connecting to message queue %s at %s:%d:%s" % ((destination['QUEUE'],) + destination['ADDRESS']))
    else:
      raise ValueError("Invalid protocol in destination string \"%s\"" % destination['PROTOCOL'])

    self.router.addDestination(destination['ADDRESS'])
    factory = self.client_factories[destination['ADDRESS']] = ClientFactory(destination)

    connectAttempted = DeferredList(
        [factory.connectionMade, factory.connectFailed],
        fireOnOneCallback=True,
        fireOnOneErrback=True)
    if self.running:
      factory.startConnecting() # this can trigger & replace connectFailed

    return connectAttempted

  def stopClient(self, destination_address):
    factory = self.client_factories.get(destination_address)
    if factory is None:
      return

    self.router.removeDestination(destination_address)
    stopCompleted = factory.disconnect()
    stopCompleted.addCallback(lambda result: self.disconnectClient(destination_address))
    return stopCompleted

  def disconnectClient(self, destination_address):
    factory = self.client_factories.pop(destination_address)
    c = factory.connector
    if c and c.state == 'connecting' and not factory.hasQueuedDatapoints():
      c.stopConnecting()

  def stopAllClients(self):
    deferreds = []
    for destination_address in list(self.client_factories):
      deferreds.append( self.stopClient(destination_address) )
    return DeferredList(deferreds)

  def sendDatapoint(self, metric, datapoint):
    for destination_address in self.router.getDestinations(metric):
      self.client_factories[destination_address].sendDatapoint(metric, datapoint)

  def __str__(self):
    return "<%s[%x]>" % (self.__class__.__name__, id(self))