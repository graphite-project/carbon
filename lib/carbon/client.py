from collections import deque, defaultdict
from time import time
from six import with_metaclass

from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver

from carbon.conf import settings
from carbon.util import pickle
from carbon.util import PluginRegistrar, TaggedSeries
from carbon.util import enableTcpKeepAlive
from carbon import instrumentation, log, pipeline, state

try:
    from OpenSSL import SSL
except ImportError:
    SSL = None
try:
    from twisted.internet import ssl
except ImportError:
    ssl = None

try:
    import signal
except ImportError:
    log.debug("Couldn't import signal module")

try:
    from carbon.resolver import setUpRandomResolver
except ImportError:
    setUpRandomResolver = None


SEND_QUEUE_LOW_WATERMARK = settings.MAX_QUEUE_SIZE * settings.QUEUE_LOW_WATERMARK_PCT


class CarbonClientProtocol(object):

  def connectionMade(self):
    log.clients("%s::connectionMade" % self)
    self.paused = False
    self.connected = True
    self.transport.registerProducer(self, streaming=True)
    # Define internal metric names
    self.lastResetTime = time()
    self.destination = self.factory.destination
    self.destinationName = self.factory.destinationName
    self.queuedUntilReady = 'destinations.%s.queuedUntilReady' % self.destinationName
    self.sent = 'destinations.%s.sent' % self.destinationName
    self.batchesSent = 'destinations.%s.batchesSent' % self.destinationName

    self.slowConnectionReset = 'destinations.%s.slowConnectionReset' % self.destinationName
    enableTcpKeepAlive(self.transport, settings.TCP_KEEPALIVE, settings)

    d = self.factory.connectionMade
    # Setup a new deferred before calling the callback to allow callbacks
    # to re-register themselves.
    self.factory.connectionMade = Deferred()
    d.callback(self)

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

  def _sendDatapointsNow(self, datapoints):
    """Implement this function to actually send datapoints."""
    raise NotImplementedError()

  def sendDatapointsNow(self, datapoints):
    self._sendDatapointsNow(datapoints)
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

    if not self.connectionQualityMonitor():
      self.resetConnectionForQualityReasons("Sent: {0}, Received: {1}".format(
        instrumentation.prior_stats.get(self.sent, 0),
        instrumentation.prior_stats.get('metricsReceived', 0)))

    self.sendDatapointsNow(self.factory.takeSomeFromQueue())
    if (self.factory.queueFull.called and queueSize < SEND_QUEUE_LOW_WATERMARK):
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
    if not settings.USE_RATIO_RESET:
      return True

    if settings.DESTINATION_POOL_REPLICAS:
        received = self.factory.attemptedRelays
    else:
        received = 'metricsReceived'

    destination_sent = float(instrumentation.prior_stats.get(self.sent, 0))
    total_received = float(instrumentation.prior_stats.get(received, 0))
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


class CAReplaceClientContextFactory:
    """A context factory for SSL clients needing a different CA chain."""

    isClient = 1
    # SSLv23_METHOD allows SSLv2, SSLv3, and TLSv1.  We disable SSLv2 below,
    # though.
    method = SSL.SSLv23_METHOD if SSL else None

    _cafile = None

    def __init__(self, file=None):
        self._cafile = file

    def getContext(self):
        ctx = SSL.Context(self.method)
        ctx.set_options(SSL.OP_NO_SSLv2)
        if self._cafile is not None:
            ctx.use_certificate_chain_file(self._cafile)
        return ctx


class CarbonClientFactory(with_metaclass(PluginRegistrar, ReconnectingClientFactory, object)):
  plugins = {}
  maxDelay = 5

  def __init__(self, destination, router):
    self.destination = destination
    self.router = router
    self.destinationName = ('%s:%d:%s' % destination).replace('.', '_')
    self.host, self.port, self.carbon_instance = destination
    self.addr = (self.host, self.port)
    self.started = False
    # This factory maintains protocol state across reconnects
    self.queue = deque()  # Change to make this the sole source of metrics to be sent.
    self.connectedProtocol = None
    self.queueEmpty = Deferred()
    self.queueFull = Deferred()
    self.queueFull.addCallbacks(self.queueFullCallback, log.err)
    self.queueHasSpace = Deferred()
    self.queueHasSpace.addCallbacks(self.queueSpaceCallback, log.err)
    # Args: {'connector': connector, 'reason': reason}
    self.connectFailed = Deferred()
    # Args: {'connector': connector, 'reason': reason}
    self.connectionLost = Deferred()
    # Args: protocol instance
    self.connectionMade = Deferred()
    self.connectionMade.addCallbacks(self.clientConnectionMade, log.err)
    self.deferSendPending = None
    # Define internal metric names
    self.attemptedRelays = 'destinations.%s.attemptedRelays' % self.destinationName
    self.fullQueueDrops = 'destinations.%s.fullQueueDrops' % self.destinationName
    self.queuedUntilConnected = 'destinations.%s.queuedUntilConnected' % self.destinationName
    self.relayMaxQueueLength = 'destinations.%s.relayMaxQueueLength' % self.destinationName

  def clientProtocol(self):
    raise NotImplementedError()

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
      self.queueFull.addCallbacks(self.queueFullCallback, log.err)
      state.events.cacheSpaceAvailable()
    self.queueHasSpace = Deferred()
    self.queueHasSpace.addCallbacks(self.queueSpaceCallback, log.err)

  def buildProtocol(self, addr):
    self.connectedProtocol = self.clientProtocol()
    self.connectedProtocol.factory = self
    return self.connectedProtocol

  def startConnecting(self):  # calling this startFactory yields recursion problems
    self.started = True

    if settings['DESTINATION_TRANSPORT'] == "ssl":
       if not SSL or not ssl:
           print("SSL destination transport request, but no Python OpenSSL available.")
           raise SystemExit(1)
       authority = None
       if settings['DESTINATION_SSL_CA']:
           try:
             with open(settings['DESTINATION_SSL_CA']) as f:
               authority = ssl.Certificate.loadPEM(f.read())
           except IOError:
             print("Failed to read CA chain: %s" % settings['DESTINATION_SSL_CA'])
             raise SystemExit(1)
       # Twisted 14 introduced this function, it might not be around on older installs.
       if hasattr(ssl, "optionsForClientTLS"):
           from six import u
           client = ssl.optionsForClientTLS(u(self.host), authority)
       else:
           client = CAReplaceClientContextFactory(settings['DESTINATION_SSL_CA'])
       self.connector = reactor.connectSSL(self.host, self.port, self, client)
    else:
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
      for _ in range(settings.MAX_DATAPOINTS_PER_MESSAGE):
        try:
          yield self.queue.popleft()
        except IndexError:
          return
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
    log.clients("%s::startedConnecting (%s:%d)" % (
        self, connector.host, connector.port))

  def clientConnectionMade(self, client):
    log.clients("%s::connectionMade (%s)" % (self, client))
    self.resetDelay()
    self.destinationUp(client.destination)
    self.connectionMade.addCallbacks(self.clientConnectionMade, log.err)
    return client

  def clientConnectionLost(self, connector, reason):
    ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    log.clients("%s::clientConnectionLost (%s:%d) %s" % (
        self, connector.host, connector.port, reason.getErrorMessage()))
    self.connectedProtocol = None

    self.destinationDown(self.destination)

    args = dict(connector=connector, reason=reason)
    d = self.connectionLost
    self.connectionLost = Deferred()
    d.callback(args)

  def clientConnectionFailed(self, connector, reason):
    ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    log.clients("%s::clientConnectionFailed (%s:%d) %s" % (
        self, connector.host, connector.port, reason.getErrorMessage()))

    self.destinationDown(self.destination)

    args = dict(connector=connector, reason=reason)
    d = self.connectFailed
    self.connectFailed = Deferred()
    d.callback(args)

  def destinationUp(self, destination):
    log.clients("Destination is up: %s:%d:%s" % destination)
    if not self.router.hasDestination(destination):
      log.clients("Adding client %s:%d:%s to router" % destination)
      self.router.addDestination(destination)
      state.events.resumeReceivingMetrics()

  def destinationDown(self, destination):
    # Only blacklist the destination if we tried a lot.
    log.clients("Destination is down: %s:%d:%s (%d/%d)" % (
        destination[0], destination[1], destination[2], self.retries,
        settings.DYNAMIC_ROUTER_MAX_RETRIES))
    # Retries comes from the ReconnectingClientFactory.
    if self.retries < settings.DYNAMIC_ROUTER_MAX_RETRIES:
      return

    if settings.DYNAMIC_ROUTER and self.router.hasDestination(destination):
      log.clients("Removing client %s:%d:%s to router" % destination)
      self.router.removeDestination(destination)
      # Do not receive more metrics if we don't have any usable destinations.
      if not self.router.countDestinations():
          state.events.pauseReceivingMetrics()
      # Re-inject queued metrics.
      metrics = list(self.queue)
      log.clients("Re-injecting %d metrics from %s" % (len(metrics), self))
      for metric, datapoint in metrics:
          state.events.metricGenerated(metric, datapoint)
      self.queue.clear()

  def disconnect(self):
    self.queueEmpty.addCallbacks(lambda result: self.stopConnecting(), log.err)
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


# Basic clients and associated factories.
class CarbonPickleClientProtocol(CarbonClientProtocol, Int32StringReceiver):

  def _sendDatapointsNow(self, datapoints):
    self.sendString(pickle.dumps(datapoints, protocol=2))


class CarbonPickleClientFactory(CarbonClientFactory):
  plugin_name = "pickle"

  def clientProtocol(self):
      return CarbonPickleClientProtocol()


class CarbonLineClientProtocol(CarbonClientProtocol, LineOnlyReceiver):

  def _sendDatapointsNow(self, datapoints):
    for metric, datapoint in datapoints:
      if isinstance(datapoint[1], float):
        value = ("%.10f" % datapoint[1]).rstrip('0').rstrip('.')
      else:
        value = "%d" % datapoint[1]
      to_send = "%s %s %d" % (metric, value, datapoint[0])
      self.sendLine(to_send.encode('utf-8'))


class CarbonLineClientFactory(CarbonClientFactory):
  plugin_name = "line"

  def clientProtocol(self):
      return CarbonLineClientProtocol()


class FakeClientFactory(object):
  """Fake client factory that buffers points

  This is used when all the destinations are down and before we
  pause the reception of metrics to avoid loosing points.
  """

  def __init__(self):
    # This queue isn't explicitly bounded but will implicitly be. It receives
    # only metrics when no destinations are available, and as soon as we detect
    # that we don't have any destination we pause the producer: this mean that
    # it will contain only a few seconds of metrics.
    self.queue = deque()
    self.started = False

  def startConnecting(self):
    pass

  def sendDatapoint(self, metric, datapoint):
    self.queue.append((metric, datapoint))

  def sendHighPriorityDatapoint(self, metric, datapoint):
    self.queue.append((metric, datapoint))

  def reinjectDatapoints(self):
    metrics = list(self.queue)
    log.clients("Re-injecting %d metrics from %s" % (len(metrics), self))
    for metric, datapoint in metrics:
        state.events.metricGenerated(metric, datapoint)
    self.queue.clear()


class CarbonClientManager(Service):
  def __init__(self, router):
    if settings.DESTINATION_POOL_REPLICAS:
        # If we decide to open multiple TCP connection to a replica, we probably
        # want to try to also load-balance across hosts. In this case we need
        # to make sure rfc3484 doesn't get in the way.
        if setUpRandomResolver:
          setUpRandomResolver(reactor)
        else:
          print("Import error, Twisted >= 17.1.0 needed for using DESTINATION_POOL_REPLICAS.")
          raise SystemExit(1)

    self.router = router
    self.client_factories = {}  # { destination : CarbonClientFactory() }
    # { destination[0:2]: set(CarbonClientFactory()) }
    self.pooled_factories = defaultdict(set)

    # This fake factory will be used as a buffer when we did not manage
    # to connect to any destination.
    fake_factory = FakeClientFactory()
    self.client_factories[None] = fake_factory
    state.events.resumeReceivingMetrics.addHandler(fake_factory.reinjectDatapoints)

  def createFactory(self, destination):
    factory_name = settings["DESTINATION_PROTOCOL"]
    factory_class = CarbonClientFactory.plugins.get(factory_name)

    if not factory_class:
      print("In carbon.conf, DESTINATION_PROTOCOL must be one of %s. "
            "Invalid value: '%s'" % (', '.join(CarbonClientFactory.plugins), factory_name))
      raise SystemExit(1)

    return factory_class(destination, self.router)

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
    if not settings.DYNAMIC_ROUTER:
        # If not using a dynamic router we add the destination before
        # it's known to be working.
        self.router.addDestination(destination)

    factory = self.createFactory(destination)
    self.client_factories[destination] = factory
    self.pooled_factories[destination[0:2]].add(factory)

    connectAttempted = DeferredList(
        [factory.connectionMade, factory.connectFailed],
        fireOnOneCallback=True,
        fireOnOneErrback=True)
    if self.running:
      factory.startConnecting()  # this can trigger & replace connectFailed

    return connectAttempted

  def stopClient(self, destination):
    factory = self.client_factories.get(destination)
    if factory is None or destination is None:
      return None

    self.router.removeDestination(destination)
    stopCompleted = factory.disconnect()
    stopCompleted.addCallbacks(
        lambda result: self.disconnectClient(destination), log.err
    )
    return stopCompleted

  def disconnectClient(self, destination):
    factory = self.client_factories.pop(destination)
    self.pooled_factories[destination[0:2]].remove(factory)
    c = factory.connector
    if c and c.state == 'connecting' and not factory.hasQueuedDatapoints():
      c.stopConnecting()

  def stopAllClients(self):
    deferreds = []
    for destination in list(self.client_factories):
      deferred = self.stopClient(destination)
      if deferred:
        deferreds.append(deferred)
    return DeferredList(deferreds)

  def getDestinations(self, metric):
    destinations = list(self.router.getDestinations(metric))
    # If we can't find any destination we just buffer the
    # points. We will also pause the socket on the receiving side.
    if not destinations:
      return [None]
    return destinations

  def getFactories(self, metric):
    destinations = self.getDestinations(metric)
    factories = set()

    if not settings.DESTINATION_POOL_REPLICAS:
      # Simple case, with only one replica per destination.
      for d in destinations:
        # If we can't find it, we add to the 'fake' factory / buffer.
        factories.add(self.client_factories.get(d))
    else:
      # Here we might have multiple replicas per destination.
      for d in destinations:
        if d is None:
          # d == None means there are no destinations currently available, so
          # we just put the data into our fake factory / buffer.
          factories.add(self.client_factories[None])
        else:
          # Else we take the replica with the smallest queue size.
          key = d[0:2]  # Take only host:port, not instance.
          factories.add(min(self.pooled_factories[key], key=lambda f: f.queueSize))
    return factories

  def sendDatapoint(self, metric, datapoint):
    for factory in self.getFactories(metric):
      factory.sendDatapoint(metric, datapoint)

  def sendHighPriorityDatapoint(self, metric, datapoint):
    for factory in self.getFactories(metric):
      factory.sendHighPriorityDatapoint(metric, datapoint)

  def __str__(self):
    return "<%s[%x]>" % (self.__class__.__name__, id(self))


class RelayProcessor(pipeline.Processor):
  plugin_name = 'relay'

  def process(self, metric, datapoint):
    if settings.TAG_RELAY_NORMALIZED:
      # normalize metric name
      try:
        metric = TaggedSeries.parse(metric).path
      except Exception as err:
        log.msg('Error parsing metric %s: %s' % (metric, err))
        # continue anyway with processing the unnormalized metric for robustness

    state.client_manager.sendDatapoint(metric, datapoint)
    return pipeline.Processor.NO_OUTPUT
