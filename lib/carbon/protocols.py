import time

from twisted.internet.protocol import ServerFactory, DatagramProtocol
from twisted.application.internet import TCPServer, UDPServer
from twisted.internet.error import ConnectionDone
from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from twisted.protocols.policies import TimeoutMixin
from carbon import log, events, state, management
from carbon.conf import settings
from carbon.regexlist import WhiteList, BlackList
from carbon.util import pickle, get_unpickler
from carbon.util import PluginRegistrar


class CarbonReceiverFactory(ServerFactory):
  def buildProtocol(self, addr):
    from carbon.conf import settings

    # Don't establish the connection if we have reached the limit.
    if len(state.connectedMetricReceiverProtocols) < settings.MAX_RECEIVER_CONNECTIONS:
      return ServerFactory.buildProtocol(self, addr)
    else:
      return None


class CarbonServerProtocol(object):
  __metaclass__ = PluginRegistrar
  plugins = {}

  @classmethod
  def build(cls, root_service):
    plugin_up = cls.plugin_name.upper()
    interface = settings.get('%s_RECEIVER_INTERFACE' % plugin_up, None)
    port = int(settings.get('%s_RECEIVER_PORT' % plugin_up, 0))
    protocol = cls

    if not port:
      return

    if hasattr(protocol, 'datagramReceived'):
      service = UDPServer(port, protocol(), interface=interface)
      service.setServiceParent(root_service)
    else:
      factory = CarbonReceiverFactory()
      factory.protocol = protocol
      service = TCPServer(port, factory, interface=interface)
      service.setServiceParent(root_service)


class MetricReceiver(CarbonServerProtocol, TimeoutMixin):
  """ Base class for all metric receiving protocols, handles flow
  control events and connection state logging.
  """

  def connectionMade(self):
    self.setTimeout(settings.METRIC_CLIENT_IDLE_TIMEOUT)
    self.peerName = self.getPeerName()
    if settings.LOG_LISTENER_CONN_SUCCESS:
      log.listener("%s connection with %s established" % (
        self.__class__.__name__, self.peerName))

    if state.metricReceiversPaused:
      self.pauseReceiving()

    state.connectedMetricReceiverProtocols.add(self)
    if settings.USE_FLOW_CONTROL:
      events.pauseReceivingMetrics.addHandler(self.pauseReceiving)
      events.resumeReceivingMetrics.addHandler(self.resumeReceiving)

  def getPeerName(self):
    if hasattr(self.transport, 'getPeer'):
      peer = self.transport.getPeer()
      return "%s:%d" % (peer.host, peer.port)
    else:
      return "peer"

  def pauseReceiving(self):
    self.transport.pauseProducing()

  def resumeReceiving(self):
    self.transport.resumeProducing()

  def connectionLost(self, reason):
    if reason.check(ConnectionDone):
      if settings.LOG_LISTENER_CONN_SUCCESS:
        log.listener("%s connection with %s closed cleanly" % (self.__class__.__name__, self.peerName))

    else:
      log.listener("%s connection with %s lost: %s" % (self.__class__.__name__, self.peerName, reason.value))

    state.connectedMetricReceiverProtocols.remove(self)
    if settings.USE_FLOW_CONTROL:
      events.pauseReceivingMetrics.removeHandler(self.pauseReceiving)
      events.resumeReceivingMetrics.removeHandler(self.resumeReceiving)

  def metricReceived(self, metric, datapoint):
    if BlackList and metric in BlackList:
      instrumentation.increment('blacklistMatches')
      return
    if WhiteList and metric not in WhiteList:
      instrumentation.increment('whitelistRejects')
      return
    if datapoint[1] != datapoint[1]:  # filter out NaN values
      return
    if int(datapoint[0]) == -1:  # use current time if none given: https://github.com/graphite-project/carbon/issues/54
      datapoint = (time.time(), datapoint[1])

    events.metricReceived(metric, datapoint)
    self.resetTimeout()


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
  plugin_name = "line"
  delimiter = '\n'

  def lineReceived(self, line):
    try:
      metric, value, timestamp = line.strip().split()
      datapoint = (float(timestamp), float(value))
    except ValueError:
      if len(line) > 400:
        line = line[:400] + '...'
      log.listener('invalid line received from client %s, ignoring [%s]' % (self.peerName, line.strip().encode('string_escape')))
      return

    self.metricReceived(metric, datapoint)


class MetricDatagramReceiver(MetricReceiver, DatagramProtocol):
  plugin_name = "udp"

  @classmethod
  def build(cls, root_service):
    if not settings.ENABLE_UDP_LISTENER:
      return

    super(MetricDatagramReceiver, cls).build(root_service)

  def datagramReceived(self, data, (host, port)):
    for line in data.splitlines():
      try:
        metric, value, timestamp = line.strip().split()
        datapoint = (float(timestamp), float(value))

        self.metricReceived(metric, datapoint)
      except ValueError:
        if len(line) > 400:
          line = line[:400] + '...'
        log.listener('invalid line received from %s, ignoring [%s]' % (host, line.strip().encode('string_escape')))


class MetricPickleReceiver(MetricReceiver, Int32StringReceiver):
  plugin_name = "pickle"
  MAX_LENGTH = 2 ** 20

  def connectionMade(self):
    MetricReceiver.connectionMade(self)
    self.unpickler = get_unpickler(insecure=settings.USE_INSECURE_UNPICKLER)

  def stringReceived(self, data):
    try:
      datapoints = self.unpickler.loads(data)
    except pickle.UnpicklingError:
      log.listener('invalid pickle received from %s, ignoring' % self.peerName)
      return

    for raw in datapoints:
      try:
        (metric, (value, timestamp)) = raw
      except Exception, e:
        log.listener('Error decoding pickle: %s' % e)

      try:
        datapoint = (float(value), float(timestamp))  # force proper types
      except ValueError:
        continue

      self.metricReceived(metric, datapoint)


class CacheManagementHandler(Int32StringReceiver):
  MAX_LENGTH = 1024 ** 3 # 1mb

  def connectionMade(self):
    peer = self.transport.getPeer()
    self.peerAddr = "%s:%d" % (peer.host, peer.port)
    log.query("%s connected" % self.peerAddr)
    self.unpickler = get_unpickler(insecure=settings.USE_INSECURE_UNPICKLER)

  def connectionLost(self, reason):
    if reason.check(ConnectionDone):
      log.query("%s disconnected" % self.peerAddr)
    else:
      log.query("%s connection lost: %s" % (self.peerAddr, reason.value))

  def stringReceived(self, rawRequest):
    request = self.unpickler.loads(rawRequest)
    if request['type'] == 'cache-query':
      metric = request['metric']
      datapoints = MetricCache.get(metric, {}).items()
      result = dict(datapoints=datapoints)
      if settings.LOG_CACHE_HITS:
        log.query('[%s] cache query for \"%s\" returned %d values' % (self.peerAddr, metric, len(datapoints)))
      instrumentation.increment('cacheQueries')

    elif request['type'] == 'cache-query-bulk':
      datapointsByMetric = {}
      metrics = request['metrics']
      for metric in metrics:
        datapointsByMetric[metric] = MetricCache.get(metric, {}).items()

      result = dict(datapointsByMetric=datapointsByMetric)

      if settings.LOG_CACHE_HITS:
        log.query('[%s] cache query bulk for \"%d\" metrics returned %d values' %
            (self.peerAddr, len(metrics), sum([len(datapoints) for datapoints in datapointsByMetric.values()])))
      instrumentation.increment('cacheBulkQueries')
      instrumentation.append('cacheBulkQuerySize', len(metrics))

    elif request['type'] == 'get-metadata':
      result = management.getMetadata(request['metric'], request['key'])

    elif request['type'] == 'set-metadata':
      result = management.setMetadata(request['metric'], request['key'], request['value'])

    else:
      result = dict(error="Invalid request type \"%s\"" % request['type'])

    response = pickle.dumps(result, protocol=-1)
    self.sendString(response)


# Avoid import circularities
from carbon.cache import MetricCache
from carbon import instrumentation
