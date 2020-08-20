import time
import socket
import sys

from twisted.internet.protocol import ServerFactory, DatagramProtocol
# from twisted.application.internet import TCPServer, UDPServer
from twisted.application import service
from twisted.internet.error import ConnectionDone
from twisted.internet import reactor, tcp, udp
from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from twisted.protocols.policies import TimeoutMixin
from carbon import log, events, state, management
from carbon.conf import settings
from carbon.regexlist import WhiteList, BlackList
from carbon.util import pickle, get_unpickler
from carbon.util import PluginRegistrar
from six import with_metaclass
from carbon.util import enableTcpKeepAlive


def checkIfAcceptingConnections():
  clients = len(state.connectedMetricReceiverProtocols)
  max_clients = settings.MAX_RECEIVER_CONNECTIONS

  if clients < max_clients:
    for port in state.listeningPorts:
      if port.paused:
        log.listener(
          "Resuming %s (%d/%d connections)" % (port, clients, max_clients))
        port.resumeProducing()
        port.paused = False
  else:
    for port in state.listeningPorts:
      if not port.paused:
        log.listener(
          "Pausing %s (%d/%d connections)" % (port, clients, max_clients))
        port.pauseProducing()
        port.paused = True


class CarbonReceiverFactory(ServerFactory):

  def buildProtocol(self, addr):
    clients = len(state.connectedMetricReceiverProtocols)
    max_clients = settings.MAX_RECEIVER_CONNECTIONS

    if clients < max_clients:
      return ServerFactory.buildProtocol(self, addr)
    else:
      return None


class CarbonService(service.Service):
    """Create our own socket to support SO_REUSEPORT.
    To be removed when twisted supports it natively
    See: https://github.com/twisted/twisted/pull/759.
    """
    factory = None
    protocol = None

    def __init__(self, interface, port, protocol, factory):
        self.protocol = protocol
        self.factory = factory
        self.interface = interface
        self.port = port

    def startService(self):
        # use socket creation from twisted to use the same options as before
        if hasattr(self.protocol, 'datagramReceived'):
            tmp_port = udp.Port(None, None, interface=self.interface)
        else:
            tmp_port = tcp.Port(None, None, interface=self.interface)
        carbon_sock = tmp_port.createInternetSocket()
        if hasattr(socket, 'SO_REUSEPORT'):
            carbon_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        carbon_sock.bind((self.interface, self.port))

        if hasattr(self.protocol, 'datagramReceived'):
            self._port = reactor.adoptDatagramPort(
                carbon_sock.fileno(), socket.AF_INET, self.protocol())
        else:
            carbon_sock.listen(tmp_port.backlog)
            self._port = reactor.adoptStreamPort(
                carbon_sock.fileno(), socket.AF_INET, self.factory)
            state.listeningPorts.append(self._port)
            self._port.paused = False
        carbon_sock.close()

    def stopService(self):
        self._port.stopListening()


class CarbonServerProtocol(with_metaclass(PluginRegistrar, object)):
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
      service = CarbonService(interface, port, protocol, None)
    else:
      factory = CarbonReceiverFactory()
      factory.protocol = protocol
      service = CarbonService(interface, port, protocol, factory)
    service.setServiceParent(root_service)


class MetricReceiver(CarbonServerProtocol, TimeoutMixin):
  """ Base class for all metric receiving protocols, handles flow
  control events and connection state logging.
  """

  def connectionMade(self):
    self.setTimeout(settings.METRIC_CLIENT_IDLE_TIMEOUT)
    enableTcpKeepAlive(self.transport, settings.TCP_KEEPALIVE, settings)
    self.peerName = self.getPeerName()

    if settings.LOG_LISTENER_CONN_SUCCESS:
      log.listener("%s connection with %s established" % (
        self.__class__.__name__, self.peerName))

    if state.metricReceiversPaused:
      self.pauseReceiving()

    state.connectedMetricReceiverProtocols.add(self)
    checkIfAcceptingConnections()
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
        log.listener(
          "%s connection with %s closed cleanly" % (self.__class__.__name__, self.peerName))

    else:
      if settings.LOG_LISTENER_CONN_LOST:
        log.listener(
          "%s connection with %s lost: %s" % (self.__class__.__name__, self.peerName, reason.value))

    state.connectedMetricReceiverProtocols.remove(self)
    checkIfAcceptingConnections()
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
    # use current time if none given: https://github.com/graphite-project/carbon/issues/54
    if int(datapoint[0]) == -1:
      datapoint = (time.time(), datapoint[1])
    res = settings.MIN_TIMESTAMP_RESOLUTION
    if res:
      datapoint = (int(datapoint[0]) // res * res, datapoint[1])
    events.metricReceived(metric, datapoint)
    self.resetTimeout()


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
  plugin_name = "line"
  delimiter = b'\n'

  def lineReceived(self, line):
    if sys.version_info >= (3, 0):
      line = line.decode('utf-8')

    try:
      metric, value, timestamp = line.strip().split()
      datapoint = (float(timestamp), float(value))
    except ValueError:
      if len(line) > 400:
        line = line[:400] + '...'
      log.listener('invalid line received from client %s, ignoring [%s]' %
                   (self.peerName, repr(line.strip())[1:-1]))
      return

    self.metricReceived(metric, datapoint)


class MetricDatagramReceiver(MetricReceiver, DatagramProtocol):
  plugin_name = "udp"

  @classmethod
  def build(cls, root_service):
    if not settings.ENABLE_UDP_LISTENER:
      return

    super(MetricDatagramReceiver, cls).build(root_service)

  def datagramReceived(self, data, addr):
    (host, _) = addr
    if sys.version_info >= (3, 0):
      data = data.decode('utf-8')

    for line in data.splitlines():
      try:
        metric, value, timestamp = line.strip().split()
        datapoint = (float(timestamp), float(value))

        self.metricReceived(metric, datapoint)
      except ValueError:
        if len(line) > 400:
          line = line[:400] + '...'
        log.listener('invalid line received from %s, ignoring [%s]' %
                     (host, repr(line.strip())[1:-1]))


class MetricPickleReceiver(MetricReceiver, Int32StringReceiver):
  plugin_name = "pickle"

  def __init__(self):
    super(MetricPickleReceiver, self).__init__()
    self.MAX_LENGTH = settings.PICKLE_RECEIVER_MAX_LENGTH

  def connectionMade(self):
    MetricReceiver.connectionMade(self)
    self.unpickler = get_unpickler(insecure=settings.USE_INSECURE_UNPICKLER)

  def stringReceived(self, data):
    try:
      datapoints = self.unpickler.loads(data)
    # Pickle can throw a wide range of exceptions
    except (pickle.UnpicklingError, ValueError, IndexError, ImportError,
            KeyError, EOFError) as exc:
      log.listener('invalid pickle received from %s, error: "%s", ignoring' % (
                   self.peerName, exc))
      return

    for raw in datapoints:
      try:
        (metric, (value, timestamp)) = raw
      except Exception as e:
        log.listener('Error decoding pickle: %s' % e)
        continue

      try:
        datapoint = (float(value), float(timestamp))  # force proper types
      except (ValueError, TypeError):
        continue

      # convert python2 unicode objects to str/bytes
      if not isinstance(metric, str):
        metric = metric.encode('utf-8')

      self.metricReceived(metric, datapoint)


class CacheManagementHandler(Int32StringReceiver):
  MAX_LENGTH = 1024 ** 3  # 1mb

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
    cache = MetricCache()
    if request['type'] == 'cache-query':
      metric = request['metric']
      datapoints = list(cache.get(metric, {}).items())
      result = dict(datapoints=datapoints)
      if settings.LOG_CACHE_HITS:
        log.query('[%s] cache query for \"%s\" returned %d values' % (
          self.peerAddr, metric, len(datapoints)
        ))
      instrumentation.increment('cacheQueries')

    elif request['type'] == 'cache-query-bulk':
      datapointsByMetric = {}
      metrics = request['metrics']
      for metric in metrics:
        datapointsByMetric[metric] = list(cache.get(metric, {}).items())

      result = dict(datapointsByMetric=datapointsByMetric)

      if settings.LOG_CACHE_HITS:
        log.query('[%s] cache query bulk for \"%d\" metrics returned %d values' % (
          self.peerAddr,
          len(metrics),
          sum([len(datapoints) for datapoints in datapointsByMetric.values()])
        ))
      instrumentation.increment('cacheBulkQueries')
      instrumentation.append('cacheBulkQuerySize', len(metrics))

    elif request['type'] == 'get-metadata':
      result = management.getMetadata(request['metric'], request['key'])

    elif request['type'] == 'set-metadata':
      result = management.setMetadata(request['metric'], request['key'], request['value'])

    else:
      result = dict(error="Invalid request type \"%s\"" % request['type'])

    response = pickle.dumps(result, protocol=2)
    self.sendString(response)


# Avoid import circularities
from carbon.cache import MetricCache  # NOQA
from carbon import instrumentation  # NOQA
