import time

from twisted.internet.protocol import DatagramProtocol
from twisted.internet.error import ConnectionDone
from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from carbon import log, events, state, management
from carbon.conf import settings
from carbon.regexlist import WhiteList, BlackList
from carbon.util import pickle, get_unpickler


class MetricReceiver:
  """ Base class for all metric receiving protocols, handles flow
  control events and connection state logging.
  """
  def connectionMade(self):
    self.peerName = self.getPeerName()
    if settings.LOG_LISTENER_CONNECTIONS:
      log.listener("%s connection with %s established" % (self.__class__.__name__, self.peerName))

    if state.metricReceiversPaused:
      self.pauseReceiving()

    state.connectedMetricReceiverProtocols.add(self)
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
      if settings.LOG_LISTENER_CONNECTIONS:
        log.listener("%s connection with %s closed cleanly" % (self.__class__.__name__, self.peerName))
    else:
      log.listener("%s connection with %s lost: %s" % (self.__class__.__name__, self.peerName, reason.value))

    state.connectedMetricReceiverProtocols.remove(self)
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
    if int(datapoint[0]) == -1:  # use current time if none given
      datapoint = (time.time(), datapoint[1])

    events.metricReceived(metric, datapoint)


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
  delimiter = '\n'

  def lineReceived(self, line):
    try:
      metric, value, timestamp = line.strip().split()
      datapoint = (float(timestamp), float(value))
    except ValueError:
      log.listener('invalid line (%s) received from client %s, ignoring' % (line, self.peerName))
      return

    self.metricReceived(metric, datapoint)


class MetricDatagramReceiver(MetricReceiver, DatagramProtocol):
  def datagramReceived(self, data, (host, port)):
    for line in data.splitlines():
      try:
        metric, value, timestamp = line.strip().split()
        datapoint = (float(timestamp), float(value))

        self.metricReceived(metric, datapoint)
      except ValueError:
        log.listener('invalid line (%s) received from %s, ignoring' % (line, host))


class MetricPickleReceiver(MetricReceiver, Int32StringReceiver):
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
      datapoints = MetricCache.get(metric, [])
      result = dict(datapoints=datapoints)
      if settings.LOG_CACHE_HITS:
        log.query('[%s] cache query for \"%s\" returned %d values' % (self.peerAddr, metric, len(datapoints)))
      instrumentation.increment('cacheQueries')

    elif request['type'] == 'cache-query-bulk':
      datapointsByMetric = {}
      metrics = request['metrics']
      for metric in metrics:
        datapointsByMetric[metric] = MetricCache.get(metric, [])

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
