import time
import traceback

from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
from twisted.internet.error import ConnectionDone
from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from carbon import log, events, state
from carbon.conf import settings
from carbon.util import pickle, get_unpickler


class MetricReceiver:
  """ Base class for all metric receiving protocols, handles flow
  control events and connection state logging.
  """
  def connectionMade(self):
    self.peerName = self.getPeerName()
    if settings.LOG_LISTENER_CONN_SUCCESS:
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
      if settings.LOG_LISTENER_CONN_SUCCESS:
        log.listener("%s connection with %s closed cleanly" % (self.__class__.__name__, self.peerName))

    else:
      log.listener("%s connection with %s lost: %s" % (self.__class__.__name__, self.peerName, reason.value))

    state.connectedMetricReceiverProtocols.remove(self)
    events.pauseReceivingMetrics.removeHandler(self.pauseReceiving)
    events.resumeReceivingMetrics.removeHandler(self.resumeReceiving)

  def metricReceived(self, metric, datapoint):
    if datapoint[1] != datapoint[1]: # filter out NaN values
      return
    if int(datapoint[0]) == -1: # use current time if none given: https://github.com/graphite-project/carbon/issues/54
      datapoint = (time.time(), datapoint[1])
    
    events.metricReceived(metric, datapoint)


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
  delimiter = '\n'

  def lineReceived(self, line):
    try:
      metric, value, timestamp = line.strip().split()
      datapoint = ( float(timestamp), float(value) )
    except:
      log.listener('invalid line received from client %s, ignoring' % self.peerName)
      return

    self.metricReceived(metric, datapoint)


class MetricDatagramReceiver(MetricReceiver, DatagramProtocol):
  def datagramReceived(self, data, (host, port)):
    for line in data.splitlines():
      try:
        metric, value, timestamp = line.strip().split()
        datapoint = ( float(timestamp), float(value) )

        self.metricReceived(metric, datapoint)
      except:
        log.listener('invalid line received from %s, ignoring' % host)


class MetricPickleReceiver(MetricReceiver, Int32StringReceiver):
  MAX_LENGTH = 2 ** 20

  def connectionMade(self):
    MetricReceiver.connectionMade(self)
    self.unpickler = get_unpickler(insecure=settings.USE_INSECURE_UNPICKLER)

  def stringReceived(self, data):
    try:
      datapoints = self.unpickler.loads(data)
    except:
      log.listener('invalid pickle received from %s, ignoring' % self.peerName)
      return

    for (metric, datapoint) in datapoints:
      #Expect proper types since it is coming in pickled.
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
      datapoints = MetricCache.get_datapoints(metric)
      result = dict(datapoints=datapoints)
      if settings.LOG_CACHE_HITS:
        log.query('[%s] cache query for \"%s\" returned %d values' % (self.peerAddr, metric, len(datapoints)))
      instrumentation.increment('writer.cache_queries')

    elif request['type'] == 'bulk-cache-query':
      query_results = {}
      for metric in request['metrics']:
        query_results[metric] = MetricCache.get_datapoints(metric)
      if settings.LOG_CACHE_HITS:
        log.query('[%s] cache-query-bulk for %d metrics' % (self.peerAddr, len(query_results)))
      instrumentation.increment('writer.bulk_cache_queries')
      result = dict(results=query_results)

    elif request['type'] == 'get-metadata':
      try:
        value = state.database.get_metadata(request['metric'], request['key'])
        result = dict(value=value)
      except:
        log.err()
        result = dict(error=traceback.format_exc())

    elif request['type'] == 'set-metadata':
      try:
        old_value = state.database.set_metadata(request['metric'],
                                                request['key'],
                                                request['value'])
        result = dict(old_value=old_value, new_value=request['value'])
      except:
        log.err()
        result = dict(error=traceback.format_exc())
    else:
      result = dict(error="Invalid request type \"%s\"" % request['type'])

    response = pickle.dumps(result, protocol=-1)
    self.sendString(response)


# Avoid import circularities
from carbon.cache import MetricCache
from carbon import instrumentation
