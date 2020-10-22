import os
import time
import socket
from resource import getrusage, RUSAGE_SELF

from twisted.application.service import Service
from twisted.internet.task import LoopingCall
from carbon.conf import settings


stats = {}
prior_stats = {}
HOSTNAME = socket.gethostname().replace('.', '_')
PAGESIZE = os.sysconf('SC_PAGESIZE')
rusage = getrusage(RUSAGE_SELF)
lastUsage = rusage.ru_utime + rusage.ru_stime
lastUsageTime = time.time()

# NOTE: Referencing settings in this *top level scope* will
# give you *defaults* only. Probably not what you wanted.

# TODO(chrismd) refactor the graphite metrics hierarchy to be cleaner,
# more consistent, and make room for frontend metrics.
# metric_prefix = "Graphite.backend.%(program)s.%(instance)s." % settings


def increment(stat, increase=1):
  try:
    stats[stat] += increase
  except KeyError:
    stats[stat] = increase


def max(stat, newval):
  try:
    if stats[stat] < newval:
      stats[stat] = newval
  except KeyError:
    stats[stat] = newval


def append(stat, value):
  try:
    stats[stat].append(value)
  except KeyError:
    stats[stat] = [value]


def getCpuUsage():
  global lastUsage, lastUsageTime

  rusage = getrusage(RUSAGE_SELF)
  currentUsage = rusage.ru_utime + rusage.ru_stime
  currentTime = time.time()

  usageDiff = currentUsage - lastUsage
  timeDiff = currentTime - lastUsageTime

  if timeDiff == 0:  # shouldn't be possible, but I've actually seen a ZeroDivisionError from this
    timeDiff = 0.000001

  cpuUsagePercent = (usageDiff / timeDiff) * 100.0

  lastUsage = currentUsage
  lastUsageTime = currentTime

  return cpuUsagePercent


def getMemUsage():
  with open('/proc/self/statm') as statm:
    rss_pages = int(statm.read().split()[1])
  return rss_pages * PAGESIZE


def recordMetrics():
  global lastUsage
  global prior_stats
  myStats = stats.copy()
  myPriorStats = {}
  stats.clear()

  # cache metrics
  if 'cache' in settings.program:
    record = cache_record
    updateTimes = myStats.get('updateTimes', [])
    committedPoints = myStats.get('committedPoints', 0)
    creates = myStats.get('creates', 0)
    droppedCreates = myStats.get('droppedCreates', 0)
    errors = myStats.get('errors', 0)
    cacheQueries = myStats.get('cacheQueries', 0)
    cacheBulkQueries = myStats.get('cacheBulkQueries', 0)
    cacheOverflow = myStats.get('cache.overflow', 0)
    cacheBulkQuerySizes = myStats.get('cacheBulkQuerySize', [])

    # Calculate cache-data-structure-derived metrics prior to storing anything
    # in the cache itself -- which would otherwise affect said metrics.
    cache_size = cache.MetricCache().size
    cache_queues = len(cache.MetricCache())
    record('cache.size', cache_size)
    record('cache.queues', cache_queues)

    if updateTimes:
      avgUpdateTime = sum(updateTimes) / len(updateTimes)
      record('avgUpdateTime', avgUpdateTime)

    if committedPoints:
      pointsPerUpdate = float(committedPoints) / len(updateTimes)
      record('pointsPerUpdate', pointsPerUpdate)

    if cacheBulkQuerySizes:
      avgBulkSize = sum(cacheBulkQuerySizes) / len(cacheBulkQuerySizes)
      record('cache.bulk_queries_average_size', avgBulkSize)

    record('updateOperations', len(updateTimes))
    record('committedPoints', committedPoints)
    record('creates', creates)
    record('droppedCreates', droppedCreates)
    record('errors', errors)
    record('cache.queries', cacheQueries)
    record('cache.bulk_queries', cacheBulkQueries)
    record('cache.overflow', cacheOverflow)

  # aggregator metrics
  elif 'aggregator' in settings.program:
    record = aggregator_record
    record('allocatedBuffers', len(BufferManager))
    record('bufferedDatapoints',
           sum(b.size for b in BufferManager.buffers.values()))
    record('aggregateDatapointsSent', myStats.get('aggregateDatapointsSent', 0))

  # relay metrics
  else:
    record = relay_record

  # shared relay stats for relays & aggregators
  if settings.program in ['carbon-aggregator', 'carbon-relay']:
    prefix = 'destinations.'
    relay_stats = [(k, v) for (k, v) in myStats.items() if k.startswith(prefix)]
    for stat_name, stat_value in relay_stats:
      record(stat_name, stat_value)
      # Preserve the count of sent metrics so that the ratio of
      # received : sent can be checked per-relay to determine the
      # health of the destination.
      if stat_name.endswith('.sent') or stat_name.endswith('.attemptedRelays'):
        myPriorStats[stat_name] = stat_value

  # common metrics
  record('activeConnections', len(state.connectedMetricReceiverProtocols))
  record('metricsReceived', myStats.get('metricsReceived', 0))
  record('blacklistMatches', myStats.get('blacklistMatches', 0))
  record('whitelistRejects', myStats.get('whitelistRejects', 0))
  record('cpuUsage', getCpuUsage())

  # And here preserve count of messages received in the prior period
  myPriorStats['metricsReceived'] = myStats.get('metricsReceived', 0)
  prior_stats.clear()
  prior_stats.update(myPriorStats)

  try:  # This only works on Linux
    record('memUsage', getMemUsage())
  except Exception:
    pass


def cache_record(metric, value):
    prefix = settings.CARBON_METRIC_PREFIX
    if settings.instance is None:
      fullMetric = '%s.agents.%s.%s' % (prefix, HOSTNAME, metric)
    else:
      fullMetric = '%s.agents.%s-%s.%s' % (prefix, HOSTNAME, settings.instance, metric)
    datapoint = (time.time(), value)
    cache.MetricCache().store(fullMetric, datapoint)


def relay_record(metric, value):
    prefix = settings.CARBON_METRIC_PREFIX
    if settings.instance is None:
      fullMetric = '%s.relays.%s.%s' % (prefix, HOSTNAME, metric)
    else:
      fullMetric = '%s.relays.%s-%s.%s' % (prefix, HOSTNAME, settings.instance, metric)
    datapoint = (time.time(), value)
    events.metricGenerated(fullMetric, datapoint)


def aggregator_record(metric, value):
    prefix = settings.CARBON_METRIC_PREFIX
    if settings.instance is None:
      fullMetric = '%s.aggregator.%s.%s' % (prefix, HOSTNAME, metric)
    else:
      fullMetric = '%s.aggregator.%s-%s.%s' % (prefix, HOSTNAME, settings.instance, metric)
    datapoint = (time.time(), value)
    events.metricGenerated(fullMetric, datapoint)


class InstrumentationService(Service):
    def __init__(self):
        self.record_task = LoopingCall(recordMetrics)

    def startService(self):
        if settings.CARBON_METRIC_INTERVAL > 0:
          self.record_task.start(settings.CARBON_METRIC_INTERVAL, False)
        Service.startService(self)

    def stopService(self):
        if settings.CARBON_METRIC_INTERVAL > 0:
          self.record_task.stop()
        Service.stopService(self)


# Avoid import circularities
from carbon import state, events, cache  # NOQA
from carbon.aggregator.buffers import BufferManager  # NOQA
