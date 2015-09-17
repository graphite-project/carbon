import os
import time
import socket
from resource import getrusage, RUSAGE_SELF

from twisted.application.service import Service
from twisted.internet.task import LoopingCall
from carbon.conf import settings


stats = {}
statsPersist = {}
HOSTNAME = socket.gethostname().replace('.','_')
PAGESIZE = os.sysconf('SC_PAGESIZE')
rusage = getrusage(RUSAGE_SELF)
lastUsage = rusage.ru_utime + rusage.ru_stime
lastUsageTime = time.time()

# NOTE: Referencing settings in this *top level scope* will
# give you *defaults* only. Probably not what you wanted.

# TODO(chrismd) refactor the graphite metrics hierarchy to be cleaner,
# more consistent, and make room for frontend metrics.
#metric_prefix = "Graphite.backend.%(program)s.%(instance)s." % settings


def set(stat, val, persist=False):
  if persist == False:
    stats[stat] = val
  else:
    statsPersist[stat] = val

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

  if timeDiff == 0: #shouldn't be possible, but I've actually seen a ZeroDivisionError from this
    timeDiff = 0.000001

  cpuUsagePercent = (usageDiff / timeDiff) * 100.0

  lastUsage = currentUsage
  lastUsageTime = currentTime

  return cpuUsagePercent


def getMemUsage():
  rss_pages = int( open('/proc/self/statm').read().split()[1] )
  return rss_pages * PAGESIZE


def recordMetrics():
  global lastUsage
  myStats = stats.copy()
  stats.clear()

  # cache metrics
  if settings.program == 'carbon-cache':
    record = cache_record
    updateTimes = myStats.get('updateTimes', [])
    committedPoints = myStats.get('committedPoints', 0)
    creates = myStats.get('creates', 0)
    errors = myStats.get('errors', 0)
    cacheQueries = myStats.get('cacheQueries', 0)
    cacheBulkQueries = myStats.get('cacheBulkQueries', 0)
    cacheOverflow = myStats.get('cache.overflow', 0)
    cacheBulkQuerySizes = myStats.get('cacheBulkQuerySize', [])

    # Calculate cache-data-structure-derived metrics prior to storing anything
    # in the cache itself -- which would otherwise affect said metrics.
    cache_size = cache.MetricCache.size
    cache_queues = len(cache.MetricCache)
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
    record('errors', errors)
    record('cache.queries', cacheQueries)
    record('cache.bulk_queries', cacheBulkQueries)
    record('cache.overflow', cacheOverflow)
    record('persist.queues', myStats.get('persist.queues', 0))
    record('persist.size', myStats.get('persist.size', 0))
    record('persist.fileSize', myStats.get('persist.fileSize', 0))
    record('persist.fileGeneration', myStats.get('persist.fileGeneration', 0))
    if settings.CACHE_WRITE_STRATEGY == "tuned":
      record('tuned.largest.len1',  statsPersist.get('tuned.largest.len1',  0))
      record('tuned.largest.len2',  statsPersist.get('tuned.largest.len2',  0))
      record('tuned.largest.rate',  statsPersist.get('tuned.largest.rate',  0))
      record('tuned.largest.count', statsPersist.get('tuned.largest.count', 0))
      record('tuned.largest.size',  statsPersist.get('tuned.largest.size',  0))
      record('tuned.random.rate',   statsPersist.get('tuned.random.rate',   0))
      record('tuned.random.count',  statsPersist.get('tuned.random.count',  0))
      record('tuned.random.size',   statsPersist.get('tuned.random.size',   0))
      record('tuned.oldest.late1',  statsPersist.get('tuned.oldest.late1',  0))
      record('tuned.oldest.late2',  statsPersist.get('tuned.oldest.late2',  0))
      record('tuned.oldest.rate',   statsPersist.get('tuned.oldest.rate',   0))
      record('tuned.oldest.count',  statsPersist.get('tuned.oldest.count',  0))
      record('tuned.oldest.size',   statsPersist.get('tuned.oldest.size',   0))
      record('tuned.flush.rate',    statsPersist.get('tuned.flush.rate',    0))
      record('tuned.flush.count',   statsPersist.get('tuned.flush.count',   0))
      record('tuned.flush.size',    statsPersist.get('tuned.flush.size',    0))
      record('tuned.global.rate',   statsPersist.get('tuned.global.rate',   0))
      record('tuned.global.count',  statsPersist.get('tuned.global.count',  0))
      record('tuned.global.size',   statsPersist.get('tuned.global.size',   0))


  # aggregator metrics
  elif settings.program == 'carbon-aggregator':
    record = aggregator_record
    record('allocatedBuffers', len(BufferManager))
    record('bufferedDatapoints',
           sum([b.size for b in BufferManager.buffers.values()]))
    record('aggregateDatapointsSent', myStats.get('aggregateDatapointsSent', 0))

  # relay metrics
  else:
    record = relay_record
    prefix = 'destinations.'
    relay_stats =  [(k,v) for (k,v) in myStats.items() if k.startswith(prefix)]
    for stat_name, stat_value in relay_stats:
      record(stat_name, stat_value)

  # common metrics
  record('metricsReceived', myStats.get('metricsReceived', 0))
  record('blacklistMatches', myStats.get('blacklistMatches', 0))
  record('whitelistRejects', myStats.get('whitelistRejects', 0))
  record('cpuUsage', getCpuUsage())
  try: # This only works on Linux
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
    cache.MetricCache.store(fullMetric, datapoint)

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
from carbon import state, events, cache
from carbon.aggregator.buffers import BufferManager
