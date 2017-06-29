"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import time

from carbon import state
from carbon.cache import MetricCache
from carbon.storage import loadStorageSchemas, loadAggregationSchemas
from carbon.conf import settings
from carbon import log, events, instrumentation
from carbon.util import TokenBucket

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.application.service import Service

try:
    import signal
except ImportError:
    log.msg("Couldn't import signal module")


SCHEMAS = loadStorageSchemas()
AGGREGATION_SCHEMAS = loadAggregationSchemas()


# Inititalize token buckets so that we can enforce rate limits on creates and
# updates if the config wants them.
CREATE_BUCKET = None
UPDATE_BUCKET = None
if settings.MAX_CREATES_PER_MINUTE != float('inf'):
  capacity = settings.MAX_CREATES_PER_MINUTE
  fill_rate = float(settings.MAX_CREATES_PER_MINUTE) / 60
  CREATE_BUCKET = TokenBucket(capacity, fill_rate)

if settings.MAX_UPDATES_PER_SECOND != float('inf'):
  capacity = settings.MAX_UPDATES_PER_SECOND
  fill_rate = settings.MAX_UPDATES_PER_SECOND
  UPDATE_BUCKET = TokenBucket(capacity, fill_rate)


def optimalWriteOrder():
  """Generates metrics with the most cached values first and applies a soft
  rate limit on new metrics"""
  cache = MetricCache()
  while cache:
    (metric, datapoints) = cache.drain_metric()
    if metric is None:
        break

    dbFileExists = state.database.exists(metric)

    if not dbFileExists and CREATE_BUCKET:
      # If our tokenbucket has enough tokens available to create a new metric
      # file then yield the metric data to complete that operation. Otherwise
      # we'll just drop the metric on the ground and move on to the next
      # metric.
      # XXX This behavior should probably be configurable to no tdrop metrics
      # when rate limitng unless our cache is too big or some other legit
      # reason.
      if CREATE_BUCKET.drain(1):
        yield (metric, datapoints, dbFileExists)
      continue

    yield (metric, datapoints, dbFileExists)


def writeCachedDataPoints():
  "Write datapoints until the MetricCache is completely empty"

  cache = MetricCache()
  while cache:
    dataWritten = False

    for (metric, datapoints, dbFileExists) in optimalWriteOrder():
      dataWritten = True

      if not dbFileExists:
        archiveConfig = None
        xFilesFactor, aggregationMethod = None, None

        for schema in SCHEMAS:
          if schema.matches(metric):
            if settings.LOG_CREATES:
              log.creates('new metric %s matched schema %s' % (metric, schema.name))
            archiveConfig = [archive.getTuple() for archive in schema.archives]
            break

        for schema in AGGREGATION_SCHEMAS:
          if schema.matches(metric):
            if settings.LOG_CREATES:
              log.creates('new metric %s matched aggregation schema %s'
                          % (metric, schema.name))
            xFilesFactor, aggregationMethod = schema.archives
            break

        if not archiveConfig:
          raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)

        if settings.LOG_CREATES:
          log.creates("creating database metric %s (archive=%s xff=%s agg=%s)" %
                      (metric, archiveConfig, xFilesFactor, aggregationMethod))
        try:
            state.database.create(metric, archiveConfig, xFilesFactor, aggregationMethod)
            instrumentation.increment('creates')
        except Exception as e:
            log.err()
            log.msg("Error creating %s: %s" % (metric, e))
            instrumentation.increment('errors')
            continue
      # If we've got a rate limit configured lets makes sure we enforce it
      if UPDATE_BUCKET:
        UPDATE_BUCKET.drain(1, blocking=True)
      try:
        t1 = time.time()
        # If we have duplicated points, always pick the last. update_many()
        # has no guaranted behavior for that, and in fact the current implementation
        # will keep the first point in the list.
        datapoints = dict(datapoints).items()
        state.database.write(metric, datapoints)
        updateTime = time.time() - t1
      except Exception as e:
        log.err()
        log.msg("Error writing to %s: %s" % (metric, e))
        instrumentation.increment('errors')
      else:
        pointCount = len(datapoints)
        instrumentation.increment('committedPoints', pointCount)
        instrumentation.append('updateTimes', updateTime)
        if settings.LOG_UPDATES:
          log.updates("wrote %d datapoints for %s in %.5f seconds" % (pointCount, metric, updateTime))

    # Avoid churning CPU when only new metrics are in the cache
    if not dataWritten:
      time.sleep(1)


def writeForever():
  while reactor.running:
    try:
      writeCachedDataPoints()
    except Exception:
      log.err()
      # Back-off on error to let time to the backend to recover.
      time.sleep(0.1)
    else:
      # Avoid churning CPU when there are no metrics are in the cache
      time.sleep(1)


def reloadStorageSchemas():
  global SCHEMAS
  try:
    SCHEMAS = loadStorageSchemas()
  except Exception as e:
    log.msg("Failed to reload storage SCHEMAS: %s" % (e))


def reloadAggregationSchemas():
  global AGGREGATION_SCHEMAS
  try:
    AGGREGATION_SCHEMAS = loadAggregationSchemas()
  except Exception as e:
    log.msg("Failed to reload aggregation SCHEMAS: %s" % (e))


def shutdownModifyUpdateSpeed():
    try:
        shut = settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN
        if UPDATE_BUCKET:
          UPDATE_BUCKET.setCapacityAndFillRate(shut,shut)
        if CREATE_BUCKET:
          CREATE_BUCKET.setCapacityAndFillRate(shut,shut)
        log.msg("Carbon shutting down.  Changed the update rate to: " + str(settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN))
    except KeyError:
        log.msg("Carbon shutting down.  Update rate not changed")

    # Also set MIN_TIMESTAMP_LAG to 0 to avoid waiting for nothing.
    settings.MIN_TIMESTAMP_LAG = 0


class WriterService(Service):

    def __init__(self):
        self.storage_reload_task = LoopingCall(reloadStorageSchemas)
        self.aggregation_reload_task = LoopingCall(reloadAggregationSchemas)

    def startService(self):
        if 'signal' in globals().keys():
          log.msg("Installing SIG_IGN for SIGHUP")
          signal.signal(signal.SIGHUP, signal.SIG_IGN)
        self.storage_reload_task.start(60, False)
        self.aggregation_reload_task.start(60, False)
        reactor.addSystemEventTrigger('before', 'shutdown', shutdownModifyUpdateSpeed)
        reactor.callInThread(writeForever)
        Service.startService(self)

    def stopService(self):
        self.storage_reload_task.stop()
        self.aggregation_reload_task.stop()
        Service.stopService(self)
