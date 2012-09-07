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

import os
import time
from os.path import exists, dirname

import whisper
from carbon import state
from carbon.cache import MetricCache
from carbon.storage import getFilesystemPath, loadStorageSchemas,\
    loadAggregationSchemas
from carbon.conf import settings
from carbon import log, events, instrumentation
from carbon.util import TokenBucket

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.application.service import Service


lastCreateInterval = 0
createCount = 0
schemas = loadStorageSchemas()
agg_schemas = loadAggregationSchemas()
CACHE_SIZE_LOW_WATERMARK = settings.MAX_CACHE_SIZE * 0.95


# Inititalize a few buckets so that we can enforce rate limits on creates and
# updates if the config wants them.
if settings.MAX_CREATES_PER_MINUTE != float('inf'):
  if settings.MAX_CREATES_PER_MINUTE < 1:
    capacity = (1 / settings.MAX_CREATES_PER_MINUTE) * 60.0
  else:
    capacity = 60
  create_bucket = TokenBucket(capacity, 1)
  create_cost = 60.0 / settings.MAX_CREATES_PER_MINUTE
else:
  create_bucket = None


if settings.MAX_UPDATES_PER_SECOND != float('inf'):
  update_bucket = TokenBucket(60, 1)
  update_cost = 1.0 / settings.MAX_UPDATES_PER_SECOND
else:
  update_bucket = None


def optimalWriteOrder():
  """Generates metrics with the most cached values first and applies a soft
  rate limit on new metrics"""
  while MetricCache:
    (metric, datapoints) = MetricCache.pop()
    if state.cacheTooFull and MetricCache.size < CACHE_SIZE_LOW_WATERMARK:
      events.cacheSpaceAvailable()

    dbFilePath = getFilesystemPath(metric)
    dbFileExists = exists(dbFilePath)

    if not dbFileExists and create_bucket:
      # If our tokenbucket has enough tokens available to create a new metric
      # file then yield the metric data to complete that operation. Otherwise
      # we'll just drop the metric on the ground and move on to the next
      # metric.
      # XXX This behavior should probably be configurable to no tdrop metrics
      # when rate limitng unless our cache is too big or some other legit
      # reason.
      if create_bucket.drain(create_cost):
        yield (metric, datapoints, dbFilePath, dbFileExists)
      continue

    yield (metric, datapoints, dbFilePath, dbFileExists)


def writeCachedDataPoints():
  "Write datapoints until the MetricCache is completely empty"

  while MetricCache:
    dataWritten = False

    for (metric, datapoints, dbFilePath, dbFileExists) in optimalWriteOrder():
      dataWritten = True

      if not dbFileExists:
        archiveConfig = None
        xFilesFactor, aggregationMethod = None, None

        for schema in schemas:
          if schema.matches(metric):
            log.creates('new metric %s matched schema %s' % (metric, schema.name))
            archiveConfig = [archive.getTuple() for archive in schema.archives]
            break

        for schema in agg_schemas:
          if schema.matches(metric):
            log.creates('new metric %s matched aggregation schema %s' % (metric, schema.name))
            xFilesFactor, aggregationMethod = schema.archives
            break

        if not archiveConfig:
          raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)

        dbDir = dirname(dbFilePath)
        try:
            if not exists(dbDir):
                os.makedirs(dbDir, 0755)
        except OSError as e:
            log.err("%s" % e)
        log.creates("creating database file %s (archive=%s xff=%s agg=%s)" %
                    (dbFilePath, archiveConfig, xFilesFactor, aggregationMethod))
        whisper.create(
            dbFilePath,
            archiveConfig,
            xFilesFactor,
            aggregationMethod,
            settings.WHISPER_SPARSE_CREATE,
            settings.WHISPER_FALLOCATE_CREATE)
        instrumentation.increment('creates')
      # If we've got a rate limit configured lets makes sure we enforce it with
      # a little busy loop.
      if update_bucket:
        while not update_bucket.drain(update_cost):
          time.sleep(0.1)
      try:
        t1 = time.time()
        whisper.update_many(dbFilePath, datapoints)
        updateTime = time.time() - t1
      except:
        log.msg("Error writing to %s" % (dbFilePath))
        log.err()
        instrumentation.increment('errors')
      else:
        pointCount = len(datapoints)
        instrumentation.increment('committedPoints', pointCount)
        instrumentation.append('updateTimes', updateTime)
        if settings.LOG_UPDATES:
          log.updates("wrote %d datapoints for %s in %.5f seconds" % (pointCount, metric, updateTime))

    # Avoid churning CPU when only new metrics are in the cache
    if not dataWritten:
      time.sleep(0.1)


def writeForever():
  while reactor.running:
    try:
      writeCachedDataPoints()
    except:
      log.err()
    time.sleep(1)  # The writer thread only sleeps when the cache is empty or an error occurs


def reloadStorageSchemas():
  global schemas
  try:
    schemas = loadStorageSchemas()
  except:
    log.msg("Failed to reload storage schemas")
    log.err()


def reloadAggregationSchemas():
  global agg_schemas
  try:
    agg_schemas = loadAggregationSchemas()
  except:
    log.msg("Failed to reload aggregation schemas")
    log.err()


def shutdownModifyUpdateSpeed():
    try:
        settings.MAX_UPDATES_PER_SECOND = settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN
        log.msg("Carbon shutting down.  Changed the update rate to: " + str(settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN))
    except KeyError:
        log.msg("Carbon shutting down.  Update rate not changed")


class WriterService(Service):

    def __init__(self):
        self.storage_reload_task = LoopingCall(reloadStorageSchemas)
        self.aggregation_reload_task = LoopingCall(reloadAggregationSchemas)

    def startService(self):
        self.storage_reload_task.start(60, False)
        self.aggregation_reload_task.start(60, False)
        reactor.addSystemEventTrigger('before', 'shutdown', shutdownModifyUpdateSpeed)
        reactor.callInThread(writeForever)
        Service.startService(self)

    def stopService(self):
        self.storage_reload_task.stop()
        self.aggregation_reload_task.stop()
        Service.stopService(self)
