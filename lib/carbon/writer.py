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
from carbon.conf import settings
from carbon import log, events, instrumentation
from carbon.util import TokenBucket

from twisted.internet import reactor
from twisted.application.service import Service

try:
    import signal
except ImportError:
    log.debug("Couldn't import signal module")


CACHE_SIZE_LOW_WATERMARK = settings.MAX_CACHE_SIZE * 0.95



class Writer:
    def __init__(self, metric_cache, storage, max_creates_per_minute = settings.MAX_CREATES_PER_MINUTE,
                 max_updates_per_second = settings.MAX_UPDATES_PER_SECOND):
      self.metric_cache = metric_cache
      self.storage = storage

      # Inititalize token buckets so that we can enforce rate limits on creates and
      # updates if the config wants them.
      self.create_bucket = None
      self.update_bucket = None
      if max_creates_per_minute != float('inf'):
        capacity = max_creates_per_minute
        fill_rate = float(max_creates_per_minute) / 60
        self.create_bucket = TokenBucket(capacity, fill_rate)

      if max_updates_per_second != float('inf'):
        capacity = max_updates_per_second
        fill_rate = max_updates_per_second
        self.update_bucket = TokenBucket(capacity, fill_rate)

    def optimalWriteOrder(self):
      """Generates metrics with the most cached values first and applies a soft
      rate limit on new metrics"""
      while self.metric_cache:
        (metric, datapoints) = self.metric_cache.drain_metric()
        if state.cacheTooFull and self.metric_cache.size < CACHE_SIZE_LOW_WATERMARK:
          events.cacheSpaceAvailable()

        dbFilePath = self.storage.getFilesystemPath(metric)
        dbFileExists = exists(dbFilePath)

        if not dbFileExists and self.create_bucket:
          # If our tokenbucket has enough tokens available to create a new metric
          # file then yield the metric data to complete that operation. Otherwise
          # we'll just drop the metric on the ground and move on to the next
          # metric.
          # XXX This behavior should probably be configurable to no tdrop metrics
          # when rate limitng unless our cache is too big or some other legit
          # reason.
          if self.create_bucket.drain(1):
            yield (metric, datapoints, dbFilePath, dbFileExists)
          continue

        yield (metric, datapoints, dbFilePath, dbFileExists)


    def writeCachedDataPoints(self):
      "Write datapoints until the self.metric_cache is completely empty"

      while self.metric_cache:
        dataWritten = False

        for (metric, datapoints, dbFilePath, dbFileExists) in self.optimalWriteOrder():
          dataWritten = True

          if not dbFileExists:
            archiveConfig = None
            xFilesFactor, aggregationMethod = None, None

            for schema in self.storage.storage_schemas:
              if schema.matches(metric):
                log.creates('new metric %s matched schema %s' % (metric, schema.name))
                archiveConfig = [archive.getTuple() for archive in schema.archives]
                break

            for schema in self.storage.aggregation_schemas:
              if schema.matches(metric):
                log.creates('new metric %s matched aggregation schema %s' % (metric, schema.name))
                xFilesFactor, aggregationMethod = schema.archives
                break

            if not archiveConfig:
              raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % metric)

            dbDir = dirname(dbFilePath)
            try:
                if not exists(dbDir):
                    os.makedirs(dbDir)
            except OSError, e:
                log.err("%s" % e)
            log.creates("creating database file %s (archive=%s xff=%s agg=%s)" %
                        (dbFilePath, archiveConfig, xFilesFactor, aggregationMethod))
            try:
                whisper.create(
                    dbFilePath,
                    archiveConfig,
                    xFilesFactor,
                    aggregationMethod,
                    settings.WHISPER_SPARSE_CREATE,
                    settings.WHISPER_FALLOCATE_CREATE)
                instrumentation.increment('creates')
            except:
                log.err("Error creating %s" % (dbFilePath))
                continue
          # If we've got a rate limit configured lets makes sure we enforce it
          if self.update_bucket:
            self.update_bucket.drain(1, blocking=True)
          try:
            t1 = time.time()
            whisper.update_many(dbFilePath, datapoints)
            updateTime = time.time() - t1
          except Exception:
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


    def writeForever(self):
      while reactor.running:
        try:
          self.writeCachedDataPoints()
        except Exception:
          log.err()
        time.sleep(1)  # The writer thread only sleeps when the cache is empty or an error occurs

    def shutdownModifyUpdateSpeed(self):
        try:
            shut = settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN
            if self.update_bucket:
              self.update_bucket.setCapacityAndFillRate(shut,shut)
            if self.create_bucket:
              self.create_bucket.setCapacityAndFillRate(shut,shut)
            log.msg("Carbon shutting down.  Changed the update rate to: " + str(settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN))
        except KeyError:
            log.msg("Carbon shutting down.  Update rate not changed")


class WriterService(Service):

    def __init__(self, writer):
        self.writer = writer

    def startService(self):
        if 'signal' in globals().keys():
          log.debug("Installing SIG_IGN for SIGHUP")
          signal.signal(signal.SIGHUP, signal.SIG_IGN)
        self.writer.storage.startRefreshes()
        reactor.addSystemEventTrigger('before', 'shutdown', self.writer.shutdownModifyUpdateSpeed)
        reactor.callInThread(self.writer.writeForever)
        Service.startService(self)

    def stopService(self):
        self.writer.storage.stopRefreshes()
        Service.stopService(self)
