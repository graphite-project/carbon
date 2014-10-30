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
import errno
from threading import Lock

import whisper
from carbon import state
from carbon.cache import MetricCache
from carbon.storage import getFilesystemPath, loadStorageSchemas,\
    loadAggregationSchemas
from carbon.conf import settings
from carbon import log, events, instrumentation

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.internet.threads import deferToThread
from twisted.application.service import Service

try:
    import signal
except ImportError:
    log.debug("Couldn't import signal module")


lastCreateInterval = 0
createCount = 0
schemas = None
agg_schemas = None
CACHE_SIZE_LOW_WATERMARK = settings.MAX_CACHE_SIZE * 0.95
write_lock = Lock()


def optimalWriteOrder():
  """Generates metrics with the most cached values first and applies a soft
  rate limit on new metrics"""
  global lastCreateInterval
  global createCount
  metrics = MetricCache.counts()

  t = time.time()
  metrics.sort(key=lambda item: item[1], reverse=True)  # by queue size, descending
  log.debug("Sorted %d cache queues in %.6f seconds" % (len(metrics),
                                                        time.time() - t))

  for metric, queueSize in metrics:
    if state.cacheTooFull and MetricCache.size < CACHE_SIZE_LOW_WATERMARK:
      events.cacheSpaceAvailable()

    dbFilePath = getFilesystemPath(metric)
    dbFileExists = exists(dbFilePath)

    if not dbFileExists:
      createCount += 1
      now = time.time()

      if now - lastCreateInterval >= 60:
        lastCreateInterval = now
        createCount = 1

      elif createCount >= settings.MAX_CREATES_PER_MINUTE:
        # dropping queued up datapoints for new metrics prevents filling up the entire cache
        # when a bunch of new metrics are received.
        try:
          MetricCache.pop(metric)
        except KeyError:
          pass
        instrumentation.increment('droppedCreates')
        continue

    try:  # metrics can momentarily disappear from the MetricCache due to the implementation of MetricCache.store()
      datapoints = MetricCache.pop(metric)
    except KeyError:
      log.msg("MetricCache contention, skipping %s update for now" % metric)
      continue  # we simply move on to the next metric when this race condition occurs

    yield (metric, datapoints, dbFilePath, dbFileExists)

def createWhisperFile(metric, dbFilePath, dbFileExists):
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
      os.makedirs(dbDir)
    except OSError as e:
      if e.errno != errno.EEXIST:
        log.err("%s" % e)
    log.creates("creating database file %s (archive=%s xff=%s agg=%s)" %
                (dbFilePath, archiveConfig, xFilesFactor, aggregationMethod))
    try:
      whisper.create(dbFilePath, archiveConfig, xFilesFactor, aggregationMethod, settings.WHISPER_SPARSE_CREATE, settings.WHISPER_FALLOCATE_CREATE)
      instrumentation.increment('creates')
    except Exception, e:
      log.err("Error creating %s: %s" % (dbFilePath, e))
      return False
  return True

def writeWhisperFile(dbFilePath, datapoints):
  try:
    whisper.update_many(dbFilePath, datapoints)
  except:
    log.msg("Error writing to %s" % (dbFilePath))
    log.err()
    instrumentation.increment('errors')
    return False
  return True

def writeCachedDataPoints():
  "Write datapoints until the MetricCache is completely empty"
  updates = 0
  lastSecond = 0
  while MetricCache:
    dataWritten = False

    for (metric, datapoints, dbFilePath, dbFileExists) in optimalWriteOrder():
      dataWritten = True
      write_lock.acquire()
      try:
          if not createWhisperFile(metric, dbFilePath, dbFileExists):
              continue
          t1 = time.time()
          written = writeWhisperFile(dbFilePath, datapoints)
      finally:
        write_lock.release()
      if written:
        t2 = time.time()
        updateTime = t2 - t1
        pointCount = len(datapoints)
        instrumentation.increment('committedPoints', pointCount)
        instrumentation.append('updateTimes', updateTime)

        if settings.LOG_UPDATES:
          log.updates("wrote %d datapoints for %s in %.5f seconds" % (pointCount, metric, updateTime))
        # Rate limit update operations
        thisSecond = int(t2)

        if thisSecond != lastSecond:
          lastSecond = thisSecond
          updates = 0
        else:
          updates += 1
          if updates >= settings.MAX_UPDATES_PER_SECOND:
            time.sleep(int(t2 + 1) - t2)

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


def reloadStorageSchemas(first_run=False):
  global schemas
  try:
    schemas = loadStorageSchemas()
  except Exception, e:
    if first_run:
        raise e
    log.msg("Failed to reload storage schemas")
    log.err()


def reloadAggregationSchemas(first_run=False):
  global agg_schemas
  try:
    agg_schemas = loadAggregationSchemas()
  except Exception, e:
    if first_run:
      raise e
    log.msg("Failed to reload aggregation schemas")
    log.err()


def shutdownModifyUpdateSpeed():
    try:
        settings.MAX_UPDATES_PER_SECOND = settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN
        log.msg("Carbon shutting down.  Changed the update rate to: " + str(settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN))
    except KeyError:
        log.msg("Carbon shutting down.  Update rate not changed")


def _flush(prefix=None):
    """ Write/create whisped files at maximal speed """
    assert(prefix==None or hasattr(prefix, 'startswith'))
    log.msg("flush started (prefix: %s)" % prefix)
    started = time.time()
    metrics = MetricCache.counts()
    updates = 0
    write_lock.acquire()
    try:
        for metric, queueSize in metrics:
            if prefix and not metric.startswith(prefix):
                continue
            dbFilePath = getFilesystemPath(metric)
            dbFileExists = exists(dbFilePath)
            try:
                datapoints = MetricCache.pop(metric)
            except KeyError:
                continue
            if not createWhisperFile(metric, dbFilePath, dbFileExists):
                continue
            if not writeWhisperFile(dbFilePath, datapoints):
                continue
    	    updates += 1
    finally:
        write_lock.release()
    log.msg('flush finished (updates: %d, time: %.5f sec)' % (updates, time.time()-started))
    return updates

def flush(prefix=None):
    """ Flush cache in separate thread """
    reactor.callInThread(_flush, prefix)

class WriterService(Service):

    def __init__(self):
        # first start on service initialization
        reloadStorageSchemas(True)
        reloadAggregationSchemas(True)
        self.storage_reload_task = LoopingCall(reloadStorageSchemas)
        self.aggregation_reload_task = LoopingCall(reloadAggregationSchemas)

    def startService(self):
        if 'signal' in globals().keys():
          log.debug("Installing SIG_IGN for SIGHUP")
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
