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

import time, os
import re
import random
from collections import deque
from carbon.conf import settings
from carbon.regexlist import FlushList
try:
    from collections import defaultdict
except ImportError:
    from util import defaultdict


class _MetricCache(defaultdict):
  def __init__(self, defaultfactory=deque, method="sorted"):
    self.size = 0
    self.method = method
    if self.method == "sorted":
      self.queue = self.gen_queue()
    elif self.method == "tuned":
      self.queue = self.gen_queue_tuned()
    else:
      self.queue = False
    super(_MetricCache, self).__init__(defaultfactory)

  def gen_queue(self):
    while True:
      t = time.time()
      queue = sorted(self.counts, key=lambda x: x[1])
      if settings.LOG_CACHE_QUEUE_SORTS:
        log.msg("Sorted %d cache queues in %.6f seconds" % (len(queue), time.time() - t))
      while queue:
        yield queue.pop()[0]

  def gen_queue_tuned(self):

    while True:

      start = time.time()
      t = time.time()
      g_count = 0
      g_size = 0
      queue = sorted(self.counts, key=lambda x: x[1])
      if settings.LOG_CACHE_QUEUE_SORTS:
        log.msg("[tuned#1] sorted %d queues in %.2f seconds" % (len(queue), time.time() - t))

      # parse config
      m = re.search("^(?P<val1>\d+(?:\.\d+)?)(?P<unit1>[%s]?)$", str(settings.CACHE_WRITE_TUNED_STRATEGY_LARGEST))
      if m:
        if m.group("unit1") == "s":
          timelimit1 = int(m.group("val1"))
          limit1 = -1
        elif m.group("unit1") == "%":
          timelimit1 = -1
          limit1 = len(queue) * float(m.group("val1")) / 100
        else:
          timelimit1 = -1
          limit1 = int(m.group("val1"))
      else:
        limit1 = len(queue) * 0.01
        timelimit1 = -1

      m = re.search("^(?P<val2>\d+(?:\.\d+)?)(?P<unit2>[%s]?)$", str(settings.CACHE_WRITE_TUNED_STRATEGY_RANDOM))
      if m:
        if m.group("unit2") == "s":
          timelimit2 = int(m.group("val2"))
          limit2 = -1
        elif m.group("unit2") == "%":
          timelimit2 = -1
          limit2 = len(queue) * float(m.group("val2")) / 100
        else:
          timelimit2 = -1
          limit2 = int(m.group("val2"))
      else:
        limit2 = 0
        timelimit2 = 60

      m = re.search("^(?P<val3>\d+(?:\.\d+)?)(?P<unit3>[%s]?)$", str(settings.CACHE_WRITE_TUNED_STRATEGY_OLDEST))
      if m:
        if m.group("unit3") == "s":
          timelimit3 = int(m.group("val3"))
          limit3 = -1
        elif m.group("unit3") == "%":
          timelimit3 = -1
          limit3 = len(queue) * float(m.group("val3")) / 100
        else:
          timelimit3 = -1
          limit3 = int(m.group("val3"))
      else:
        limit3 = 0
        timelimit3 = 100


      # Step 1 : got largest queues (those with lots of metrics)
      if limit1 > 0 or timelimit1 > 0:
        t = time.time()
        count = 0
        size = 0
        len1 = 0
        len2 = 0
        while queue:
          if (limit1 != -1 and count >= limit1) or (timelimit1 != -1 and time.time() - t >= timelimit1):
            break
          metric = queue.pop()
          count += 1
          size += len(self[metric[0]])
          if count == 1:
            len1 = metric[1]
          len2 = metric[1]
          yield metric[0]
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("[tuned#1] written %d queues/%d metrics in %.2f seconds (%.2f queues/sec %.2f metrics/sec) (more numerous metrics/queue : %d -> %d)" % (count, size, time.time() - t, count / (time.time() - t), size / (time.time() - t), len1, len2))
        g_size += size
        g_count += count

      # Step 2 : got random queues
      if limit2 > 0 or timelimit2 > 0:
        t = time.time()
        random.shuffle(queue)
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("[tuned#2] shuffled %d queues in %.2f seconds" % (len(queue), time.time() - t))

        t = time.time()
        count = 0
        size = 0
        while queue:
          if (limit2 != -1 and count >= limit2) or (timelimit2 != -1 and time.time() - t >= timelimit2):
            break
          metric = queue.pop()
          count += 1
          size += len(self[metric[0]])
          yield metric[0]
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("[tuned#2] written %d queues/%d metrics in %.2f seconds (%.2f queues/sec %.2f metrics/sec) (random)" % (count, size, time.time() - t, count / (time.time() - t), size / (time.time() - t)))
        g_size += size
        g_count += count

      # Step 3 : got oldest queues (those with oldest metrics)
      if limit3 > 0 or timelimit3 > 0:
        t = time.time()
        ordered = sorted(self.oldest, key=lambda x: x[1], reverse=True)
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("[tuned#3] sorted %d queues in %.2f seconds" % (len(queue), time.time() - t))

        t = time.time()
        count = 0
        size = 0
        ts1 = 0
        ts2 = 0
        while ordered:
          if (limit3 != -1 and count >= limit3) or (timelimit3 != -1 and time.time() - t >= timelimit3):
            break
          metric = ordered.pop()
          count += 1
          size += len(self[metric[0]])
          if count == 1:
            ts1 = time.time() - metric[1]
          ts2 = time.time() - metric[1]
          yield metric[0]
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("[tuned#3] written %d queues/%d metrics in %.2f seconds (%.2f queues/sec %.2f metrics/sec) (oldest : %d sec -> %d sec late)" % (count, size, time.time() - t, count / (time.time() - t), size / (time.time() - t), int(ts1), int(ts2)))
        g_size += size
        g_count += count

      # Step 4 : got from flushlist (config file)
      if FlushList is not None and len(queue) != 0:
        t = time.time()
        count = 0
        size = 0
        for metric in self.items():
          if metric[0] in FlushList:
            count += 1
            size += len(self[metric[0]])
            yield metric[0]
        if count != 0:
          if settings.LOG_CACHE_QUEUE_SORTS:
            log.msg("[tuned#4] written %d queues/%d metrics in %.2f seconds (%.2f queues/sec %.2f metrics/sec) (flushlist)" % (count, size, time.time() - t, count / (time.time() - t), size / (time.time() - t)))
        g_size += size
        g_count += count

      if settings.LOG_CACHE_QUEUE_SORTS:
        log.msg("[tuned##] written %d queues/%d metrics in %.2f second (%.2f queues/sec %.2f metrics/sec) (global)" % (g_count, g_size, time.time() - start, g_count / (time.time() - start), g_size / (time.time() - start)))

  def shutdown(self):
    # change to simple dequeuing system. generator will not be used any more
    self.method = "naive"

  def store(self, metric, datapoint):
    self.size += 1
    self[metric].append(datapoint)
    if self.isFull():
      log.msg("MetricCache is full: self.size=%d" % self.size)
      state.events.cacheFull()

  def isFull(self):
    # Short circuit this test if there is no max cache size, then we don't need
    # to do the someone expensive work of calculating the current size.
    return settings.MAX_CACHE_SIZE != float('inf') and self.size >= settings.MAX_CACHE_SIZE

  def pop(self, metric=None):
    if not self:
      raise KeyError(metric)
    elif not metric and self.method == "max":
      metric = max(self.items(), key=lambda x: len(x[1]))[0]
      datapoints = (metric, super(_MetricCache, self).pop(metric))
    elif not metric and self.method == "naive":
      datapoints = self.popitem()
    elif not metric and self.method == "sorted" or self.method == "tuned":
      metric = self.queue.next()

      # async flushing events could cause errors (if metric already popped)
      try:
        popped = super(_MetricCache, self).pop(metric)
        # Save only last value for each timestamp
        ordered = sorted(dict(popped).items(), key=lambda x: x[0])
        datapoints = (metric, deque(ordered))
      except KeyError, e:
        # got 1st item available
        datapoints = self.popitem()

    self.size -= len(datapoints[1])
    return datapoints

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]

  @property
  def oldest(self):
    try:
      return [(metric, sorted(datapoints)[0][0]) for (metric, datapoints) in self.items()]
    except Exception, e:
      log.exception(e)
      log.msg("failed to create oldest list (%s)" % e)
      return []

  def loadPersistMetricCache(self):
    persist_file = "%s/%s" % (settings.LOG_DIR, settings.CACHE_PERSIST_FILE)
    if not os.path.isfile(persist_file):
      return
    log.msg("Loading cache from persistant file : %s" % persist_file)

    t = time.time()
    try:
      queues = 0
      size = 0
      persist = open(persist_file, "r")
      for line in persist:
        try:
          fields = line.split(" ")
          metric = fields.pop(0)
          if len(fields) == 0:
            continue
          queues += 1
          while len(fields):
            value = float(fields.pop(0))
            timestamp = float(fields.pop(0))
            size += 1
            datapoint = (float(timestamp), float(value))
            self.store(metric, datapoint)
        except Exception, e:
          log.msg("Persisted cache : invalid data : %s" % line)
      persist.close()
      log.msg("Loaded persisted cache in %.2f seconds (metrics:%d queues:%d filesize:%d)" % (time.time() - t, size, queues, os.path.getsize(persist_file)))
    except Exception, e:
      log.err()
      log.msg("Unable to load persisted cache : %s" % persist_file)
      persist.close()

  def savePersistMetricCache(self, flush = False):
    started = time.time()
    metrics = self.items()
    persist_file = "%s/%s" % (settings.LOG_DIR, settings.CACHE_PERSIST_FILE)
    persist_file_tmp = "%s.tmp" % persist_file
    try:
      if os.path.isfile(persist_file):
        mtime = os.path.getmtime(persist_file)

        # try to protect from too many API calls (unless shutdown)
        if time.time() - mtime <= 60 and flush == False:
          log.msg("Cancel persist cache task (min interval : 60 sec)")
          return
    except OSError:
      log.err("Failed to get mtime of %s" % persist_file)
      return

    log.msg("Starting to save cache to persistant file : %s" % persist_file)

    if os.path.isfile(persist_file_tmp):
      os.unlink(persist_file_tmp)

    try:
      persist = open(persist_file_tmp, "w")
      queues = len(metrics)
      size = 0

      # do not pop metrics unless shutdown mode
      if flush == False:
        for metric, datapoints in metrics:
          size += len(datapoints)
          points = ""
          for datapoint in datapoints:
            points += " %s %s" % (datapoint[1], int(datapoint[0]))
          persist.write("%s%s\n" % (metric, points))
      else:
        while MetricCache:
          (metric, datapoints) = self.pop()
          size += len(datapoints)
          points = ""
          for datapoint in datapoints:
            points += " %s %s" % (datapoint[1], int(datapoint[0]))
          persist.write("%s%s\n" % (metric, points))

      persist.close()

      if os.path.isfile(persist_file):
        os.unlink(persist_file)

      os.rename(persist_file_tmp, persist_file)
    except Exception, e:
      log.msg("Error : unable to create persist file %s (%s)" % (persist_file, e))
      log.err()
      persist.close()
      return

    stopped = time.time()
    if flush == False:
      from carbon import instrumentation
      instrumentation.set('persist.queues', queues)
      instrumentation.set('persist.size', size)
      instrumentation.set('persist.fileSize', os.path.getsize(persist_file))
      instrumentation.set('persist.fileGeneration', stopped-started)
    log.msg("Persisted cache saved in %.2f seconds (metrics:%d queues:%d filesize:%d)" % (stopped-started, size, queues, os.path.getsize(persist_file)))


# Ghetto singleton

MetricCache = _MetricCache(method=settings.CACHE_WRITE_STRATEGY)


# Avoid import circularities
from carbon import log, state
