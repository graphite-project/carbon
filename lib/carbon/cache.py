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
from collections import deque
from carbon.conf import settings
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
    elif not metric and self.method == "sorted":
      metric = self.queue.next()
      # Save only last value for each timestamp
      popped = super(_MetricCache, self).pop(metric)
      ordered = sorted(dict(popped).items(), key=lambda x: x[0])
      datapoints = (metric, deque(ordered))
    self.size -= len(datapoints[1])
    return datapoints

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]

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
