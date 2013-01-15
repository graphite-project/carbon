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
from threading import Lock
from carbon.conf import settings
from carbon import log, instrumentation
from carbon.pipeline import Processor


instrumentation.configure_stats('pipeline.cache_microseconds', ('total', 'min', 'max', 'avg'))

ONE_MILLION = 1000000 # I hate counting zeroes

def by_timestamp((timestamp, value)): # useful sort key function
  return timestamp


class CacheFeedingProcessor(Processor):
  plugin_name = 'write'

  def process(self, metric, datapoint):
    t = time.time()
    MetricCache.store(metric, datapoint)
    duration_micros = (time.time() - t) * ONE_MILLION
    instrumentation.append('pipeline.cache_microseconds', duration_micros)
    return Processor.NO_OUTPUT


class MetricCache(dict):
  def __init__(self):
    self.size = 0
    self.lock = Lock()
    self.MAX_CACHE_SIZE = settings.MAX_CACHE_SIZE
    self.CACHE_SIZE_LOW_WATERMARK = settings.CACHE_SIZE_LOW_WATERMARK

  def __setitem__(self, key, value):
    raise TypeError("Use store() method instead!")

  def store(self, metric, datapoint):
    self.lock.acquire()
    try:
      self.setdefault(metric, {})
      if datapoint[0] in self[metric]:
        self.size += 1 # Not a duplicate, increment
      self[metric][datapoint[0]] = datapoint
    finally:
      self.lock.release()

    if self.size >= self.MAX_CACHE_SIZE:
      log.msg("MetricCache is full: self.size=%d" % self.size)
      state.events.cacheFull()

  def getDatapoints(self, metric):
    return sorted(self.get(metric, {}).values(), key=by_timestamp)

  def pop(self, metric):
    self.lock.acquire()
    try:
      datapoint_index = dict.pop(self, metric)
      self.size -= len(datapoint_index)
    finally:
      self.lock.release()
    return sorted(datapoint_index.values(), key=by_timestamp)

  def drain(self):
    "Removes and generates metrics in order of most cached values to least"
    if not self:
      return

    t = time.time()
    self.lock.acquire()
    try:
      metric_queue_sizes = [ (metric, len(datapoints)) for metric,datapoints in self.items() ]
    finally:
      self.lock.release()

    micros = int((time.time() - t) * 1000000)
    log.debug("Generated %d cache queues in %d microseconds" %
            (len(metric_queue_sizes), micros))

    t = time.time()
    metric_queue_sizes.sort(key=lambda item: item[1], reverse=True)
    micros = int((time.time() - t) * 1000000)
    log.debug("Sorted %d cache queues in %d microseconds" %
            (len(metric_queue_sizes), micros))

    for metric, queue_size in metric_queue_sizes:
      yield (metric, self.pop(metric))

      if state.cacheTooFull and self.size < self.CACHE_SIZE_LOW_WATERMARK:
        log.msg("cache size below watermark")
        state.events.cacheSpaceAvailable()


# Ghetto singleton
MetricCache = MetricCache()

# Avoid import circularities
from carbon import log, state, instrumentation

instrumentation.configure_metric_function('writer.cached_metrics', lambda: len(MetricCache))
instrumentation.configure_metric_function('writer.cached_datapoints', lambda: MetricCache.size)
