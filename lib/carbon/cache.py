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
from collections import deque
from carbon.conf import settings
try:
    from collections import defaultdict
except:
    from util import defaultdict


class _MetricCache(defaultdict):
  def __init__(self, defaultfactory=deque, method="sorted"):
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
        log.debug("Sorted %d cache queues in %.6f seconds" % (len(queue), time.time() - t))
      while queue:
        yield queue.pop()[0]

  @property
  def size(self):
    return reduce(lambda x, y: x + len(y), self.values(), 0)

  def store(self, metric, datapoint):
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
    elif not metric and self.method == "naive":
      return self.popitem()
    elif not metric and self.method == "sorted":
      metric = self.queue.next()
    datapoints = (metric, super(_MetricCache, self).pop(metric))
    return datapoints

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]


# Ghetto singleton

MetricCache = _MetricCache(method=settings.CACHE_WRITE_STRATEGY)


# Avoid import circularities
from carbon import log, state
