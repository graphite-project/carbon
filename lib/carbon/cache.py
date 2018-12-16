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
import threading
from operator import itemgetter
from random import choice
from collections import defaultdict

from carbon.conf import settings
from carbon import events, log
from carbon.pipeline import Processor
from carbon.util import TaggedSeries


def by_timestamp(t_v):  # useful sort key function
  (timestamp, _) = t_v
  return timestamp


class CacheFeedingProcessor(Processor):
  plugin_name = 'write'

  def __init__(self, *args, **kwargs):
    super(CacheFeedingProcessor, self).__init__(*args, **kwargs)
    self.cache = MetricCache()

  def process(self, metric, datapoint):
    # normalize metric name (reorder tags)
    try:
      metric = TaggedSeries.parse(metric).path
    except Exception as err:
      log.msg('Error parsing metric %s: %s' % (metric, err))

    self.cache.store(metric, datapoint)
    return Processor.NO_OUTPUT


class DrainStrategy(object):
  """Implements the strategy for writing metrics.
  The strategy chooses what order (if any) metrics
  will be popped from the backing cache"""
  def __init__(self, cache):
    self.cache = cache

  def choose_item(self):
    raise NotImplementedError()


class NaiveStrategy(DrainStrategy):
  """Pop points in an unordered fashion."""
  def __init__(self, cache):
    super(NaiveStrategy, self).__init__(cache)

    def _generate_queue():
      while True:
        metric_names = list(self.cache.keys())
        while metric_names:
          yield metric_names.pop()

    self.queue = _generate_queue()

  def choose_item(self):
    return next(self.queue)


class MaxStrategy(DrainStrategy):
  """Always pop the metric with the greatest number of points stored.
  This method leads to less variance in pointsPerUpdate but may mean
  that infrequently or irregularly updated metrics may not be written
  until shutdown """
  def choose_item(self):
    metric_name, _ = max(self.cache.items(), key=lambda x: len(itemgetter(1)(x)))
    return metric_name


class RandomStrategy(DrainStrategy):
  """Pop points randomly"""
  def choose_item(self):
    return choice(list(self.cache.keys()))  # nosec


class SortedStrategy(DrainStrategy):
  """ The default strategy which prefers metrics with a greater number
  of cached points but guarantees every point gets written exactly once during
  a loop of the cache """
  def __init__(self, cache):
    super(SortedStrategy, self).__init__(cache)

    def _generate_queue():
      while True:
        t = time.time()
        metric_counts = sorted(self.cache.counts, key=lambda x: x[1])
        size = len(metric_counts)
        if settings.LOG_CACHE_QUEUE_SORTS and size:
          log.msg("Sorted %d cache queues in %.6f seconds" % (size, time.time() - t))
        while metric_counts:
          yield itemgetter(0)(metric_counts.pop())
        if settings.LOG_CACHE_QUEUE_SORTS and size:
          log.msg("Queue consumed in %.6f seconds" % (time.time() - t))

    self.queue = _generate_queue()

  def choose_item(self):
    return next(self.queue)


class TimeSortedStrategy(DrainStrategy):
  """ This strategy prefers metrics wich are lagging behind
  guarantees every point gets written exactly once during
  a loop of the cache """
  def __init__(self, cache):
    super(TimeSortedStrategy, self).__init__(cache)

    def _generate_queue():
      while True:
        t = time.time()
        metric_lw = sorted(self.cache.watermarks, key=lambda x: x[1], reverse=True)
        if settings.MIN_TIMESTAMP_LAG:
          metric_lw = [x for x in metric_lw if t - x[1] > settings.MIN_TIMESTAMP_LAG]
        size = len(metric_lw)
        if settings.LOG_CACHE_QUEUE_SORTS and size:
          log.msg("Sorted %d cache queues in %.6f seconds" % (size, time.time() - t))
        if not metric_lw:
          # If there is nothing to do give a chance to sleep to the reader.
          yield None
        while metric_lw:
          yield itemgetter(0)(metric_lw.pop())
        if settings.LOG_CACHE_QUEUE_SORTS and size:
          log.msg("Queue consumed in %.6f seconds" % (time.time() - t))

    self.queue = _generate_queue()

  def choose_item(self):
    return next(self.queue)


class _MetricCache(defaultdict):
  """A Singleton dictionary of metric names and lists of their datapoints"""
  def __init__(self, strategy=None):
    self.lock = threading.Lock()
    self.size = 0
    self.strategy = None
    if strategy:
      self.strategy = strategy(self)
    super(_MetricCache, self).__init__(dict)

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints)
            in self.items()]

  @property
  def watermarks(self):
    return [(metric, min(datapoints.keys()), max(datapoints.keys()))
            for (metric, datapoints) in self.items()
            if datapoints]

  @property
  def is_full(self):
    if settings.MAX_CACHE_SIZE == float('inf'):
      return False
    else:
      return self.size >= settings.MAX_CACHE_SIZE

  def _check_available_space(self):
    if state.cacheTooFull and self.size < settings.CACHE_SIZE_LOW_WATERMARK:
      log.msg("MetricCache below watermark: self.size=%d" % self.size)
      events.cacheSpaceAvailable()

  def drain_metric(self):
    """Returns a metric and it's datapoints in order determined by the
    `DrainStrategy`_"""
    if not self:
      return (None, [])
    if self.strategy:
      with self.lock:
        metric = self.strategy.choose_item()
    else:
      # Avoid .keys() as it dumps the whole list
      metric = next(iter(self))
    if metric is None:
      return (None, [])
    return (metric, self.pop(metric))

  def get_datapoints(self, metric):
    """Return a list of currently cached datapoints sorted by timestamp"""
    return sorted(self.get(metric, {}).items(), key=by_timestamp)

  def pop(self, metric):
    with self.lock:
      datapoint_index = defaultdict.pop(self, metric)
      self.size -= len(datapoint_index)
    self._check_available_space()

    return sorted(datapoint_index.items(), key=by_timestamp)

  def store(self, metric, datapoint):
    timestamp, value = datapoint
    with self.lock:
      if timestamp not in self[metric]:
        # Not a duplicate, hence process if cache is not full
        if self.is_full:
          log.msg("MetricCache is full: self.size=%d" % self.size)
          events.cacheFull()
        else:
          self.size += 1
          self[metric][timestamp] = value
      else:
        # Updating a duplicate does not increase the cache size
        self[metric][timestamp] = value


_Cache = None


def MetricCache():
  global _Cache
  if _Cache is not None:
    return _Cache

  # Initialize a singleton cache instance
  # TODO: use plugins.
  write_strategy = None
  if settings.CACHE_WRITE_STRATEGY == 'naive':
    write_strategy = NaiveStrategy
  if settings.CACHE_WRITE_STRATEGY == 'max':
    write_strategy = MaxStrategy
  if settings.CACHE_WRITE_STRATEGY == 'sorted':
    write_strategy = SortedStrategy
  if settings.CACHE_WRITE_STRATEGY == 'timesorted':
    write_strategy = TimeSortedStrategy
  if settings.CACHE_WRITE_STRATEGY == 'random':
    write_strategy = RandomStrategy

  _Cache = _MetricCache(write_strategy)
  return _Cache


# Avoid import circularities
from carbon import state  # NOQA
