import time
from twisted.internet.task import LoopingCall
from carbon.conf import settings
from carbon import log


class BufferManager:
  def __init__(self):
    self.buffers = {}

  def __len__(self):
    return len(self.buffers)

  def get_buffer(self, metric_path):
    if metric_path not in self.buffers:
      log.aggregator("Allocating new metric buffer for %s" % metric_path)
      self.buffers[metric_path] = MetricBuffer(metric_path)

    return self.buffers[metric_path]

  def clear(self):
    for buffer in self.buffers.values():
      buffer.close()

    self.buffers.clear()


class MetricBuffer:
  __slots__ = ('metric_path', 'interval_buffers', 'compute_task', 'configured',
               'aggregation_frequency', 'aggregation_func', 'most_recent_interval')

  def __init__(self, metric_path):
    self.metric_path = metric_path
    self.interval_buffers = {}
    self.compute_task = None
    self.configured = False
    self.aggregation_frequency = None
    self.aggregation_func = None
    self.most_recent_interval = None

  def input(self, datapoint):
    (timestamp, value) = datapoint
    interval = timestamp - (timestamp % self.aggregation_frequency)
    if interval > self.most_recent_interval:
      self.most_recent_interval = interval
      buffer = self.interval_buffers[interval] = IntervalBuffer(interval)
      buffer.input(datapoint)
    else:
      if interval in self.interval_buffers:
        buffer = self.interval_buffers[interval]
        buffer.input(datapoint)
      else:
        ## rather drop the datapoint than overwrite the entry in the storage?
        log.aggregator("WARNING: dropped datapoint on %(path)s, out of sequence for %(interval)d seconds. Consider increasing MAX_AGGREGATION_INTERVALS=%(mai)d  " \
           % ('path',self.metric_path, 'interval', self.most_recent_interval - interval, 'mai', settings['MAX_AGGREGATION_INTERVALS']) )


  def configure_aggregation(self, frequency, func):
    self.aggregation_frequency = int(frequency)
    self.aggregation_func = func
    self.compute_task = LoopingCall(self.compute_value)
    compute_frequency = min(settings['WRITE_BACK_FREQUENCY'], frequency) or frequency
    self.compute_task.start(compute_frequency, now=False)
    self.configured = True

  def compute_value(self):
    
    interval_threshold = self.most_recent_interval - (settings['MAX_AGGREGATION_INTERVALS'] * self.aggregation_frequency) 
    age_threshold = 0 
    if ( settings['MAX_AGGREGATION_AGE'] > 0 ):
      age_threshold = time.time() - self.aggregation_frequency * max( settings['MAX_AGGREGATION_AGE'], settings['MAX_AGGREGATION_INTERVALS'] )
 
    for buffer in self.interval_buffers.values():
      if buffer.active:
        value = self.aggregation_func(buffer.values)
        datapoint = (buffer.interval, value)
        state.events.metricGenerated(self.metric_path, datapoint)
        state.instrumentation.increment('aggregateDatapointsSent')
        buffer.mark_inactive()

      if buffer.interval < interval_threshold or buffer.updated < age_threshold :
        del self.interval_buffers[buffer.interval]
        if not self.interval_buffers:
          self.close()
          self.configured = False
          del BufferManager.buffers[self.metric_path]

  def close(self):
    if self.compute_task and self.compute_task.running:
      self.compute_task.stop()

  @property
  def size(self):
    return sum([len(buf.values) for buf in self.interval_buffers.values()])


class IntervalBuffer:
  __slots__ = ('interval', 'values', 'active','updated')

  def __init__(self, interval):
    self.interval = interval
    self.values = []
    self.active = True
    self.updated = time.time()

  def input(self, datapoint):
    self.values.append( datapoint[1] )
    self.active = True
    self.updated = time.time()

  def mark_inactive(self):
    self.active = False


# Shared importable singleton
BufferManager = BufferManager()

# Avoid import circularity
from carbon import state
