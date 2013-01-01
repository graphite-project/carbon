import time
from carbon import instrumentation
from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.pipeline import Processor
from carbon.conf import settings


instrumentation.configure_counters([
  'aggregation.datapoints_generated',
  'aggregation.datapoints_analyzed',
  'aggregation.datapoints_filtered',
])
instrumentation.configure_stats('pipeline.aggregation_microseconds', ('total', 'min', 'max', 'avg'))

ONE_MILLION = 1000000 # I hate counting zeroes

class AggregationProcessor(Processor):
  plugin_name = 'aggregate'

  def pipeline_ready(self):
    self.aggregation_filtering_enabled = False;
    if settings.ENABLE_AGGREGATION_FILTERING:
      filters = settings.read_filters('aggregation-filters.conf')
      self.aggregation_filtering_enabled = True;
    else:
      filters = []

  def process(self, metric, datapoint):
    t = time.time()
    if self.aggregation_filtering_enabled:
      for filter in self.filters:
        if filter.action == 'allow':
          if filter.matches(metric):
            break
        elif filter.action == 'exclude':
          if filter.matches(metric):
            instrumentation.increment('aggregation.datapoints_filtered')
            duration_micros = (time.time() - t) * ONE_MILLION
            instrumentation.append('pipeline.aggregation_microseconds', duration_micros)
            yield (metric, datapoint)
            return

    instrumentation.increment('aggregation.datapoints_analyzed')
    aggregate_metrics = set()

    for rule in RuleManager.rules:
      aggregate_metric = rule.get_aggregate_metric(metric)

      if aggregate_metric is None:
        continue
      else:
        aggregate_metrics.add(aggregate_metric)

      buffer = BufferManager.get_buffer(aggregate_metric)

      if not buffer.configured:
        buffer.configure_aggregation(rule.frequency, rule.aggregation_func)

      buffer.input(datapoint)

    if metric not in aggregate_metrics:
      duration_micros = (time.time() - t) * ONE_MILLION
      instrumentation.append('pipeline.aggregation_microseconds', duration_micros)
      yield (metric, datapoint)
