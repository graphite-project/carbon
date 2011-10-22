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


class AggregationProcessor(Processor):
  plugin_name = 'aggregate'
  filter_function = staticmethod(lambda metric: True)

  def pipeline_ready(self):
    if settings.ENABLE_AGGREGATION_FILTERING:
      self.filters = settings.read_filters('aggregation-filters.conf', store=False)
    else:
      self.filters = []

  def process(self, metric, datapoint):
    for filter in self.filters:
      if not filter.allow(metric):
        instrumentation.increment('aggregation.datapoints_filtered')
        yield (metric, datapoint)
        return

    instrumentation.increment('aggregation.datapoints_analyzed')
    for rule in RuleManager.rules:
      aggregate_metric = rule.get_aggregate_metric(metric)

      if aggregate_metric is None:
        continue

      buffer = BufferManager.get_buffer(aggregate_metric)

      if not buffer.configured:
        buffer.configure_aggregation(rule.frequency, rule.aggregation_func)

      buffer.input(datapoint)

    yield (metric, datapoint)
