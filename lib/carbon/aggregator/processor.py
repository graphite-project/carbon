from carbon import instrumentation
from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.pipeline import Processor
from carbon.conf import settings
from carbon.util import load_module


instrumentation.configure_counters([
  'aggregation.datapoints_generated',
  'aggregation.datapoints_analyzed',
  'aggregation.datapoints_filtered',
])


class AggregationProcessor(Processor):
  plugin_name = 'aggregate'
  filter_function = staticmethod(lambda metric: True)

  def pipeline_ready(self):
    if settings.AGGREGATION_FILTER_MODULE:
      self.filter_function = load_module(settings.AGGREGATION_FILTER_MODULE, member='allow')

  def process(self, metric, datapoint):
    if not self.filter_function(metric):
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
