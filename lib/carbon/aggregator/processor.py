from carbon import instrumentation
from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.pipeline import Processor


instrumentation.configure_counters([
  'aggregation.datapoints_generated',
  'aggregation.datapoints_analyzed',
])


class AggregationProcessor(Processor):
  plugin_name = 'aggregate'

  def process(self, metric, datapoint):
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
