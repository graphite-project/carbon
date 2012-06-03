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

  def pipeline_ready(self):
    if settings.ENABLE_AGGREGATION_FILTERING:
      filters = settings.read_filters('aggregation-filters.conf')
    else:
      filters = []

  def process(self, metric, datapoint):
    for filter in self.filters:
      if filter.action == 'allow':
        if filter.matches(metric):
          break
      elif filter.action == 'exclude':
        if filter.matches(metric):
          instrumentation.increment('aggregation.datapoints_filtered')
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
      yield (metric, datapoint)
