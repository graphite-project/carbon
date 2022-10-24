from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.instrumentation import increment
from carbon.pipeline import Processor
from carbon.conf import settings
from carbon import log


class AggregationProcessor(Processor):
  plugin_name = 'aggregate'

  def process(self, metric, datapoint):
    increment('datapointsReceived')

    aggregate_metrics = set()

    for rule in RuleManager.rules:
      aggregate_metric = rule.get_aggregate_metric(metric)

      if aggregate_metric is None:
        continue
      else:
        aggregate_metrics.add(aggregate_metric)

      values_buffer = BufferManager.get_buffer(aggregate_metric)

      if not values_buffer.configured:
        values_buffer.configure_aggregation(rule.frequency, rule.aggregation_func)

      values_buffer.input(datapoint)

    if settings.FORWARD_ALL and metric not in aggregate_metrics:
      if settings.LOG_AGGREGATOR_MISSES and len(aggregate_metrics) == 0:
        log.msg(
          "Couldn't match metric %s with any aggregation rule. Passing on un-aggregated." % metric)
      yield (metric, datapoint)
