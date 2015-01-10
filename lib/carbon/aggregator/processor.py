from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.instrumentation import increment
from carbon.pipeline import Processor
from carbon.rewrite import PRE, POST, RewriteRuleManager


class AggregationProcessor(Processor):
  plugin_name = 'aggregate'

  def process(self, metric, datapoint):
    increment('datapointsReceived')

    for rule in RewriteRuleManager.rules(PRE):
      metric = rule.apply(metric)

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

    for rule in RewriteRuleManager.rules(POST):
      metric = rule.apply(metric)

    if metric not in aggregate_metrics:
      yield (metric, datapoint)
