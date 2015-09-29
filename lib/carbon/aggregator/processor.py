from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.instrumentation import increment
from carbon.pipeline import Processor
from carbon.rewrite import PRE, POST, RewriteRuleManager
from carbon.conf import settings
from carbon import log


class AggregationProcessor(Processor):
  plugin_name = 'aggregate'

  def process(self, metric, datapoint):
    increment('datapointsReceived')

    for rule in RewriteRuleManager.rules(PRE):
      metric = rule.apply(metric)

    aggregate_metrics = set()

    if settings.AGGREGATOR_RULE_METHOD == "rules":
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
    elfif settings.AGGREGATOR_RULE_METHOD == "sumall":
      sum_index = metric.find(".sum.")
      if sum_index != -1:
        aggregate_metric = metric[:sum_index] + ".sum_all.hosts"
        aggregate_metrics.append(aggregate_metric)

        buffer = BufferManager.get_buffer(aggregate_metric)

        if not buffer.configured:
          buffer.configure_aggregatio(60, sum)

        buffer.input(datapoint)

    for rule in RewriteRuleManager.rules(POST):
      metric = rule.apply(metric)

    if metric not in aggregate_metrics:
      if settings.LOG_AGGREGATOR_MISSES and len(aggregate_metrics) == 0:
        log.msg("Couldn't match metric %s with any aggregation rule. Passing on un-aggregated." % metric)
      yield (metric, datapoint)
