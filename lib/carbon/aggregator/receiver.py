from carbon.instrumentation import increment
from carbon.aggregator.rules import RuleManager
from carbon.aggregator.buffers import BufferManager
from carbon.rewrite import RewriteRuleManager
from carbon import events, log

# Specity whether to use aggregation-rules.conf or manual replacement rule.
USE_RULE_MANAGER = False

def process(metric, datapoint):
  increment('datapointsReceived')

  for rule in RewriteRuleManager.preRules:
    metric = rule.apply(metric)

  aggregate_metrics = []

  if USE_RULE_MANAGER == True:
    for rule in RuleManager.rules:
      aggregate_metric = rule.get_aggregate_metric(metric)

      if aggregate_metric is None:
        continue
      else:
        aggregate_metrics.append(aggregate_metric)

      buffer = BufferManager.get_buffer(aggregate_metric)

      if not buffer.configured:
        buffer.configure_aggregation(rule.frequency, rule.aggregation_func)

      buffer.input(datapoint)
  # Custom internal rule
  else:
    sum_index = metric.find(".sum")
    if sum_index != -1:
      aggregate_metric = metric[:sum_index] + ".sum_all.hosts"
      aggregate_metrics.append(aggregate_metric)

      buffer = BufferManager.get_buffer(aggregate_metric)

      if not buffer.configured:
        buffer.configure_aggregation(60, sum)

      buffer.input(datapoint)

  for rule in RewriteRuleManager.postRules:
    metric = rule.apply(metric)

  if metric not in aggregate_metrics:
    log.msg("Couldn't match metric %s with any aggregation rule. Passing on un-aggregated." % metric)
    events.metricGenerated(metric, datapoint)
