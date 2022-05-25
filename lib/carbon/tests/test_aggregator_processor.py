from mock import patch
from unittest import TestCase

from carbon import instrumentation
from carbon.pipeline import Processor
from carbon.aggregator.buffers import BufferManager
from carbon.aggregator.rules import AggregationRule, RuleManager
from carbon.aggregator.processor import AggregationProcessor


class AggregationProcessorTest(TestCase):
  def setUp(self):
    self.sample_aggregation_rule = AggregationRule(r'^carbon.foo', r'carbon.foo.sum', 'sum', 1)
    self.sample_overwriting_aggregation_rule = \
        AggregationRule(r'^carbon.foo', r'carbon.foo', 'sum', 1)
    self.processor = AggregationProcessor()

  def tearDown(self):
    instrumentation.stats.clear()
    BufferManager.clear()
    RuleManager.clear()

  def test_registers_plugin(self):
    self.assertTrue('aggregate' in Processor.plugins)

  def test_process_increments_datapoints_metric(self):
    list(self.processor.process('carbon.foo', (0, 0)))
    self.assertEqual(1, instrumentation.stats['datapointsReceived'])

  def test_unaggregated_metrics_pass_through_when_no_rules(self):
    result = list(self.processor.process('carbon.foo', (0, 0)))
    self.assertEqual([('carbon.foo', (0, 0))], result)

  def test_unaggregated_metrics_pass_through(self):
    RuleManager.rules = [self.sample_aggregation_rule]
    result = list(self.processor.process('carbon.foo', (0, 0)))
    self.assertEqual([('carbon.foo', (0, 0))], result)

  def test_aggregation_rule_checked(self):
    RuleManager.rules = [self.sample_aggregation_rule]
    with patch.object(self.sample_aggregation_rule, 'get_aggregate_metric'):
      list(self.processor.process('carbon.foo', (0, 0)))
      self.sample_aggregation_rule.get_aggregate_metric.assert_called_once_with('carbon.foo')

  def test_new_buffer_configured(self):
    RuleManager.rules = [self.sample_aggregation_rule]
    list(self.processor.process('carbon.foo', (0, 0)))
    values_buffer = BufferManager.get_buffer('carbon.foo.sum')

    self.assertTrue(values_buffer.configured)
    self.assertEqual(1, values_buffer.aggregation_frequency)
    self.assertEqual(sum, values_buffer.aggregation_func)

  def test_buffer_receives_value(self):
    RuleManager.rules = [self.sample_aggregation_rule]
    list(self.processor.process('carbon.foo', (0, 0)))
    values_buffer = BufferManager.get_buffer('carbon.foo.sum')

    self.assertEqual([0], values_buffer.interval_buffers[0].values)

  def test_metric_not_passed_through_when_aggregate_overwrites(self):
    RuleManager.rules = [self.sample_overwriting_aggregation_rule]
    result = list(self.processor.process('carbon.foo', (0, 0)))

    self.assertEqual([], result)
