from mock import call, Mock, patch
from unittest import TestCase

from carbon import instrumentation
from carbon.rewrite import PRE, POST, RewriteRule, RewriteRuleManager
from carbon.pipeline import Processor
from carbon.aggregator.buffers import BufferManager
from carbon.aggregator.rules import AggregationRule, RuleManager
from carbon.aggregator.processor import AggregationProcessor


class AggregationProcessorTest(TestCase):
  def setUp(self):
    self.sample_rewrite_rule = RewriteRule(r'^(carbon.foo)', r'\1.bar')
    self.sample_aggregation_rule = AggregationRule(r'^carbon.foo', r'carbon.foo.sum', 'sum', 1)
    self.sample_overwriting_aggregation_rule = \
        AggregationRule(r'^carbon.foo', r'carbon.foo', 'sum', 1)
    self.processor = AggregationProcessor()

  def tearDown(self):
    instrumentation.stats.clear()
    BufferManager.clear()
    RuleManager.clear()
    RewriteRuleManager.clear()

  def test_registers_plugin(self):
    self.assertTrue('aggregate' in Processor.plugins)

  def test_process_increments_datapoints_metric(self):
    list(self.processor.process('carbon.foo', (0, 0)))
    self.assertEqual(1, instrumentation.stats['datapointsReceived'])

  def test_pre_rules_applied(self):
    RewriteRuleManager.rulesets[PRE] = [self.sample_rewrite_rule]
    result = list(self.processor.process('carbon.foo', (0, 0)))
    self.assertEqual('carbon.foo.bar', result[0][0])

  def test_post_rules_applied(self):
    RewriteRuleManager.rulesets[POST] = [self.sample_rewrite_rule]
    result = list(self.processor.process('carbon.foo', (0, 0)))
    self.assertEqual('carbon.foo.bar', result[0][0])

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

  def test_pre_rewrite_then_aggregation_rule(self):
    RewriteRuleManager.rulesets[PRE] = [self.sample_rewrite_rule]
    RuleManager.rules = [self.sample_aggregation_rule]
    apply_mock = Mock(side_effect=['carbon.foo.rewrite', 'carbon.foo.rewrite.aggregate'])

    with patch.object(self.sample_rewrite_rule, 'apply', apply_mock):
      with patch.object(self.sample_aggregation_rule, 'get_aggregate_metric', apply_mock):
        list(self.processor.process('carbon.foo', (0, 0)))

    # Pre rewrite gets metric as passed in and transforms before aggregation rule called
    self.assertEqual([call('carbon.foo'), call('carbon.foo.rewrite')], apply_mock.call_args_list)

  def test_aggregation_rule_then_post_rewrite(self):
    RuleManager.rules = [self.sample_aggregation_rule]
    RewriteRuleManager.rulesets[POST] = [self.sample_rewrite_rule]
    apply_mock = Mock(side_effect=['carbon.foo.aggregate', 'carbon.foo.aggregate.rewrite'])

    with patch.object(self.sample_rewrite_rule, 'apply', apply_mock):
      with patch.object(self.sample_aggregation_rule, 'get_aggregate_metric', apply_mock):
        list(self.processor.process('carbon.foo', (0, 0)))
    # Aggregation rule gets metric as passed in and so does post rewrite (due to no pre)
    self.assertEqual([call('carbon.foo'), call('carbon.foo')], apply_mock.call_args_list)

  def test_pre_rewrite_then_post_rewrite(self):
    RewriteRuleManager.rulesets[PRE] = [self.sample_rewrite_rule]
    RewriteRuleManager.rulesets[POST] = [self.sample_rewrite_rule]
    apply_mock = Mock(side_effect=['carbon.foo.rewrite', 'carbon.foo.rewrite.rewrite'])

    with patch.object(self.sample_rewrite_rule, 'apply', apply_mock):
      list(self.processor.process('carbon.foo', (0, 0)))

    # Pre rewrite gets metric as passed in and transforms before post rewrite called
    self.assertEqual([call('carbon.foo'), call('carbon.foo.rewrite')], apply_mock.call_args_list)

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
