from mock import Mock, mock_open, patch
from unittest import TestCase
from carbon.pipeline import Processor
from carbon.rewrite import PRE, RewriteProcessor, RewriteRule, RewriteRuleManager


class RewriteProcessorTest(TestCase):
  def tearDown(self):
    RewriteRuleManager.clear()

  def test_registers_plugin(self):
    self.assertTrue('rewrite' in Processor.plugins)

  def test_applies_rule(self):
    mock_rule = Mock(spec=RewriteRule)
    RewriteRuleManager.rulesets[PRE] = [mock_rule]
    list(RewriteProcessor(PRE).process('carbon.foo', (0, 0)))
    mock_rule.apply.assert_called_once_with('carbon.foo')

  def test_applies_rule_and_returns_metric(self):
    mock_rule = Mock(spec=RewriteRule)
    mock_rule.apply.return_value = 'carbon.foo.bar'
    RewriteRuleManager.rulesets[PRE] = [mock_rule]
    result = list(RewriteProcessor(PRE).process('carbon.foo', (0, 0)))
    self.assertEqual(('carbon.foo.bar', (0, 0)), result[0])

  def test_passes_through_with_no_rules(self):
    result = list(RewriteProcessor(PRE).process('carbon.foo', (0, 0)))
    self.assertEqual(('carbon.foo', (0, 0)), result[0])


class TestRewriteRule(TestCase):
  def setUp(self):
    self.sample_rule = RewriteRule('^carbon[.]foo[.]', 'carbon_foo.')

  def test_instantiation_compiles_pattern(self):
    self.assertTrue(hasattr(self.sample_rule.regex, 'sub'))

  def test_apply_substitutes(self):
    result = self.sample_rule.apply('carbon.foo.bar')
    self.assertEqual('carbon_foo.bar', result)


class TestRewriteRuleManager(TestCase):
  def setUp(self):
    self.sample_config = """
[pre]
^carbon.foo = carbon.foo.bar
^carbon.bar = carbon.bar.baz
"""
    self.sample_multi_config = """
[pre]
^carbon.foo = carbon.foo.bar
^carbon.bar = carbon.bar.baz

[post]
^carbon.baz = carbon.foo.bar
"""
    self.broken_pattern_config = """
[pre]
^carbon.foo = carbon.foo.bar
^carbon.(bar = carbon.bar.baz
"""
    self.commented_config = """
[pre]
^carbon.foo = carbon.foo.bar
#^carbon.bar = carbon.bar.baz
"""

  def tearDown(self):
    RewriteRuleManager.rules_file = None
    RewriteRuleManager.rules_last_read = 0.0
    RewriteRuleManager.clear()

  def test_looping_call_reads_rules(self):
    self.assertEqual(RewriteRuleManager.read_rules, RewriteRuleManager.read_task.f)

  def test_request_for_nonexistent_rules_returns_iterable(self):
    try:
      iter(RewriteRuleManager.rules('foo'))
    except TypeError:
      self.fail("RewriteRuleManager.rules() returned a non-iterable type")

  def test_read_from_starts_task(self):
    with patch.object(RewriteRuleManager, 'read_rules'):
      with patch.object(RewriteRuleManager.read_task, 'start') as task_start_mock:
        RewriteRuleManager.read_from('foo.conf')
        self.assertEqual(1, task_start_mock.call_count)

  def test_read_records_mtime(self):
    import carbon.rewrite
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', mock_open(), create=True):
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=1234)):
          RewriteRuleManager.read_rules()
    self.assertEqual(1234, RewriteRuleManager.rules_last_read)

  def test_read_clears_if_no_file(self):
    import carbon.rewrite
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'exists', Mock(return_value=False)):
      with patch.object(RewriteRuleManager, 'clear') as clear_mock:
        RewriteRuleManager.read_rules()
        clear_mock.assert_called_once_with()

  def test_rules_unchanged_if_mtime_unchanged(self):
    import carbon.rewrite
    mtime = 1234
    rulesets = {'pre': [Mock(RewriteRule)]}
    RewriteRuleManager.rules_last_read = mtime
    RewriteRuleManager.rulesets.update(rulesets)
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
      with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=mtime)):
        RewriteRuleManager.read_rules()

    self.assertEqual(rulesets, RewriteRuleManager.rulesets)

  def test_read_doesnt_open_file_if_mtime_unchanged(self):
    import carbon.rewrite
    mtime = 1234
    RewriteRuleManager.rules_last_read = mtime
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', mock_open(), create=True) as open_mock:
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=1234)):
          RewriteRuleManager.read_rules()
          self.assertFalse(open_mock.called)

  def test_read_opens_file_if_mtime_newer(self):
    import carbon.rewrite
    RewriteRuleManager.rules_last_read = 1234
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', mock_open(), create=True) as open_mock:
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=5678)):
          RewriteRuleManager.read_rules()
          self.assertTrue(open_mock.called)

  def test_section_parsed_into_ruleset(self):
    import carbon.rewrite

    open_mock = Mock(return_value=iter(self.sample_config.splitlines()))
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', open_mock, create=True):
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=1234)):
          RewriteRuleManager.read_rules()
    self.assertTrue('pre' in RewriteRuleManager.rulesets)

  def test_multiple_section_parsed_into_ruleset(self):
    import carbon.rewrite

    open_mock = Mock(return_value=iter(self.sample_multi_config.splitlines()))
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', open_mock, create=True):
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=1234)):
          RewriteRuleManager.read_rules()
    self.assertTrue('pre' in RewriteRuleManager.rulesets)
    self.assertTrue('post' in RewriteRuleManager.rulesets)

  def test_rules_parsed(self):
    import carbon.rewrite

    open_mock = Mock(return_value=iter(self.sample_config.splitlines()))
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', open_mock, create=True):
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=1234)):
          RewriteRuleManager.read_rules()
    self.assertEqual(2, len(RewriteRuleManager.rules('pre')))

  def test_broken_patterns_ignored(self):
    import carbon.rewrite

    open_mock = Mock(return_value=iter(self.broken_pattern_config.splitlines()))
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', open_mock, create=True):
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=1234)):
          RewriteRuleManager.read_rules()
    self.assertEqual(1, len(RewriteRuleManager.rules('pre')))

  def test_comments_ignored(self):
    import carbon.rewrite

    open_mock = Mock(return_value=iter(self.commented_config.splitlines()))
    RewriteRuleManager.rules_file = 'foo.conf'

    with patch.object(carbon.rewrite, 'open', open_mock, create=True):
      with patch.object(carbon.rewrite, 'exists', Mock(return_value=True)):
        with patch.object(carbon.rewrite, 'getmtime', Mock(return_value=1234)):
          RewriteRuleManager.read_rules()
    self.assertEqual(1, len(RewriteRuleManager.rules('pre')))
