import errno
import os
import re
from os import makedirs
from os.path import dirname, join
from unittest import TestCase
from mock import Mock, mock_open, patch

from twisted.python.usage import Options
from carbon.conf import CarbonConfiguration, CarbonDaemonOptions, ConfigError, Filter


def mock_open_iter(mock=None, read_data=None):
  """Allow the mocked file handle to be iterated over. Unfortunately, this
  allows the data in read_data to be read multiple times (once with read(),
  once iteratively)."""
  open_mock = mock_open(mock, read_data)
  if read_data:
    lines_with_endings = [l + '\n' for l in read_data.split('\n')]
    open_mock.return_value.__iter__.return_value = lines_with_endings
    # context open
    open_mock.return_value.__enter__.return_value.__iter__.return_value = lines_with_endings
  return open_mock

class CarbonConfigurationTest(TestCase):
  sample_config = {
      "global": """
GLOBAL_KEY = global_foo
[foo_section]
FOO_KEY = foo_value
""",
      "section": """
[foo_section]
FOO_KEY = foo_value
""",
      "types": """
INT = 1
FLOAT = 1.0
BOOL = true
STRING = string
DEFAULT = false""",
      "comments": """
# A comment
#[commented_section]
#COMMENTED_SETTING = value
\t # Comment with prepending whitespace
""",
      "ordered_sections": """
      [section_one]
      [section_two]
      [section_three]
"""
  }

  def setUp(self):
    self.carbon_config = CarbonConfiguration()

  def test_missing_item(self):
    self.assertRaises(ConfigError, self.carbon_config.__getitem__, 'foo')

  def test_missing_attribute(self):
    self.assertRaises(ConfigError, self.carbon_config.__getattribute__, 'foo')

  def test_attribute_access(self):
    self.carbon_config['foo'] = 'bar'
    self.assertEqual('bar', self.carbon_config.foo)

  @patch('carbon.conf.glob')
  def test_get_config_files(self, glob_mock):
    glob_mock.return_value = ['/test-instance/daemon.conf',
                              '/test-instance/listeners.conf']
    self.carbon_config.use_config_directory('/test-instance')
    glob_mock.assert_called_with('/test-instance/*.conf')
    self.assertEquals(['daemon.conf', 'listeners.conf'], self.carbon_config.config_files)

  def test_file_exists_unconfigured(self):
    self.assertRaises(ConfigError, self.carbon_config.file_exists, 'daemon.conf')

  def test_file_exists(self):
    self.carbon_config.config_dir = '/test-instance'
    self.carbon_config.config_files = ['daemon.conf', 'listeners.conf']
    self.assertTrue(self.carbon_config.file_exists('daemon.conf'))
    self.assertTrue(self.carbon_config.file_exists('listeners.conf'))
    self.assertFalse(self.carbon_config.file_exists('bar.conf'))

  def test_get_path_unconfigured(self):
    self.assertRaises(ConfigError, self.carbon_config.get_path, 'daemon.conf')

  @patch('carbon.conf.exists', new=Mock(return_value=False))
  def test_get_path_nonexistent(self):
    self.carbon_config.config_dir = '/test-instance'
    self.assertRaises(ConfigError, self.carbon_config.get_path, 'foo.conf')

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_get_path_existing(self):
    self.carbon_config.config_dir = '/test-instance'
    self.assertEqual(join('/test-instance', 'daemon.conf'), self.carbon_config.get_path('daemon.conf'))

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_read_empty_file(self):
    from carbon.conf import defaults
    open_mock = mock_open_iter()
    self.carbon_config.use_config_directory('/test-instance')
    with patch('__builtin__.open', open_mock):
      result = self.carbon_config.read_file('foo')
    self.assertEqual({}, result)
    self.assertEqual(defaults, self.carbon_config)

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_ignore_comments(self):
    from carbon.conf import defaults
    open_mock = mock_open_iter(read_data=self.sample_config['comments'])
    self.carbon_config.use_config_directory('/test-instance')
    with patch('__builtin__.open', open_mock):
      result = self.carbon_config.read_file('foo')
    self.assertEqual({}, result)
    self.assertEqual(defaults, self.carbon_config)

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_read_globals(self):
    open_mock = mock_open_iter(read_data=self.sample_config['global'])
    self.carbon_config.use_config_directory('/test-instance')
    with patch('__builtin__.open', open_mock):
      result = self.carbon_config.read_file('foo')
      self.assertEqual('global_foo', result['GLOBAL_KEY'])
      self.assertEqual('global_foo', self.carbon_config['GLOBAL_KEY'])
      self.assertFalse(result.has_key('FOO_KEY'))
      self.assertFalse(self.carbon_config.has_key('FOO_KEY'))

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_read_section(self):
    open_mock = mock_open_iter(read_data=self.sample_config['section'])
    self.carbon_config.use_config_directory('/test-instance')
    with patch('__builtin__.open', open_mock):
      result = self.carbon_config.read_file('foo')
      self.assertEqual({'FOO_KEY': 'foo_value'}, result['foo_section'])
      self.assertEqual({'FOO_KEY': 'foo_value'}, self.carbon_config['foo_section'])

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_type_coercion(self):
    open_mock = mock_open_iter(read_data=self.sample_config['types'])
    self.carbon_config.use_config_directory('/test-instance')
    with patch.dict('carbon.conf.defaults', {
        'INT': int(2),
        'FLOAT': float(2.0),
        'BOOL': bool(True),
        'STRING': str('string')
    }):
      with patch('__builtin__.open', open_mock):
        result = self.carbon_config.read_file('foo')
        self.assertEqual(int, type(result['INT']))
        self.assertEqual(float, type(result['FLOAT']))
        self.assertEqual(bool, type(result['BOOL']))
        self.assertEqual(str, type(result['STRING']))
        # Default is string
        self.assertEqual(str, type(result['DEFAULT']))

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_no_store(self):
    from carbon.conf import defaults
    open_mock = mock_open_iter(read_data=self.sample_config['section'])
    self.carbon_config.use_config_directory('/test-instance')
    with patch('__builtin__.open', open_mock):
      result = self.carbon_config.read_file('foo', store=False)
      self.assertEqual({'FOO_KEY': 'foo_value'}, result['foo_section'])
      self.assertEqual(defaults, self.carbon_config)

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_ordered_items(self):
    open_mock = mock_open_iter(read_data=self.sample_config['ordered_sections'])
    self.carbon_config.use_config_directory('/test-instance')
    with patch('__builtin__.open', open_mock):
      result = self.carbon_config.read_file('foo', ordered_items=True)
      self.assertEqual(list, type(result))
      self.assertEqual('section_one', result[0][0])
      self.assertEqual('section_two', result[1][0])
      self.assertEqual('section_three', result[2][0])


class FilterTest(TestCase):
  def test_action_init(self):
    self.assertEqual('include', Filter('include', '').action)
    self.assertEqual('exclude', Filter('exclude', '').action)
    self.assertRaises(ValueError, lambda: Filter('foo', ''))

  def test_regex_init(self):
    self.assertEqual(re.compile('.*'), Filter('include', '.*').regex)
    self.assertRaises(re.error, lambda: Filter('include', '('))

  def test_matches(self):
    self.assertTrue(Filter('include', '.*').matches('anything'))
    self.assertTrue(Filter('exclude', '.*').matches('anything'))
    self.assertFalse(Filter('include', '[123]').matches('abc'))

  def test_include_allow_on_match(self):
    with patch.object(Filter, 'matches', return_value=True):
      self.assertTrue(Filter('include', '').allow(''))

  def test_include_disallow_no_match(self):
    with patch.object(Filter, 'matches', return_value=False):
      self.assertFalse(Filter('include', '').allow(''))

  def test_exclude_allow_no_match(self):
    with patch.object(Filter, 'matches', return_value=False):
      self.assertTrue(Filter('exclude', '').allow(''))

  def test_exclude_disallow_on_match(self):
    with patch.object(Filter, 'matches', return_value=True):
      self.assertFalse(Filter('exclude', '').allow(''))


class CarbonOptionsTest(TestCase):
  def setUp(self):
    self.daemon_options = CarbonDaemonOptions()

  def test_long_options(self):
    self.assertTrue('debug' in self.daemon_options.longOpt)
    self.assertTrue('nodaemon' in self.daemon_options.longOpt)
    self.assertTrue('config=' in self.daemon_options.longOpt)
    self.assertTrue('logdir=' in self.daemon_options.longOpt)
    self.assertTrue('umask=' in self.daemon_options.longOpt)

  def test_short_options(self):
    self.assertTrue('c' in self.daemon_options.shortOpt)


class HandleActionTest(TestCase):
  def setUp(self):
    self.daemon_options = CarbonDaemonOptions()
    self.daemon_options['instance'] = 'a'
    self.daemon_options.parent = Options()
    self.daemon_options.parent['pidfile'] = 'pidfile_path/carbon.pid'

  def test_invalid_action(self):
    self.daemon_options['action'] = 'foo'
    try:
      self.daemon_options.handleAction()
    except SystemExit, e:
      self.assertEqual(1, e.code)
    except Exception, e:
      self.fail("Unexpected exception type: %s" % e)

  @patch('carbon.conf.exists', new=Mock(return_value=False))
  def test_stop_no_pidfile(self):
    self.daemon_options['action'] = 'stop'
    try:
      self.daemon_options.handleAction()
    except SystemExit, e:
      self.assertEqual(0, e.code)
    except Exception, e:
      self.fail("Unexpected exception type: %s" % e)

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_stop_error_reading_pidfile(self):
    open_mock = mock_open()
    open_mock.return_value.read.side_effect = OSError
    open_mock.return_value.__enter__.read.side_effect = OSError
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'stop'
      try:
        self.daemon_options.handleAction()
      except SystemExit, e:
        self.assertEqual(1, e.code)
      except Exception, e:
        self.fail("Unexpected exception type: %s" % e)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  @patch('os.kill')
  def test_stop_pid_not_exist(self, kill_mock):
    kill_mock.side_effect = OSError(errno.ESRCH, None)
    open_mock = mock_open(read_data='123456')
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'stop'
      try:
        self.daemon_options.handleAction()
      except SystemExit, e:
        self.assertEqual(0, e.code)
      except Exception, e:
        self.fail("Unexpected exception type: %s" % e)
      kill_mock.assert_called_once_with(123456, 15)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  @patch('os.kill')
  def test_stop_pid_no_permission(self, kill_mock):
    kill_mock.side_effect = OSError(errno.EPERM)
    open_mock = mock_open(read_data='123456')
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'stop'
      self.assertRaises(OSError, self.daemon_options.handleAction)
      kill_mock.assert_called_once_with(123456, 15)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  @patch('os.kill')
  def test_stop_pid_exists(self, kill_mock):
    open_mock = mock_open(read_data='123456')
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'stop'
      try:
        self.daemon_options.handleAction()
      except SystemExit, e:
        self.assertEqual(0, e.code)
      except Exception, e:
        self.fail("Unexpected exception type: %s" % e)
      kill_mock.assert_called_once_with(123456, 15)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=False))
  def test_status_no_pidfile(self):
    self.daemon_options['action'] = 'status'
    try:
      self.daemon_options.handleAction()
    except SystemExit, e:
      self.assertEqual(1, e.code)
    except Exception, e:
      self.fail("Unexpected exception type: %s" % e)

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  def test_status_error_reading_pidfile(self):
    open_mock = mock_open()
    open_mock.return_value.read.side_effect = OSError
    open_mock.return_value.__enter__.read.side_effect = OSError
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'status'
      try:
        self.daemon_options.handleAction()
      except SystemExit, e:
        self.assertEqual(1, e.code)
      except Exception, e:
        self.fail("Unexpected exception type: %s" % e)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  @patch('carbon.conf._process_alive')
  def test_status_not_running(self, process_alive_mock):
    process_alive_mock.return_value = False
    open_mock = mock_open(read_data='123456')
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'status'
      try:
        self.daemon_options.handleAction()
      except SystemExit, e:
        self.assertEqual(1, e.code)
      except Exception, e:
        self.fail("Unexpected exception type: %s" % e)
      process_alive_mock.assert_called_once_with(123456)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  @patch('carbon.conf._process_alive')
  def test_status_running(self, process_alive_mock):
    process_alive_mock.return_value = True
    open_mock = mock_open(read_data='123456')
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'status'
      try:
        self.daemon_options.handleAction()
      except SystemExit, e:
        self.assertEqual(0, e.code)
      except Exception, e:
        self.fail("Unexpected exception type: %s" % e)
      process_alive_mock.assert_called_once_with(123456)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=False))
  def test_start_no_pidfile(self):
    with patch('__builtin__.open', mock_open()) as open_mock:
      self.daemon_options['action'] = 'start'
      self.assertEqual(None, self.daemon_options.handleAction())
      self.assertFalse(open_mock.called)

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  @patch('os.unlink')
  @patch('carbon.conf._process_alive')
  def test_start_with_active_pidfile(self, process_alive_mock, unlink_mock):
    process_alive_mock.return_value = True
    open_mock = mock_open(read_data='123456')
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'start'
      try:
        self.daemon_options.handleAction()
      except SystemExit, e:
        self.assertEqual(1, e.code)
      except Exception, e:
        self.fail("Unexpected exception type: %s" % e)
      process_alive_mock.assert_called_once_with(123456)
      self.assertFalse(unlink_mock.called)
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')

  @patch('carbon.conf.exists', new=Mock(return_value=True))
  @patch('os.unlink')
  @patch('carbon.conf._process_alive')
  def test_start_with_inactive_pidfile(self, process_alive_mock, unlink_mock):
    process_alive_mock.return_value = False
    open_mock = mock_open(read_data='123456')
    with patch('__builtin__.open', open_mock):
      self.daemon_options['action'] = 'start'
      self.daemon_options.handleAction()
      process_alive_mock.assert_called_once_with(123456)
      unlink_mock.assert_called_once_with('pidfile_path/carbon.pid')
      open_mock.assert_called_once_with('pidfile_path/carbon.pid', 'r')
