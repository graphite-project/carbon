from carbon.protocols import MetricReceiver
from unittest import TestCase
from mock import Mock, patch
from carbon.cache import _MetricCache

import os.path
import pickle


class TestMetricReceiversHandler(TestCase):
  def test_build(self):
    expected_plugins = sorted(['line', 'udp', 'pickle', 'amqp', 'protobuf'])

    # Can't always test manhole because 'cryptography' can
    # be a pain to install and we don't want to make the CI
    # flaky because of that.
    try:
      import carbon.manhole
      expected_plugins.append('manhole')
    except ImportError:
      pass

    expected_plugins = sorted(expected_plugins)
    plugins = sorted(MetricReceiver.plugins.keys())
    self.assertEquals(expected_plugins, plugins)

    class _FakeService(object):
      def addService(_, __):
        pass
    fake_service = _FakeService()

    for plugin_name, plugin_class in MetricReceiver.plugins.items():
      plugin_class.build(fake_service)


class TestCacheManagementHandler(TestCase):
  def setUp(self):
    test_directory = os.path.dirname(os.path.realpath(__file__))
    settings = {
      'CONF_DIR': os.path.join(test_directory, 'data', 'conf-directory'),
      'USE_INSECURE_PICKLER': False
    }
    self._settings_patch = patch.dict('carbon.conf.settings', settings)
    self._settings_patch.start()

    from carbon.protocols import CacheManagementHandler
    self.handler = CacheManagementHandler()

    self.cache = _MetricCache()
    def _get_cache():
      return self.cache

    self._metriccache_patch = patch('carbon.protocols.MetricCache', _get_cache)
    self._metriccache_patch.start()

    self.handler.unpickler = pickle
    self.handler.peerAddr = 'localhost:7002'

    self.send_string_mock = Mock(side_effect=self._save_response)
    self._send_string_patch = patch.object(self.handler, 'sendString', self.send_string_mock)
    self._send_string_patch.start()

  def tearDown(self):
    self._settings_patch.stop()
    self._send_string_patch.stop()
    self._metriccache_patch.stop()
    del self.cache

  def _save_response(self, arg):
    self.response = None
    if arg:
      raw_response = arg
      self.response = pickle.loads(raw_response)

  def send_request(self, request_type, **kwargs):
    request = {}
    request['type'] = request_type
    request.update(kwargs)
    self.handler.stringReceived(pickle.dumps(request))

  @patch('carbon.protocols.get_unpickler')
  def test_pickler_configured_on_connect(self, get_unpickler_mock):
    from twisted.internet.address import IPv4Address
    address = IPv4Address('TCP', 'localhost', 7002)
    self.handler.transport = Mock()
    self.handler.transport.getPeer = Mock(return_value=address)
    self.handler.connectionMade()
    get_unpickler_mock.assert_called_once_with(insecure=False)

  def test_invalid_request_type_returns_error(self):
    response = self.send_request('foo')

    self.assertIn('error', self.response)

  def test_cache_query_returns_response_dict(self):
    self.send_request('cache-query', metric='carbon.foo')
    self.assertIsInstance(self.response, dict)

  def test_cache_query_response_has_datapoints(self):
    self.send_request('cache-query', metric='carbon.foo')
    self.assertIn('datapoints', self.response)

  def test_cache_query_returns_empty_if_no_match(self):
    self.send_request('cache-query', metric='carbon.foo')
    self.assertEquals({'datapoints': []}, self.response)

  def test_cache_query_returns_cached_datapoints_if_matches(self):
    self.cache.store('carbon.foo', (600, 1.0))
    self.send_request('cache-query', metric='carbon.foo')
    self.assertEqual([(600, 1.0)], self.response['datapoints'])

  def test_cache_bulk_query_returns_response_dict(self):
    self.send_request('cache-query-bulk', metrics=[])
    self.assertIsInstance(self.response, dict)

  def test_cache_bulk_query_response_has_datapointsByMetric(self):
    self.send_request('cache-query-bulk', metrics=[])
    self.assertIn('datapointsByMetric', self.response)

  def test_cache_bulk_query_response_returns_empty_if_no_match(self):
    self.send_request('cache-query-bulk', metrics=[])
    self.assertEquals({'datapointsByMetric': {}}, self.response)

  def test_cache_bulk_query_response(self):
    self.cache.store('carbon.foo', (600, 1.0))
    self.cache.store('carbon.bar', (600, 2.0))

    expected_response = {'carbon.foo': [(600, 1.0)], 'carbon.bar': [(600, 2.0)]}
    self.send_request('cache-query-bulk', metrics=['carbon.foo', 'carbon.bar'])
    self.assertEquals({'datapointsByMetric': expected_response}, self.response)
