import time
import unittest
from collections import deque
from mocker import MockerTestCase
from mock import Mock, PropertyMock, patch
from carbon.cache import MetricCache, _MetricCache
from carbon.conf import read_config


class FakeOptions(object):

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, value):
        self.__dict__[name] = value


class MetricCacheIntegrity(MockerTestCase):

    def calculate_size(self, cache):
        return reduce(lambda x, y: x + len(y), cache.values(), 0)

    def test_metrics_cache_init(self):
        """A new metric cache should have zero elements"""
        self.assertEqual(0, MetricCache.size)

    def test_write_strategy_sorted(self):
        """Create a metric cache, insert metrics, ensure sorted writes"""
        self.assertEqual("sorted", MetricCache.method)
        now = time.time()
        datapoint1 = (now - 10, float(1))
        datapoint2 = (now, float(2))
        MetricCache.store("d.e.f", datapoint1)
        # Simulate metrics arriving out of time
        MetricCache.store("a.b.c", datapoint2)
        MetricCache.store("a.b.c", datapoint1)

        (m, d) = MetricCache.pop()
        self.assertEqual(("a.b.c", deque([datapoint1, datapoint2])), (m, d))
        (m, d) = MetricCache.pop()
        self.assertEqual(("d.e.f", deque([datapoint1])), (m, d))

        self.assertEqual(0, MetricCache.size)
        self.assertEqual(0, self.calculate_size(MetricCache))

    def test_write_strategy_naive(self):
        """Create a metric cache, insert metrics, ensure naive writes"""
        config = self.makeFile(content="[foo]\nCACHE_WRITE_STRATEGY = naive")
        settings = read_config("carbon-foo",
                               FakeOptions(config=config, instance=None,
                                           pidfile=None, logdir=None),
                               ROOT_DIR="foo")
        cache = _MetricCache(method=settings.CACHE_WRITE_STRATEGY)
        self.assertEqual("naive", cache.method)
        now = time.time()
        datapoint1 = (now - 10, float(1))
        datapoint2 = (now, float(2))
        cache.store("d.e.f", datapoint1)
        cache.store("a.b.c", datapoint1)
        cache.store("a.b.c", datapoint2)

        cache.pop()
        cache.pop()

        self.assertEqual(0, cache.size)
        self.assertEqual(0, self.calculate_size(cache))

    def test_write_strategy_max(self):
        """Create a metric cache, insert metrics, ensure naive writes"""
        config = self.makeFile(content="[foo]\nCACHE_WRITE_STRATEGY = max")
        settings = read_config("carbon-foo",
                               FakeOptions(config=config, instance=None,
                                           pidfile=None, logdir=None),
                               ROOT_DIR="foo")
        cache = _MetricCache(method=settings.CACHE_WRITE_STRATEGY)
        self.assertEqual("max", cache.method)
        now = time.time()
        datapoint1 = (now - 10, float(1))
        datapoint2 = (now, float(2))
        cache.store("d.e.f", datapoint1)
        cache.store("a.b.c", datapoint1)
        cache.store("a.b.c", datapoint2)

        (m, d) = cache.pop()
        self.assertEqual(("a.b.c", deque([datapoint1, datapoint2])), (m, d))
        (m, d) = cache.pop()
        self.assertEqual(("d.e.f", deque([datapoint1])), (m, d))

        self.assertEqual(0, cache.size)
        self.assertEqual(0, self.calculate_size(cache))

    def test_empty_pop(self):
        """Create a metric cache, insert metrics, pop too many times"""
        config = self.makeFile(content="[foo]\nCACHE_WRITE_STRATEGY = naive")
        settings = read_config("carbon-foo",
                               FakeOptions(config=config, instance=None,
                                           pidfile=None, logdir=None),
                               ROOT_DIR="foo")
        cache = _MetricCache(method=settings.CACHE_WRITE_STRATEGY)
        self.assertEqual("naive", cache.method)
        now = time.time()
        datapoint1 = (now - 10, float(1))
        datapoint2 = (now, float(2))
        cache.store("d.e.f", datapoint1)
        cache.store("a.b.c", datapoint1)
        cache.store("a.b.c", datapoint2)

        cache.pop()
        cache.pop()
        with self.assertRaises(KeyError):
            cache.pop()

        self.assertEqual(0, cache.size)
        self.assertEqual(0, self.calculate_size(cache))


class MetricCacheTest(unittest.TestCase):
  def setUp(self):
    settings = {
      'MAX_CACHE_SIZE': float('inf'),
      'CACHE_SIZE_LOW_WATERMARK': float('inf'),
      'CACHE_WRITE_STRATEGY': 'naive'
    }
    self._settings_patch = patch.dict('carbon.conf.settings', settings)
    self._settings_patch.start()
    self.metric_cache = _MetricCache(method="naive")

  def tearDown(self):
    self._settings_patch.stop()

  def test_cache_is_a_dict(self):
    self.assertTrue(issubclass(_MetricCache, dict))

  def test_initial_size(self):
    self.assertEqual(0, self.metric_cache.size)

  def test_store_new_metric(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEqual(1, self.metric_cache.size)
    self.assertEqual(deque([(123456, 1.0)]), self.metric_cache['foo'])

  def test_store_multiple_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.assertEqual(2, self.metric_cache.size)
    result = self.metric_cache['foo']
    self.assertTrue((123456, 1.0) in result)
    self.assertTrue((123457, 2.0) in result)

  """ TODO: FAILING
  def test_store_duplicate_timestamp(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123456, 2.0))
    self.assertEqual(1, self.metric_cache.size)
    self.assertEqual([(123456, 2.0)], self.metric_cache['foo'].items())
  """

  def test_store_checks_fullness(self):
    is_full_mock = Mock()
    with patch.object(_MetricCache, 'isFull', is_full_mock):
      with patch('carbon.cache.events'):
        metric_cache = _MetricCache()
        metric_cache.store('foo', (123456, 1.0))
        is_full_mock.assert_called_once()

  def test_store_on_full_triggers_events(self):
    is_full_mock = Mock(return_value=True)
    with patch.object(_MetricCache, 'isFull', is_full_mock):
      with patch('carbon.cache.events') as events_mock:
        self.metric_cache.store('foo', (123456, 1.0))
        events_mock.return_value.cacheFull.assert_called_once()

  def test_pop_multiple_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    result = self.metric_cache.pop('foo')
    self.assertTrue((123456, 1.0) in result[1])
    self.assertTrue((123457, 2.0) in result[1])

  def test_pop_reduces_size(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.pop('foo')
    self.assertEqual(0, self.metric_cache.size)

  """ TODO: FAILING
  def test_pop_returns_sorted_timestamps(self):
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('foo', (123458, 3.0))
    self.metric_cache.store('foo', (123456, 1.0))
    result = self.metric_cache.pop('foo')
    expected = [(123456, 1.0), (123457, 2.0), (123458, 3.0)]
    self.assertEqual(expected, result)
  """

  def test_pop_raises_on_missing(self):
    self.assertRaises(KeyError, self.metric_cache.pop, 'foo')

  def test_get_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEqual(deque([(123456, 1.0)]), self.metric_cache['foo'])

  def test_get_datapoints_doesnt_pop(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEqual(deque([(123456, 1.0)]), self.metric_cache['foo'])
    self.assertEqual(1, self.metric_cache.size)
    self.assertEqual(deque([(123456, 1.0)]), self.metric_cache['foo'])

  def test_get_datapoints_returns_empty_on_missing(self):
    self.assertEqual(deque([]), self.metric_cache['foo'])

  """ TODO: FAILING
  def test_get_datapoints_returns_sorted_timestamps(self):
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('foo', (123458, 3.0))
    self.metric_cache.store('foo', (123456, 1.0))
    result = self.metric_cache['foo']
    expected = deque([(123456, 1.0), (123457, 2.0), (123458, 3.0)])
    self.assertEqual(expected, result)
  """

  def test_is_full_short_circuits_on_inf(self):
    with patch.object(self.metric_cache, 'size') as size_mock:
      self.metric_cache.isFull
      size_mock.assert_not_called()

  def test_is_full(self):
    self._settings_patch.values['MAX_CACHE_SIZE'] = 2.0
    self._settings_patch.start()
    with patch('carbon.cache.events'):
      self.assertFalse(self.metric_cache.isFull())
      self.metric_cache.store('foo', (123456, 1.0))
      self.assertFalse(self.metric_cache.isFull())
      self.metric_cache.store('foo', (123457, 1.0))
      self.assertTrue(self.metric_cache.isFull())

  def test_counts_one_datapoint(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEqual([('foo', 1)], self.metric_cache.counts)

  def test_counts_two_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.assertEqual([('foo', 2)], self.metric_cache.counts)

  def test_counts_multiple_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('bar', (123458, 3.0))
    self.assertTrue(('foo', 2) in self.metric_cache.counts)
    self.assertTrue(('bar', 1) in self.metric_cache.counts)
