from unittest import TestCase
from mock import Mock, patch
from carbon.cache import _MetricCache
from carbon.cache import *

class PropertyMock(Mock):
  def __get__(self, instance, owner):
    return self()

class MetricCacheTest(TestCase):
  def setUp(self):
    self.strategy_mock = Mock(spec=DrainStrategy)
    self.metric_cache = _MetricCache(self.strategy_mock)

  def test_cache_is_a_dict(self):
    self.assertTrue(issubclass(_MetricCache, dict))

  def test_initial_size(self):
    self.assertEquals(0, self.metric_cache.size)

  def test_store_new_metric(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEqual(1, self.metric_cache.size)
    self.assertEqual([(123456, 1.0)], self.metric_cache['foo'].items())

  def test_store_multiple_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.assertEqual(2, self.metric_cache.size)
    result = self.metric_cache['foo'].items()
    self.assertTrue((123456, 1.0) in result)
    self.assertTrue((123457, 2.0) in result)

  def test_store_duplicate_timestamp(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123456, 2.0))
    self.assertEqual(1, self.metric_cache.size)
    self.assertEqual([(123456, 2.0)], self.metric_cache['foo'].items())

  def test_store_checks_fullness(self):
    is_full_mock = PropertyMock()
    with patch.object(_MetricCache, 'is_full', is_full_mock):
      with patch('carbon.state.events'):
        metric_cache = _MetricCache()
        metric_cache.store('foo', (123456, 1.0))
        is_full_mock.assert_called_once()

  def test_store_on_full_triggers_events(self):
    is_full_mock = PropertyMock(return_value=True)
    with patch.object(_MetricCache, 'is_full', is_full_mock):
      with patch('carbon.state.events') as events_mock:
        self.metric_cache.store('foo', (123456, 1.0))
        events_mock.return_value.cacheFull.assert_called_once()

  def test_pop_multiple_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    result = self.metric_cache.pop('foo')
    self.assertTrue((123456, 1.0) in result)
    self.assertTrue((123457, 2.0) in result)

  def test_pop_reduces_size(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.pop('foo')
    self.assertEqual(0, self.metric_cache.size)

  def test_pop_triggers_space_check(self):
    with patch.object(self.metric_cache, '_check_available_space') as check_space_mock:
      self.metric_cache.store('foo', (123456, 1.0))
      self.metric_cache.pop('foo')
      check_space_mock.assert_called_once()

  def test_pop_returns_sorted_timestamps(self):
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('foo', (123458, 3.0))
    self.metric_cache.store('foo', (123456, 1.0))
    result = self.metric_cache.pop('foo')
    expected = [(123456, 1.0), (123457, 2.0), (123458, 3.0)]
    self.assertEqual(expected, result)

  def test_pop_raises_on_missing(self):
    self.assertRaises(KeyError, self.metric_cache.pop, 'foo')

  def test_get_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEqual([(123456, 1.0)], self.metric_cache.get_datapoints('foo'))

  def test_get_datapoints_doesnt_pop(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEqual([(123456, 1.0)], self.metric_cache.get_datapoints('foo'))
    self.assertEqual(1, self.metric_cache.size)
    self.assertEqual([(123456, 1.0)], self.metric_cache.get_datapoints('foo'))

  def test_get_datapoints_returns_empty_on_missing(self):
    self.assertEqual([], self.metric_cache.get_datapoints('foo'))

  def test_get_datapoints_returns_sorted_timestamps(self):
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('foo', (123458, 3.0))
    self.metric_cache.store('foo', (123456, 1.0))
    result = self.metric_cache.get_datapoints('foo')
    expected = [(123456, 1.0), (123457, 2.0), (123458, 3.0)]
    self.assertEqual(expected, result)

  def test_drain_metric_respects_strategy(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('bar', (123456, 1.0))
    self.metric_cache.store('baz', (123456, 1.0))
    self.strategy_mock.return_value.choose_item.side_effect = ['bar', 'baz', 'foo']
    self.assertEqual('bar', self.metric_cache.drain_metric()[0])
    self.assertEqual('baz', self.metric_cache.drain_metric()[0])
    self.assertEqual('foo', self.metric_cache.drain_metric()[0])

  def test_drain_metric_works_without_strategy(self):
    metric_cache = _MetricCache()  # No strategy

    metric_cache.store('foo', (123456, 1.0))
    self.assertEqual('foo', metric_cache.drain_metric()[0])

  def test_is_full_short_circuits_on_inf(self):
    with patch.dict('carbon.conf.settings', {
      'MAX_CACHE_SIZE': float('inf')
    }):
      with patch.object(self.metric_cache, 'size') as size_mock:
        self.metric_cache.is_full
        size_mock.assert_not_called()

  def test_is_full(self):
    with patch.dict('carbon.conf.settings', {
      'MAX_CACHE_SIZE': 2
    }):
      with patch('carbon.state.events'):
        self.assertFalse(self.metric_cache.is_full)
        self.metric_cache.store('foo', (123456, 1.0))
        self.assertFalse(self.metric_cache.is_full)
        self.metric_cache.store('foo', (123457, 1.0))
        self.assertTrue(self.metric_cache.is_full)

  def test_counts_one_datapoint(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.assertEquals([('foo', 1)], self.metric_cache.counts)

  def test_counts_two_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.assertEquals([('foo', 2)], self.metric_cache.counts)

  def test_counts_multiple_datapoints(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('bar', (123458, 3.0))
    self.assertTrue(('foo', 2) in self.metric_cache.counts)
    self.assertTrue(('bar', 1) in self.metric_cache.counts)


class DrainStrategyTest(TestCase):
  def setUp(self):
    self.metric_cache = _MetricCache()

  def test_max_strategy(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('foo', (123458, 3.0))
    self.metric_cache.store('bar', (123459, 4.0))
    self.metric_cache.store('bar', (123460, 5.0))
    self.metric_cache.store('baz', (123461, 6.0))

    max_strategy = MaxStrategy(self.metric_cache)
    # foo has 3
    self.assertEquals('foo', max_strategy.choose_item())
    # add 2 more 'bar' for 4 total
    self.metric_cache.store('bar', (123462, 8.0))
    self.metric_cache.store('bar', (123463, 9.0))
    self.assertEquals('bar', max_strategy.choose_item())

    self.metric_cache.pop('foo')
    self.metric_cache.pop('bar')
    self.assertEquals('baz', max_strategy.choose_item())

  def test_sorted_strategy_static_cache(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('foo', (123458, 3.0))
    self.metric_cache.store('bar', (123459, 4.0))
    self.metric_cache.store('bar', (123460, 5.0))
    self.metric_cache.store('baz', (123461, 6.0))

    sorted_strategy = SortedStrategy(self.metric_cache)
    # In order from most to least
    self.assertEquals('foo', sorted_strategy.choose_item())
    self.assertEquals('bar', sorted_strategy.choose_item())
    self.assertEquals('baz', sorted_strategy.choose_item())

  def test_sorted_strategy_changing_sizes(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('foo', (123457, 2.0))
    self.metric_cache.store('foo', (123458, 3.0))
    self.metric_cache.store('bar', (123459, 4.0))
    self.metric_cache.store('bar', (123460, 5.0))
    self.metric_cache.store('baz', (123461, 6.0))

    sorted_strategy = SortedStrategy(self.metric_cache)
    # In order from most to least foo, bar, baz
    self.assertEquals('foo', sorted_strategy.choose_item())

    # 'baz' gets 2 more, now greater than 'bar'
    self.metric_cache.store('baz', (123461, 6.0))
    self.metric_cache.store('baz', (123461, 6.0))
    # But 'bar' is popped anyway, because sort has already happened
    self.assertEquals('bar', sorted_strategy.choose_item())
    self.assertEquals('baz', sorted_strategy.choose_item())

    # Sort happens again
    self.assertEquals('foo', sorted_strategy.choose_item())
    self.assertEquals('bar', sorted_strategy.choose_item())
    self.assertEquals('baz', sorted_strategy.choose_item())


class RandomStrategyTest(TestCase):
  def setUp(self):
    self.metric_cache = _MetricCache()

  def test_random_strategy(self):
    self.metric_cache.store('foo', (123456, 1.0))
    self.metric_cache.store('bar', (123457, 2.0))
    self.metric_cache.store('baz', (123458, 3.0))

    strategy = RandomStrategy(self.metric_cache)
    for _i in range(3):
      item = strategy.choose_item()
      self.assertTrue(item in self.metric_cache)
      self.metric_cache.pop(item)

