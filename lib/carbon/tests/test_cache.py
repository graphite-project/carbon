import time
from collections import deque
from mocker import MockerTestCase
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

    def test_write_strategy_naive(self):
        """Create a metric cache, insert metrics, ensure naive writes"""
        config = self.makeFile(content="[foo]\nCACHE_WRITE_STRATEGY = naive")
        settings = read_config("carbon-foo",
                               FakeOptions(config=config, instance=None,
                                           pidfile=None, logdir=None),
                               ROOT_DIR="foo")
        cache = _MetricCache(method=settings.CACHE_WRITE_STRATEGY)
        self.assertEqual("naive", cache.method)

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
