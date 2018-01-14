import unittest

from carbon.aggregator.rules import AGGREGATION_METHODS

PERCENTILE_METHODS = ['p999', 'p99', 'p95', 'p90', 'p80', 'p75', 'p50']
VALUES = [4, 8, 15, 16, 23, 42]


def almost_equal(a, b):
    return abs(a - b) < 0.0000000001


class AggregationMethodTest(unittest.TestCase):
    def test_percentile_simple(self):
        for method in PERCENTILE_METHODS:
            self.assertTrue(almost_equal(AGGREGATION_METHODS[method]([1]), 1))

    def test_percentile_order(self):
        for method in PERCENTILE_METHODS:
            a = AGGREGATION_METHODS[method]([1, 2, 3, 4, 5])
            b = AGGREGATION_METHODS[method]([3, 2, 1, 4, 5])
            self.assertTrue(almost_equal(a, b))

    def test_percentile_values(self):
        examples = [
            ('p999', 41.905, ),
            ('p99', 41.05, ),
            ('p95', 37.25, ),
            ('p90', 32.5, ),
            ('p80', 23, ),
            ('p75', 21.25, ),
            ('p50', 15.5, ),
        ]

        for (method, result) in examples:
            self.assertTrue(almost_equal(AGGREGATION_METHODS[method](VALUES), result))
