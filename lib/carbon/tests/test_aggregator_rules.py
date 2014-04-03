import os
import unittest
from carbon.aggregator.rules import AggregationRule

class AggregationRuleTest(unittest.TestCase):

    def test_inclusive_regexes(self):
        """
        Test case for https://github.com/graphite-project/carbon/pull/120

        Consider the two rules:

        aggregated.hist.p99        (10) = avg hosts.*.hist.p99
        aggregated.hist.p999       (10) = avg hosts.*.hist.p999

        Before the abovementioned patch the second rule would be treated as
        expected but the first rule would lead to an aggegated metric
        aggregated.hist.p99 which would in fact be equivalent to
        avgSeries(hosts.*.hist.p99,hosts.*.hist.p999).
        """

        method = 'avg'
        frequency = 10

        input_pattern = 'hosts.*.hist.p99'
        output_pattern = 'aggregated.hist.p99'
        rule99 = AggregationRule(input_pattern, output_pattern,
                                 method, frequency)

        input_pattern = 'hosts.*.hist.p999'
        output_pattern = 'aggregated.hist.p999'
        rule999 = AggregationRule(input_pattern, output_pattern,
                                  method, frequency)

        self.assertEqual(rule99.get_aggregate_metric('hosts.abc.hist.p99'),
                         'aggregated.hist.p99')
        self.assertEqual(rule99.get_aggregate_metric('hosts.abc.hist.p999'),
                         None)

        self.assertEqual(rule999.get_aggregate_metric('hosts.abc.hist.p99'),
                         None)
        self.assertEqual(rule999.get_aggregate_metric('hosts.abc.hist.p999'),
                         'aggregated.hist.p999')
