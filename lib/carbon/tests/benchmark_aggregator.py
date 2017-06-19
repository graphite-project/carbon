import timeit
import time

from carbon.aggregator.processor import AggregationProcessor, RuleManager, settings
from carbon.aggregator.buffers import BufferManager
from carbon.tests.util import print_stats
from carbon.conf import settings
from carbon import state

METRIC = 'prod.applications.foo.1.requests'
METRIC_AGGR = 'prod.applications.foo.all.requests'
FREQUENCY = 1000


def bench_aggregator_noop():
    RuleManager.clear()
    _bench_aggregator("noop")


def bench_aggregator_sum():
    RuleManager.clear()
    RuleManager.rules = [
        RuleManager.parse_definition(
            ('<env>.applications.<app>.all.requests (%d) =' % FREQUENCY) +
            'sum <env>.applications.<app>.*.requests'),
    ]
    _bench_aggregator("sum")


def bench_aggregator_fake():
    RuleManager.clear()
    RuleManager.rules = [
        RuleManager.parse_definition('foo (60) = sum bar'),
    ]
    _bench_aggregator("fake")


def _bench_aggregator(name):
    print("== %s ==" % name)
    max_intervals = settings['MAX_AGGREGATION_INTERVALS']
    now = time.time() - (max_intervals * FREQUENCY)

    buf = None
    for n in [1, 1000, 10000, 100000, 1000000, 10000000]:
        count = 0
        processor = AggregationProcessor()
        processor.process(METRIC, (now, 1))

        def _process():
            processor.process(METRIC, (now + _process.i, 1))
            if (_process.i % FREQUENCY) == 0 and buf is not None:
                buf.compute_values()
            _process.i += 1
        _process.i = 0

        if buf is None:
            buf = BufferManager.get_buffer(METRIC_AGGR, 1, None)

        t = timeit.timeit(_process, number=n)
        buf.close()
        print_stats(n, t)
    print("")


def main():
    settings.LOG_AGGREGATOR_MISSES = False
    class _Fake(object):
        def metricGenerated(self, metric, datapoint):
            pass

        def increment(self, metric):
            pass

    state.events = _Fake()
    state.instrumentation = _Fake()
    _bench_aggregator("warmup")
    bench_aggregator_noop()
    bench_aggregator_sum()
    bench_aggregator_fake()


if __name__ == '__main__':
    main()
