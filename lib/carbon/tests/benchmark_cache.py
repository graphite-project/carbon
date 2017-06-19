import timeit

from carbon.cache import _MetricCache, DrainStrategy, \
    NaiveStrategy, MaxStrategy, RandomStrategy, SortedStrategy, TimeSortedStrategy


metric_cache = _MetricCache(DrainStrategy)
count = 0
strategies = {
    'naive': NaiveStrategy,
    'max': MaxStrategy,
    'random': RandomStrategy,
    'sorted': SortedStrategy,
    'timesorted': TimeSortedStrategy,
}

def command_store_foo():
    global count
    count = count + 1
    return metric_cache.store('foo', (count, 1.0))

def command_store_foo_n():
    global count
    count = count + 1
    return metric_cache.store("foo.%d" % count, (count, 1.0))

def command_drain():
    while metric_cache:
        metric_cache.drain_metric()
    return metric_cache.size

def print_stats(n, t):
    usec = t * 1e6
    if usec < 1000:
        print("    datapoints: %-10d usecs: %d" % (n, int(usec)))
    else:
        msec = usec / 1000
        if msec < 1000:
            print("    datapoints: %-10d msecs: %d" % (n, int(msec)))
        else:
            sec = msec / 1000
            print("    datapoints: %-10d  secs: %3g" % (n, sec))


if __name__ == '__main__':
    print("Benchmarking single metric MetricCache store...")
    for n in [1000, 10000, 100000, 1000000]:
        count = 0
        metric_cache = _MetricCache(DrainStrategy)
        t = timeit.timeit(command_store_foo, number=n)
        print_stats(n, t)

    print("Benchmarking unique metric MetricCache store...")
    for n in [1000, 10000, 100000, 1000000]:
        count = 0
        metric_cache = _MetricCache(DrainStrategy)
        t = timeit.timeit(command_store_foo_n, number=n)
        print_stats(n, t)

    print("Benchmarking single metric MetricCache drain...")
    for name, strategy in sorted(strategies.items()):
        print("CACHE_WRITE_STRATEGY: %s" % name)
        for n in [1000, 10000, 100000, 1000000]:
            count = 0
            metric_cache = _MetricCache(strategy)
            timeit.timeit(command_store_foo, number=n)
            t = timeit.timeit(command_drain, number=1)
            print_stats(n, t)

    print("Benchmarking unique metric MetricCache drain...")
    for name, strategy in sorted(strategies.items()):
        print("CACHE_WRITE_STRATEGY: %s" % name)
        for n in [1000, 10000, 100000, 1000000]:
            # remove me when strategy is fast
            if (name == 'max' and n > 10000) or (name == 'random' and n > 100000):
                print("    datapoints: %-10d [skipped]" % n)
                continue
            count = 0
            metric_cache = _MetricCache(strategy)
            timeit.timeit(command_store_foo_n, number=n)
            t = timeit.timeit(command_drain, number=1)
            print_stats(n, t)
