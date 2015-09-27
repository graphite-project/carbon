import timeit
from mock import Mock
from carbon.cache import MetricCache, _MetricCache

settings = {
        'MAX_CACHE_SIZE': float('inf'),
        'CACHE_SIZE_LOW_WATERMARK': float('inf'),
        'CACHE_WRITE_STRATEGY': 'naive'
        }
metric_cache = _MetricCache(method="naive")
i = 0

def reset(method):
    i = 0

def command_store_foo():
    global i
    i=i+1
    return metric_cache.store('foo', (i, 1.0))

def command_store_foo_n():
    global i
    i=i+1
    return metric_cache.store("foo.%d" % i, (i, 1.0))

def command_isFull():
    return metric_cache.isFull()

def command_drain():
    while metric_cache:
        metric_cache.pop()
    return metric_cache.size

if __name__ == '__main__':
    import timeit

    print("Benchmarking single metric MetricCache.store()...")
    print("datapoints time")
    for n in [1000, 10000, 100000, 1000000]:
        reset("naive")
        metric_cache = _MetricCache(method="naive")
        print("%d\t%f" % (n, timeit.timeit(command_store_foo, number=n)))

    print("Benchmarking unique metric MetricCache.store()...")
    print("datapoints time")
    for n in [1000, 10000, 100000, 1000000]:
        reset("naive")
        metric_cache = _MetricCache(method="naive")
        print("%d\t%f" % (n, timeit.timeit(command_store_foo_n, number=n)))

    print("Benchmarking MetricCache.isFull()...")
    print("cache_size time")
    for n in [1000, 10000, 100000, 1000000]:
        reset("naive")
        metric_cache = _MetricCache(method="naive")
        timeit.timeit(command_store_foo, number=n)
        print("%d\t%f" % (n, timeit.timeit(command_isFull)))

    print("Benchmarking single metric MetricCache drain...")
    print("strategy cache_size time end_size")
    for strategy in ["sorted", "max", "naive"]:
        for n in [1000, 10000, 100000, 1000000]:
            reset(strategy)
            metric_cache = _MetricCache(method=strategy)
            timeit.timeit(command_store_foo, number=n)
            print("%s\t%d\t%f\t%d" % (strategy, n, timeit.timeit(command_drain, number=1), metric_cache.size))

    print("Benchmarking unique metric MetricCache drain...")
    print("strategy cache_size time end_size")
    for strategy in ["sorted", "max", "naive"]:
        for n in [1000, 10000, 100000, 1000000]:
            if strategy == "max" and n > 10000: # remove me when max is fast
              print("%s\t%d\t%s\t%d" % (strategy, n, 'skipped', 0))
              continue
            reset(strategy)
            metric_cache = _MetricCache(method=strategy)
            timeit.timeit(command_store_foo_n, number=n)
            print("%s\t%d\t%f\t%d" % (strategy, n, timeit.timeit(command_drain, number=1), metric_cache.size))
