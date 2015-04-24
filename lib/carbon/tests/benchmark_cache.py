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
    metric_cache = _MetricCache(method=method)

def command_store_foo():
    global i
    i=i+1
    return metric_cache.store('foo', (i, 1.0))

def command_store_foo_n():
    global i
    i=i+1
    return metric_cache.store("foo.%d" % i, (i, 1.0))

if __name__ == '__main__':
    import timeit

    print("Benchmarking single metric MetricCache.store()...")
    print("datapoints time")
    for n in [1000, 10000, 100000, 1000000]:
        reset("naive")
        print("%d\t%f" % (n, timeit.timeit(command_store_foo, number=n)))

    print("Benchmarking unique metric MetricCache.store()...")
    print("datapoints time")
    for n in [1000, 10000, 100000, 1000000]:
        reset("naive")
        print("%d\t%f" % (n, timeit.timeit(command_store_foo_n, number=n)))
