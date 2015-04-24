import timeit
from mock import Mock
from carbon.cache import MetricCache, _MetricCache

settings = {
        'MAX_CACHE_SIZE': float('inf'),
        'CACHE_SIZE_LOW_WATERMARK': float('inf'),
        'CACHE_WRITE_STRATEGY': 'naive'
        }
metric_cache = _MetricCache(method="naive")
i=0

def store_command():
    global i
    i=i+1
    return metric_cache.store('foo', (i, 1.0))

if __name__ == '__main__':
    import timeit

    print("Benchmarking MetricCache.store()...")
    print("datapoints time")
    for n in [1000, 10000, 100000, 1000000]:
        metric_cache = _MetricCache(method="naive")
        print("%d\t%f" % (n, timeit.timeit(store_command, number=n)))
