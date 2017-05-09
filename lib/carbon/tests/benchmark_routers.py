import os
import timeit

from carbon.routers import DatapointRouter
from test_routers import createSettings
from six.moves import xrange


REPLICATION_FACTORS = [1, 4]
DIVERSE_REPLICAS = [True, False]
N_DESTINATIONS = [1, 16, 32, 48]


def print_stats(r, t):
    usec = t * 1e6
    msec = usec / 1000
    text = " %s %s datapoints: %d" % (r.plugin_name, r.__id, r.__count)
    if usec < 1000:
        text += " usecs: %d" % int(usec)
    elif msec < 1000:
        text += " msecs: %d" % int(msec)
    else:
        sec = msec / 1000
        text += " secs: %3g" % sec
    print(text)


def generateDestinations(n):
    for i in xrange(n):
        host_id = i % 10
        instance_id = i
        port = 2000 + i
        yield ('carbon%d' %  host_id, port, instance_id)


def benchmark(router_class):
    for replication_factor in REPLICATION_FACTORS:
        for diverse_replicas in DIVERSE_REPLICAS:
            for n_destinations in N_DESTINATIONS:
                destinations = list(generateDestinations(n_destinations))
                settings = createSettings()
                settings['REPLICATION_FACTOR'] = replication_factor
                settings['DIVERSE_REPLICAS'] = diverse_replicas
                settings['DESTINATIONS'] = destinations

                router = router_class(settings)
                router.__count = 0  # Ugly hack for timeit !
                router.__id = (
                    ' deplication_factor: %d' % replication_factor +
                    ' diverse_replicas: %d' % diverse_replicas +
                    ' n_destinations: %-5d' % n_destinations)
                settings.DESTINATIONS = []
                for destination in destinations:
                    router.addDestination(destination)
                    settings.DESTINATIONS.append(
                        '%s:%s:%s' % (
                            destination[0], destination[1], destination[2]))
                benchmark_router(router)


def benchmark_router(router):

    def router_getDestinations():
        router.__count += 1
        dst = list(router.getDestinations('foo.%d' % router.__count))
        assert(len(dst) != 0)

    n = 100000
    t = timeit.timeit(router_getDestinations, number=n)
    print_stats(router, t)


def main():
    for router_class in DatapointRouter.plugins.values():
        # Skip 'rules' because it's hard to mock.
        if router_class.plugin_name == 'rules':
            continue
        benchmark(router_class)


if __name__ == '__main__':
    main()
