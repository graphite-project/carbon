from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import conf


class CarbonAggregatorCacheServiceMaker(object):

    implements(IServiceMaker, IPlugin)
    tapname = "carbon-aggregator-cache"
    description = "Aggregate and write stats for graphite."
    options = conf.CarbonAggregatorOptions

    def makeService(self, options):
        """
        Construct a C{carbon-aggregator-cache} service.
        """
        from carbon import service
        return service.createAggregatorCacheService(options)


# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonAggregatorCacheServiceMaker()
