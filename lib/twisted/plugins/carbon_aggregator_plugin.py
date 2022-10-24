from zope.interface import implementer

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import conf


@implementer(IServiceMaker, IPlugin)
class CarbonAggregatorServiceMaker(object):

    tapname = "carbon-aggregator"
    description = "Aggregate stats for graphite."
    options = conf.CarbonAggregatorOptions

    def makeService(self, options):
        """
        Construct a C{carbon-aggregator} service.
        """
        from carbon import service
        return service.createAggregatorService(options)


# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonAggregatorServiceMaker()
