from zope.interface import implementer

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import conf


@implementer(IServiceMaker, IPlugin)
class CarbonCacheServiceMaker(object):

    tapname = "carbon-cache"
    description = "Collect stats for graphite."
    options = conf.CarbonCacheOptions

    def makeService(self, options):
        """
        Construct a C{carbon-cache} service.
        """
        from carbon import service
        return service.createCacheService(options)


# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonCacheServiceMaker()
