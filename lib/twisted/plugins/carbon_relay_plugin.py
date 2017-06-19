from zope.interface import implementer

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import conf


@implementer(IServiceMaker, IPlugin)
class CarbonRelayServiceMaker(object):

    tapname = "carbon-relay"
    description = "Relay stats for graphite."
    options = conf.CarbonRelayOptions

    def makeService(self, options):
        """
        Construct a C{carbon-relay} service.
        """
        from carbon import service
        return service.createRelayService(options)


# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonRelayServiceMaker()
