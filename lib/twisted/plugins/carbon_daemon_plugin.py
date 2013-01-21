from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import service
from carbon import conf


class CarbonDaemonServiceMaker(object):
  implements(IServiceMaker, IPlugin)
  tapname = "carbon-daemon"
  description = "Graphite's Backend"
  options = conf.CarbonDaemonOptions

  def makeService(self, options):
    """
    Construct a C{carbon-daemon} service.
    """
    return service.createDaemonService(options)


# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonDaemonServiceMaker()
