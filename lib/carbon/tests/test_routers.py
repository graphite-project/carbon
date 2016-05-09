import os
from unittest import TestCase

from carbon import routers
from carbon.util import parseDestinations
from carbon.tests import util


DESTINATIONS = ('foo:124:a', 'bar:423:b')


def createSettings():
    settings = util.TestSettings()
    settings['DIVERSE_REPLICAS'] = True,
    settings['REPLICATION_FACTOR'] = 2
    settings['DESTINATIONS'] = DESTINATIONS
    settings['relay-rules'] = os.path.join(
        os.path.dirname(__file__), 'relay-rules.conf')
    settings['aggregation-rules'] = None
    return settings


def parseDestination(destination):
    return parseDestinations([destination])[0]


class TestRelayRulesRouter(TestCase):
    def testBasic(self):
        router = routers.RelayRulesRouter(createSettings())
        for destination in DESTINATIONS:
            router.addDestination(parseDestination(destination))
        self.assertEquals(len(list(router.getDestinations('foo.bar'))), 1)


class TestOtherRouters(TestCase):
    def testBasic(self):
        settings = createSettings()
        for plugin in routers.DatapointRouter.plugins:
            # Test everything except 'rules' which is special
            if plugin == 'rules':
                continue

            router = routers.DatapointRouter.plugins[plugin](settings)
            for destination in DESTINATIONS:
                router.addDestination(parseDestination(destination))
            self.assertEquals(len(list(router.getDestinations('foo.bar'))),
                              settings['REPLICATION_FACTOR'])
