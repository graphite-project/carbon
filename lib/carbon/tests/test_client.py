from twisted.trial.unittest import TestCase
from twisted.test import proto_helpers

import carbon.client as carbon_client
from carbon.client import CarbonClientFactory
from carbon.conf import CarbonConfiguration
from carbon.routers import DatapointRouter
from carbon import instrumentation

from struct import unpack, calcsize
from pickle import loads as pickle_loads

from mock import Mock, patch

INT32_FORMAT = '!I'
INT32_SIZE = calcsize(INT32_FORMAT)

class BroadcastRouter(DatapointRouter):
    def __init__(self, destinations=[]):
        self.destinations = set(destinations)

    def addDestination(self, destination):
        self.destinations.append(destination)

    def removeDestination(self, destination):
        self.destinations.discard(destination)

    def getDestinations(self, key):
        for destination in self.destinations:
            yield destination


@patch('carbon.state.instrumentation', Mock(spec=instrumentation))
class ConnectedCarbonClientProtocol(TestCase):
    def setUp(self):
        carbon_client.settings = CarbonConfiguration()  # reset to defaults
        factory = CarbonClientFactory(('127.0.0.1', 0, 'a'))
        self.proto = factory.buildProtocol(('127.0.0.1', 0))
        self.transport = proto_helpers.StringTransport()
        self.proto.makeConnection(self.transport)

    def _decode_sent(self):
        sent_data = self.transport.value()
        pickle_size = unpack(INT32_FORMAT, sent_data[:INT32_SIZE])[0]
        return pickle_loads(sent_data[INT32_SIZE:INT32_SIZE + pickle_size])

    def test_send_datapoint(self):
        datapoint = (1000000000, 1.0)
        self.proto.sendDatapoint('foo.bar.baz', datapoint)
        self.assertEquals([('foo.bar.baz', datapoint)],
                          self._decode_sent())

