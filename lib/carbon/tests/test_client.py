from nose.twistedtools import deferred, reactor

from twisted.internet.task import deferLater
from twisted.test.proto_helpers import StringTransport

import carbon.client as carbon_client
from carbon.client import CarbonClientFactory
from carbon.routers import DatapointRouter
from carbon.tests.util import TestSettings
from carbon import instrumentation

from pickle import loads as pickle_loads
from struct import unpack, calcsize
from unittest import TestCase

from mock import Mock, patch

INT32_FORMAT = '!I'
INT32_SIZE = calcsize(INT32_FORMAT)


def decode_sent(data):
  pickle_size = unpack(INT32_FORMAT, data[:INT32_SIZE])[0]
  return pickle_loads(data[INT32_SIZE:INT32_SIZE + pickle_size])


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
class ConnectedCarbonClientProtocolTest(TestCase):
  def setUp(self):
    carbon_client.settings = TestSettings()  # reset to defaults
    factory = CarbonClientFactory(('127.0.0.1', 0, 'a'))
    self.protocol = factory.buildProtocol(('127.0.0.1', 0))
    self.transport = StringTransport()
    self.protocol.makeConnection(self.transport)

  @deferred(timeout=1.0)
  def test_send_datapoint(self):
    def assert_sent():
      sent_data = self.transport.value()
      sent_datapoints = decode_sent(sent_data)
      self.assertEqual([datapoint], sent_datapoints)

    datapoint = ('foo.bar', (1000000000, 1.0))
    self.protocol.sendDatapoint(*datapoint)
    return deferLater(reactor, 0.1, assert_sent)
