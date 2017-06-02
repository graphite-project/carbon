import carbon.client as carbon_client
from carbon.client import CarbonPickleClientFactory, CarbonPickleClientProtocol, CarbonLineClientProtocol, CarbonClientManager
from carbon.routers import DatapointRouter
from carbon.tests.util import TestSettings
from carbon import instrumentation
import carbon.service

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.internet.task import deferLater
from twisted.trial.unittest import TestCase
from twisted.test.proto_helpers import StringTransport

from mock import Mock, patch
from pickle import loads as pickle_loads
from struct import unpack, calcsize


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
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()  # reset to defaults
    factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.protocol = factory.buildProtocol(('127.0.0.1', 2003))
    self.transport = StringTransport()
    self.protocol.makeConnection(self.transport)

  def test_send_datapoint(self):
    def assert_sent():
      sent_data = self.transport.value()
      sent_datapoints = decode_sent(sent_data)
      self.assertEqual([datapoint], sent_datapoints)

    datapoint = ('foo.bar', (1000000000, 1.0))
    self.protocol.sendDatapoint(*datapoint)
    return deferLater(reactor, 0.1, assert_sent)


class CarbonLineClientProtocolTest(TestCase):
  def setUp(self):
    self.protocol = CarbonLineClientProtocol()
    self.protocol.sendLine = Mock()

  def test_send_datapoints_now(self):
    datapoint = ('foo.bar', (1000000000, 1.0))
    expected_line_to_send = "foo.bar 1.0 1000000000"

    self.protocol._sendDatapointsNow([datapoint])
    self.protocol.sendLine.assert_called_once_with(expected_line_to_send)


@patch('carbon.state.instrumentation', Mock(spec=instrumentation))
class CarbonClientFactoryTest(TestCase):
  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.protocol_mock = Mock(spec=CarbonPickleClientProtocol)
    self.protocol_patch = patch(
      'carbon.client.CarbonPickleClientProtocol', new=Mock(return_value=self.protocol_mock))
    self.protocol_patch.start()
    carbon_client.settings = TestSettings()
    self.factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.connected_factory = CarbonPickleClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
    self.connected_factory.buildProtocol(None)
    self.connected_factory.started = True

  def tearDown(self):
    if self.factory.deferSendPending and self.factory.deferSendPending.active():
      self.factory.deferSendPending.cancel()
    self.protocol_patch.stop()

  def test_schedule_send_schedules_call_to_send_queued(self):
    self.factory.scheduleSend()
    self.assertIsInstance(self.factory.deferSendPending, DelayedCall)
    self.assertTrue(self.factory.deferSendPending.active())

  def test_schedule_send_ignores_already_scheduled(self):
    self.factory.scheduleSend()
    expected_fire_time = self.factory.deferSendPending.getTime()
    self.factory.scheduleSend()
    self.assertTrue(expected_fire_time, self.factory.deferSendPending.getTime())

  def test_send_queued_should_noop_if_not_connected(self):
    self.factory.scheduleSend()
    self.assertFalse(self.protocol_mock.sendQueued.called)

  def test_send_queued_should_call_protocol_send_queued(self):
    self.connected_factory.sendQueued()
    self.protocol_mock.sendQueued.assert_called_once_with()


@patch('carbon.state.instrumentation', Mock(spec=instrumentation))
class CarbonClientManagerTest(TestCase):
  timeout = 1.0
  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    self.factory_mock = Mock(spec=CarbonPickleClientFactory)
    self.client_mgr = CarbonClientManager(self.router_mock)
    self.client_mgr.createFactory = lambda dest: self.factory_mock(dest, self.router_mock)

  def test_start_service_installs_sig_ignore(self):
    from signal import SIGHUP, SIG_IGN

    with patch('signal.signal', new=Mock()) as signal_mock:
      self.client_mgr.startService()
      signal_mock.assert_called_once_with(SIGHUP, SIG_IGN)

  def test_start_service_starts_factory_connect(self):
    factory_mock = Mock(spec=CarbonPickleClientFactory)
    factory_mock.started = False
    self.client_mgr.client_factories[('127.0.0.1', 2003, 'a')] = factory_mock
    self.client_mgr.startService()
    factory_mock.startConnecting.assert_called_once_with()

  def test_stop_service_waits_for_clients_to_disconnect(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startService()
    self.client_mgr.startClient(dest)

    disconnect_deferred = Deferred()
    reactor.callLater(0.1, disconnect_deferred.callback, 0)
    self.factory_mock.return_value.disconnect.return_value = disconnect_deferred
    return self.client_mgr.stopService()

  def test_start_client_instantiates_client_factory(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.factory_mock.assert_called_once_with(dest, self.router_mock)

  def test_start_client_ignores_duplicate(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.client_mgr.startClient(dest)
    self.factory_mock.assert_called_once_with(dest, self.router_mock)

  def test_start_client_starts_factory_if_running(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startService()
    self.client_mgr.startClient(dest)
    self.factory_mock.return_value.startConnecting.assert_called_once_with()

  def test_start_client_adds_destination_to_router(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.router_mock.addDestination.assert_called_once_with(dest)

  def test_stop_client_removes_destination_from_router(self):
    dest = ('127.0.0.1', 2003, 'a')
    self.client_mgr.startClient(dest)
    self.client_mgr.stopClient(dest)
    self.router_mock.removeDestination.assert_called_once_with(dest)
