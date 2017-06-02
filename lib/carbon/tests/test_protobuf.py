import carbon.client as carbon_client
from carbon.routers import DatapointRouter
from carbon.tests.util import TestSettings
from carbon import instrumentation
import carbon.service

from carbon.carbon_pb2 import Payload
from carbon.protobuf import CarbonProtobufClientFactory

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.base import DelayedCall
from twisted.internet.task import deferLater
from twisted.trial.unittest import TestCase
from twisted.test.proto_helpers import StringTransport

from mock import Mock, patch
from struct import unpack, calcsize


INT32_FORMAT = '!I'
INT32_SIZE = calcsize(INT32_FORMAT)


def decode_sent(data):
  pb_size = unpack(INT32_FORMAT, data[:INT32_SIZE])[0]
  data = data[INT32_SIZE:INT32_SIZE + pb_size]

  datapoints = []
  payload_pb = Payload.FromString(data)
  for metric_pb in payload_pb.metrics:
    for point_pb in metric_pb.points:
      datapoints.append(
        (metric_pb.metric, (point_pb.timestamp, point_pb.value)))
  return datapoints


@patch('carbon.state.instrumentation', Mock(spec=instrumentation))
class ConnectedCarbonClientProtocolTest(TestCase):
  def setUp(self):
    self.router_mock = Mock(spec=DatapointRouter)
    carbon_client.settings = TestSettings()  # reset to defaults
    factory = CarbonProtobufClientFactory(('127.0.0.1', 2003, 'a'), self.router_mock)
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
