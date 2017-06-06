from twisted.protocols.basic import Int32StringReceiver

from carbon import log
from carbon.protocols import MetricReceiver
from carbon.client import CarbonClientProtocol, CarbonClientFactory
from carbon.carbon_pb2 import Payload
from google.protobuf.message import DecodeError


class MetricProtobufReceiver(MetricReceiver, Int32StringReceiver):
    plugin_name = "protobuf"
    MAX_LENGTH = 2 ** 20

    def stringReceived(self, data):
        try:
            payload_pb = Payload.FromString(data)
        except DecodeError:
            log.listener('invalid protobuf received from %s, ignoring' % self.peerName)
            return

        for metric_pb in payload_pb.metrics:
            for point_pb in metric_pb.points:
                self.metricReceived(
                    metric_pb.metric, (point_pb.timestamp, point_pb.value))


class CarbonProtobufClientProtocol(CarbonClientProtocol, Int32StringReceiver):

    def _sendDatapointsNow(self, datapoints):
        metrics = {}
        payload_pb = Payload()
        for metric, datapoint in datapoints:
            if metric not in metrics:
                metric_pb = payload_pb.metrics.add()
                metric_pb.metric = metric
                metrics[metric] = metric_pb
            else:
                metric_pb = metrics[metric]
            point_pb = metric_pb.points.add()
            point_pb.timestamp = int(datapoint[0])
            point_pb.value = datapoint[1]

        self.sendString(payload_pb.SerializeToString())


class CarbonProtobufClientFactory(CarbonClientFactory):
    plugin_name = "protobuf"

    def clientProtocol(self):
        return CarbonProtobufClientProtocol()
