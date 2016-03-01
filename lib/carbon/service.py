"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

from os.path import exists

from twisted.application.service import MultiService
from twisted.application.internet import TCPServer, TCPClient, UDPServer
from twisted.internet.protocol import ServerFactory
from twisted.python.components import Componentized
from twisted.python.log import ILogObserver
# Attaching modules to the global state module simplifies import order hassles
from carbon import state, events, instrumentation, util
from carbon.exceptions import CarbonConfigException
from carbon.log import carbonLogObserver
from carbon.pipeline import Processor, run_pipeline
state.events = events
state.instrumentation = instrumentation


class CarbonReceiverFactory(ServerFactory):
  def buildProtocol(self, addr):
    from carbon.conf import settings

    # Don't establish the connection if we have reached the limit.
    if len(state.connectedMetricReceiverProtocols) < settings.MAX_RECEIVER_CONNECTIONS:
      return ServerFactory.buildProtocol(self, addr)
    else:
      return None


class CarbonRootService(MultiService):
  """Root Service that properly configures twistd logging"""

  def setServiceParent(self, parent):
    MultiService.setServiceParent(self, parent)
    if isinstance(parent, Componentized):
      parent.setComponent(ILogObserver, carbonLogObserver)


def createBaseService(config, settings):
    root_service = CarbonRootService()
    root_service.setName(settings.program)

    if settings.USE_WHITELIST:
      from carbon.regexlist import WhiteList, BlackList
      WhiteList.read_from(settings.whitelist)
      BlackList.read_from(settings.blacklist)

    # Instantiate an instrumentation service that will record metrics about
    # this service.
    from carbon.instrumentation import InstrumentationService

    service = InstrumentationService()
    service.setServiceParent(root_service)

    return root_service


def setupPipeline(pipeline, root_service, settings):
  state.pipeline_processors = []
  for processor in pipeline:
    args = []
    if ':' in processor:
      processor, arglist = processor.split(':', 1)
      args = arglist.split(',')

    if processor == 'aggregate':
      setupAggregatorProcessor(root_service, settings)
    elif processor == 'rewrite':
      setupRewriterProcessor(root_service, settings)
    elif processor == 'relay':
      setupRelayProcessor(root_service, settings)
    elif processor == 'write':
      setupWriterProcessor(root_service, settings)
    else:
      raise ValueError("Invalid pipeline processor '%s'" % processor)

    plugin_class = Processor.plugins[processor]
    state.pipeline_processors.append(plugin_class(*args))

  events.metricReceived.addHandler(run_pipeline)
  events.metricGenerated.addHandler(run_pipeline)

  #XXX This effectively reverts the desired behavior in b1a2aecb as I dont see a clear route to
  # port to pipelines. Perhaps a use case for passing a metric metadata dict along the pipeline?
  events.specialMetricReceived.addHandler(run_pipeline)
  events.specialMetricGenerated.addHandler(run_pipeline)

  def activate_processors():
    for processor in state.pipeline_processors:
      processor.pipeline_ready()

  from twisted.internet import reactor
  reactor.callWhenRunning(activate_processors)


def createCacheService(config):
  from carbon.conf import settings

  root_service = createBaseService(config, settings)
  setupPipeline(['write'], root_service, settings)
  setupReceivers(root_service, settings)

  return root_service


def createAggregatorService(config):
  from carbon.conf import settings

  settings.RELAY_METHOD = 'consistent-hashing'
  root_service = createBaseService(config, settings)
  setupPipeline(['rewrite:pre', 'aggregate', 'rewrite:post', 'relay'], root_service, settings)
  setupReceivers(root_service, settings)

  return root_service


def createRelayService(config):
  from carbon.conf import settings

  root_service = createBaseService(config, settings)
  setupPipeline(['relay'], root_service, settings)
  setupReceivers(root_service, settings)

  return root_service


def setupReceivers(root_service, settings):
  from carbon.protocols import MetricLineReceiver, MetricPickleReceiver, MetricDatagramReceiver

  for protocol, interface, port in [
      (MetricLineReceiver, settings.LINE_RECEIVER_INTERFACE, settings.LINE_RECEIVER_PORT),
      (MetricPickleReceiver, settings.PICKLE_RECEIVER_INTERFACE, settings.PICKLE_RECEIVER_PORT)
    ]:
    if port:
      factory = CarbonReceiverFactory()
      factory.protocol = protocol
      service = TCPServer(port, factory, interface=interface)
      service.setServiceParent(root_service)

  if settings.ENABLE_UDP_LISTENER:
      service = UDPServer(int(settings.UDP_RECEIVER_PORT),
                          MetricDatagramReceiver(),
                          interface=settings.UDP_RECEIVER_INTERFACE)
      service.setServiceParent(root_service)

  if settings.ENABLE_AMQP:
    from carbon import amqp_listener
    amqp_host = settings.AMQP_HOST
    amqp_port = settings.AMQP_PORT
    amqp_user = settings.AMQP_USER
    amqp_password = settings.AMQP_PASSWORD
    amqp_verbose = settings.AMQP_VERBOSE
    amqp_vhost = settings.AMQP_VHOST
    amqp_spec = settings.AMQP_SPEC
    amqp_exchange_name = settings.AMQP_EXCHANGE

    factory = amqp_listener.createAMQPListener(
      amqp_user,
      amqp_password,
      vhost=amqp_vhost,
      spec=amqp_spec,
      exchange_name=amqp_exchange_name,
      verbose=amqp_verbose)
    service = TCPClient(amqp_host, amqp_port, factory)
    service.setServiceParent(root_service)

  if settings.ENABLE_MANHOLE:
    from carbon import manhole

    factory = manhole.createManholeListener()
    service = TCPServer(
      settings.MANHOLE_PORT,
      factory,
      interface=settings.MANHOLE_INTERFACE)
    service.setServiceParent(root_service)


def setupAggregatorProcessor(root_service, settings):
  from carbon.aggregator.processor import AggregationProcessor  # Register the plugin class
  from carbon.aggregator.rules import RuleManager

  aggregation_rules_path = settings["aggregation-rules"]
  if not exists(aggregation_rules_path):
    raise CarbonConfigException("aggregation processor: file does not exist {0}".format(aggregation_rules_path))
  RuleManager.read_from(aggregation_rules_path)


def setupRewriterProcessor(root_service, settings):
  from carbon.rewrite import RewriteRuleManager

  rewrite_rules_path = settings["rewrite-rules"]
  RewriteRuleManager.read_from(rewrite_rules_path)


def setupRelayProcessor(root_service, settings):
  from carbon.routers import AggregatedConsistentHashingRouter, \
      ConsistentHashingRouter, RelayRulesRouter
  from carbon.client import CarbonClientManager

  if settings.RELAY_METHOD == 'consistent-hashing':
    router = ConsistentHashingRouter(settings.REPLICATION_FACTOR, diverse_replicas=settings.DIVERSE_REPLICAS)
  elif settings.RELAY_METHOD == 'aggregated-consistent-hashing':
    from carbon.aggregator.rules import RuleManager
    aggregation_rules_path = settings["aggregation-rules"]
    RuleManager.read_from(aggregation_rules_path)
    router = AggregatedConsistentHashingRouter(RuleManager, settings.REPLICATION_FACTOR, diverse_replicas=settings.DIVERSE_REPLICAS)
  elif settings.RELAY_METHOD == 'rules':
    router = RelayRulesRouter(settings["relay-rules"])

  state.client_manager = CarbonClientManager(router)
  state.client_manager.setServiceParent(root_service)

  for destination in util.parseDestinations(settings.DESTINATIONS):
    state.client_manager.startClient(destination)


def setupWriterProcessor(root_service, settings):
  from carbon import cache  # Register CacheFeedingProcessor
  from carbon.protocols import CacheManagementHandler
  from carbon.writer import WriterService
  from carbon import events

  factory = ServerFactory()
  factory.protocol = CacheManagementHandler
  service = TCPServer(
    settings.CACHE_QUERY_PORT,
    factory,
    interface=settings.CACHE_QUERY_INTERFACE)
  service.setServiceParent(root_service)

  writer_service = WriterService()
  writer_service.setServiceParent(root_service)

  if settings.USE_FLOW_CONTROL:
    events.cacheFull.addHandler(events.pauseReceivingMetrics)
    events.cacheSpaceAvailable.addHandler(events.resumeReceivingMetrics)
