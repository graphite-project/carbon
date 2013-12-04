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

from os.path import exists, join
from errno import ENOENT

from twisted.application.service import MultiService
from twisted.application.internet import TCPServer, TCPClient, UDPServer
from twisted.internet.protocol import ServerFactory
from twisted.python.components import Componentized
from twisted.python.log import ILogObserver
from carbon import state, events, instrumentation
from carbon.pipeline import run_pipeline, Processor
from carbon.log import carbonLogObserver
state.events = events
state.instrumentation = instrumentation


class CarbonRootService(MultiService):
  """Root Service that properly configures twistd logging"""

  def setServiceParent(self, parent):
    MultiService.setServiceParent(self, parent)
    if isinstance(parent, Componentized):
      parent.setComponent(ILogObserver, carbonLogObserver)


def createDaemonService(options):
  from carbon.conf import settings

  root_service = CarbonRootService()
  root_service.setName(options['instance'])
  settings['INSTANCE'] = options['instance']

  setupPipeline(root_service, settings)
  setupReceivers(root_service, settings)
  setupInstrumentation(root_service, settings)
  return root_service


def setupPipeline(root_service, settings):
  state.pipeline_processors = []
  for processor in settings.PIPELINE:
    if processor == 'aggregate':
      setupAggregatorProcessor(root_service, settings)
    elif processor == 'rewrite':
      setupRewriterProcessor(root_service, settings)
    elif processor == 'filter':
      setupFilterProcessor(root_service, settings)
    elif processor == 'relay':
      setupRelayProcessor(root_service, settings)
    elif processor == 'write':
      setupWriterProcessor(root_service, settings)
    else:
      raise ValueError("Invalid pipeline processor '%s'" % processor)

    plugin_class = Processor.plugins[processor]
    state.pipeline_processors.append(plugin_class())

  events.metricReceived.addHandler(run_pipeline)
  events.metricGenerated.addHandler(run_pipeline)

  #XXX This effectively reverts the desired behavior in b1a2aecb as I dont see a clear route to
  # port to megacarbon. Perhaps a use case for passing a metric metadata dict along the pipeline?
  events.specialMetricReceived.addHandler(run_pipeline)
  events.specialMetricGenerated.addHandler(run_pipeline)

  def activate_processors():
    for processor in state.pipeline_processors:
      processor.pipeline_ready()

  from twisted.internet import reactor
  reactor.callWhenRunning(activate_processors)


def setupAggregatorProcessor(root_service, settings):
  from carbon.aggregator.processor import AggregationProcessor # to register the plugin class
  from carbon.aggregator.rules import RuleManager
  from carbon import events

  aggregation_rules_path = join(settings.config_dir, "aggregation-rules.conf")
  if not exists(aggregation_rules_path):
    raise IOError(ENOENT, "%s file does not exist")
  RuleManager.read_from(aggregation_rules_path)


def setupFilterProcessor(root_service, settings):
  from carbon.filter import FilterRuleManager

  filter_rules_path = join(settings.config_dir, "filter-rules.conf")
  if not exists(filter_rules_path):
    raise IOError(ENOENT, "%s file does not exist")
  FilterRuleManager.read_from(filter_rules_path)


def setupRewriterProcessor(root_service, settings):
  from carbon.rewrite import RewriteRuleManager

  rewrite_rules_path = join(settings.config_dir, "rewrite-rules.conf")
  if not exists(rewrite_rules_path):
    raise IOError(ENOENT, "%s file does not exist")
  RewriteRuleManager.read_from(rewrite_rules_path)


def setupRelayProcessor(root_service, settings):
  from carbon.routers import AggregatedConsistentHashingRouter, \
      ConsistentHashingRouter, RelayRulesRouter
  from carbon.client import CarbonClientManager

  if settings.RELAY_METHOD == 'consistent-hashing':
    router = ConsistentHashingRouter(settings.REPLICATION_FACTOR)
  elif settings.RELAY_METHOD == 'aggregated-consistent-hashing':
    from carbon.aggregator.rules import RuleManager
    RuleManager.read_from(settings["aggregation-rules"])
    router = AggregatedConsistentHashingRouter(RuleManager, settings.REPLICATION_FACTOR)
  elif settings.RELAY_METHOD == 'relay-rules':
    router = RelayRulesRouter()

  state.client_manager = CarbonClientManager(router)
  state.client_manager.setServiceParent(root_service)

  for destination in settings.DESTINATIONS:
    state.client_manager.startClient(destination)


def setupWriterProcessor(root_service, settings):
  import carbon.cache  # important side-effect: registration of CacheFeedingProcessor
  from carbon.protocols import CacheManagementHandler
  from carbon.writer import WriterService
  from carbon import events

  factory = ServerFactory()
  factory.protocol = CacheManagementHandler
  service = TCPServer(settings.CACHE_QUERY_PORT, factory,
                      interface=settings.CACHE_QUERY_INTERFACE)
  service.setServiceParent(root_service)

  writer_service = WriterService()
  writer_service.setServiceParent(root_service)

  if settings.USE_FLOW_CONTROL:
    events.cacheFull.addHandler(events.pauseReceivingMetrics)
    events.cacheSpaceAvailable.addHandler(events.resumeReceivingMetrics)


def setupReceivers(root_service, settings):
  from carbon.protocols import (MetricLineReceiver, MetricPickleReceiver,
                                MetricDatagramReceiver)

  receiver_protocols = {
    'plaintext-receiver': MetricLineReceiver,
    'pickle-receiver': MetricPickleReceiver,
  }

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
        amqp_user, amqp_password,
        vhost=amqp_vhost, spec=amqp_spec,
        exchange_name=amqp_exchange_name,
        verbose=amqp_verbose)
    service = TCPClient(amqp_host, int(amqp_port), factory)
    service.setServiceParent(root_service)

  for listener in settings.LISTENERS:
    port = listener['port']
    interface = listener['interface']
    if listener['protocol'] == 'tcp':
      factory = ServerFactory()
      factory.protocol = receiver_protocols[listener['type']]
      service = TCPServer(port, factory, interface=interface)
    elif listener['protocol'] == 'udp':
      service = UDPServer(port, MetricDatagramReceiver(), interface=interface)

    service.setServiceParent(root_service)

  if settings.ENABLE_MANHOLE:
    from carbon import manhole

    factory = manhole.createManholeListener()
    service = TCPServer(settings.MANHOLE_PORT, factory,
                        interface=settings.MANHOLE_INTERFACE)
    service.setServiceParent(root_service)


def setupInstrumentation(root_service, settings):
  from carbon.instrumentation import InstrumentationService

  service = InstrumentationService()
  service.setServiceParent(root_service)
