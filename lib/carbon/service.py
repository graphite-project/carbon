#!/usr/bin/env python
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
# Attaching modules to the global state module simplifies import order hassles
from carbon import state, events, instrumentation
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

    setupWorkflow(root_service, settings)
    setupReceivers(root_service, settings)
    setupInstrumentation(root_service, settings)
    return root_service



def setupWorkflow(root_service, settings):
    # The order of WORKFLOW determines the order in which event handlers
    # get configured, which determines the order in which they get executed.
    for component in settings.WORKFLOW:
        if component == 'aggregate':
          setupAggregatorComponent(root_service, settings)
        elif component == 'rewrite':
          setupRewriterComponent(root_service, settings)
        elif component == 'relay':
          setupRelayComponent(root_service, settings)
        elif component == 'write':
          setupWriterComponent(root_service, settings)
        else:
          raise ValueError("Invalid workflow component '%s'" % component)


def setupAggregatorComponent(root_service, settings):
    from carbon.aggregator import receiver
    from carbon.aggregator.rules import RuleManager
    from carbon import events

    events.metricReceived.addHandler(receiver.process)

    aggregation_rules_path = join(settings.config_dir, "aggregation-rules.conf")
    if not exists(aggregation_rules_path):
        raise IOError(ENOENT, "%s file does not exist")
    RuleManager.read_from(aggregation_rules_path)


def setupRewriterComponent(root_service, settings):
    from carbon.rewrite import RewriteRuleManager

    rewrite_rules_path = join(settings.config_dir, "rewrite-rules.conf")
    if not exists(rewrite_rules_path):
        raise IOError(ENOENT, "%s file does not exist")
    RewriteRuleManager.read_from(rewrite_rules_path)


def setupRelayComponent(root_service, settings):
    from carbon.routers import ConsistentHashingRouter, RelayRulesRouter
    from carbon.client import CarbonClientManager

    if settings.RELAY_METHOD == 'consistent-hashing':
        router = ConsistentHashingRouter(settings.REPLICATION_FACTOR)
    elif settings.RELAY_METHOD == 'relay-rules':
        router = RelayRulesRouter()

    client_manager = CarbonClientManager(router)
    client_manager.setServiceParent(root_service)

    for destination in settings.DESTINATIONS:
      client_manager.startClient(destination)

    events.metricReceived.addHandler(client_manager.sendDatapoint) #XXX Nope. It's gotta process the datapoints *before* forwarding it.
    events.metricGenerated.addHandler(client_manager.sendDatapoint)


def setupWriterComponent(root_service, settings):
    from carbon.cache import MetricCache
    from carbon.protocols import CacheManagementHandler
    from carbon.writer import WriterService
    from carbon import events

    events.metricReceived.addHandler(MetricCache.store)
    events.metricGenerated.addHandler(MetricCache.store)

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
      'plaintext-receiver' : MetricLineReceiver,
      'pickle-receiver' : MetricPickleReceiver,
    }

    if settings.ENABLE_AMQP:
        from carbon import amqp_listener
        amqp_host = settings.AMQP_HOST
        amqp_port = settings.AMQP_PORT
        amqp_user = settings.AMQP_USER
        amqp_password = settings.AMQP_PASSWORD
        amqp_verbose  = settings.AMQP_VERBOSE
        amqp_vhost    = settings.AMQP_VHOST
        amqp_spec     = settings.AMQP_SPEC
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
