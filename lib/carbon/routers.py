from carbon.hashing import ConsistentHashRing, carbonHash
from carbon.util import PluginRegistrar
from six import with_metaclass
from six.moves import xrange


class DatapointRouter(with_metaclass(PluginRegistrar, object)):
  "Abstract base class for datapoint routing logic implementations"
  plugins = {}

  def addDestination(self, destination):
    "destination is a (host, port, instance) triple"
    raise NotImplementedError()

  def removeDestination(self, destination):
    "destination is a (host, port, instance) triple"
    raise NotImplementedError()

  def hasDestination(self, destination):
    "destination is a (host, port, instance) triple"
    raise NotImplementedError()

  def countDestinations(self):
    "return number of configured destinations"
    raise NotImplementedError()

  def getDestinations(self, key):
    """Generate the destinations where the given routing key should map to. Only
    destinations which are configured (addDestination has been called for it)
    may be generated by this method."""
    raise NotImplementedError()


class ConstantRouter(DatapointRouter):
  plugin_name = 'constant'

  def __init__(self, settings):
    self.destinations = set()

  def addDestination(self, destination):
    self.destinations.add(destination)

  def removeDestination(self, destination):
    self.destinations.discard(destination)

  def hasDestination(self, destination):
    return destination in self.destinations

  def countDestinations(self):
    return len(self.destinations)

  def getDestinations(self, key):
    for destination in self.destinations:
      yield destination


class RelayRulesRouter(DatapointRouter):
  plugin_name = 'rules'

  def __init__(self, settings):
    # We need to import relayrules here to avoid circular dependencies.
    from carbon.relayrules import loadRelayRules

    rules_path = settings["relay-rules"]

    self.rules_path = rules_path
    self.rules = loadRelayRules(rules_path)
    self.destinations = set()

  def addDestination(self, destination):
    self.destinations.add(destination)

  def removeDestination(self, destination):
    self.destinations.discard(destination)

  def hasDestination(self, destination):
    return destination in self.destinations

  def countDestinations(self):
    return len(self.destinations)

  def getDestinations(self, key):
    for rule in self.rules:
      if rule.matches(key):
        for destination in rule.destinations:
          if destination in self.destinations:
            yield destination
        if not rule.continue_matching:
          return


class ConsistentHashingRouter(DatapointRouter):
  plugin_name = 'consistent-hashing'

  def __init__(self, settings):
    replication_factor = settings.REPLICATION_FACTOR
    diverse_replicas = settings.DIVERSE_REPLICAS

    self.replication_factor = int(replication_factor)
    self.diverse_replicas = diverse_replicas
    self.instance_ports = {}  # { (server, instance) : port }
    hash_type = settings.ROUTER_HASH_TYPE or 'carbon_ch'
    self.ring = ConsistentHashRing([], hash_type=hash_type)

  def addDestination(self, destination):
    (server, port, instance) = destination
    if self.hasDestination(destination):
      raise Exception("destination instance (%s, %s) already configured" % (server, instance))
    self.instance_ports[(server, instance)] = port
    self.ring.add_node((server, instance))

  def removeDestination(self, destination):
    (server, port, instance) = destination
    if not self.hasDestination(destination):
      raise Exception("destination instance (%s, %s) not configured" % (server, instance))
    del self.instance_ports[(server, instance)]
    self.ring.remove_node((server, instance))

  def hasDestination(self, destination):
    (server, _, instance) = destination
    return (server, instance) in self.instance_ports

  def countDestinations(self):
    return len(self.instance_ports)

  def getDestinations(self, metric):
    key = self.getKey(metric)
    if self.diverse_replicas:
      used_servers = set()
      for (server, instance) in self.ring.get_nodes(key):
        if server in used_servers:
          continue
        else:
          used_servers.add(server)
          port = self.instance_ports[(server, instance)]
          yield (server, port, instance)
        if len(used_servers) >= self.replication_factor:
          return
    else:
      for (count, node) in enumerate(self.ring.get_nodes(key)):
        if count == self.replication_factor:
          return
        (server, instance) = node
        port = self.instance_ports[(server, instance)]
        yield (server, port, instance)

  def getKey(self, metric):
    return metric


class AggregatedConsistentHashingRouter(DatapointRouter):
  plugin_name = 'aggregated-consistent-hashing'

  def __init__(self, settings):
    from carbon.aggregator.rules import RuleManager
    aggregation_rules_path = settings["aggregation-rules"]
    if aggregation_rules_path:
      RuleManager.read_from(aggregation_rules_path)

    self.hash_router = ConsistentHashingRouter(settings)
    self.agg_rules_manager = RuleManager

  def addDestination(self, destination):
    self.hash_router.addDestination(destination)

  def removeDestination(self, destination):
    self.hash_router.removeDestination(destination)

  def hasDestination(self, destination):
    return self.hash_router.hasDestination(destination)

  def countDestinations(self):
    return self.hash_router.countDestinations()

  def getDestinations(self, key):
    # resolve metric to aggregate forms
    resolved_metrics = []
    for rule in self.agg_rules_manager.rules:
      aggregate_metric = rule.get_aggregate_metric(key)
      if aggregate_metric is None:
        continue
      else:
        resolved_metrics.append(aggregate_metric)

    # if the metric will not be aggregated, send it raw
    # (will pass through aggregation)
    if len(resolved_metrics) == 0:
      resolved_metrics.append(key)

    # get consistent hashing destinations based on aggregate forms
    destinations = set()
    for resolved_metric in resolved_metrics:
      for destination in self.hash_router.getDestinations(resolved_metric):
        destinations.add(destination)

    for destination in destinations:
      yield destination


class FastHashRing(object):
  """A very fast hash 'ring'.

  Instead of trying to avoid rebalancing data when changing
  the list of nodes we try to making routing as fast as we
  can. It's good enough because the current rebalancing
  tools performances depend on the total number of metrics
  and not the number of metrics to rebalance.
  """

  def __init__(self, settings):
    self.nodes = set()
    self.sorted_nodes = []
    self.hash_type = settings.ROUTER_HASH_TYPE or 'mmh3_ch'

  def _hash(self, key):
    return carbonHash(key, self.hash_type)

  def _update_nodes(self):
    self.sorted_nodes = sorted(
      [(self._hash(str(n)), n) for n in self.nodes],
      key=lambda v: v[0]
    )

  def add_node(self, node):
    self.nodes.add(node)
    self._update_nodes()

  def remove_node(self, node):
    self.nodes.discard(node)
    self._update_nodes()

  def get_nodes(self, key):
    if not self.nodes:
      return

    seed = self._hash(key) % len(self.nodes)

    for n in xrange(seed, seed + len(self.nodes)):
      yield self.sorted_nodes[n % len(self.sorted_nodes)][1]


class FastHashingRouter(ConsistentHashingRouter):
  """Same as ConsistentHashingRouter but using FastHashRing."""
  plugin_name = 'fast-hashing'

  def __init__(self, settings):
    super(FastHashingRouter, self).__init__(settings)
    self.ring = FastHashRing(settings)


class FastAggregatedHashingRouter(AggregatedConsistentHashingRouter):
  """Same as AggregatedConsistentHashingRouter but using FastHashRing."""
  plugin_name = 'fast-aggregated-hashing'

  def __init__(self, settings):
    super(FastAggregatedHashingRouter, self).__init__(settings)
    self.hash_router.ring = FastHashRing(settings)
