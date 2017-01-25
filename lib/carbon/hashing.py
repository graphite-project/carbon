try:
  from hashlib import md5
except ImportError:
  from md5 import md5
import bisect


class ConsistentHashRing:
  def __init__(self, nodes, replica_count=100):
    self.ring = []
    self.nodes = set()
    self.replica_count = replica_count
    for node in nodes:
      self.add_node(node)

  def compute_ring_position(self, key):
    big_hash = md5(str(key)).hexdigest()
    small_hash = int(big_hash[:4], 16)
    return small_hash

  def add_node(self, node):
    self.nodes.add(node)
    for i in range(self.replica_count):
      replica_key = "%s:%d" % (node, i)
      position = self.compute_ring_position(replica_key)
      while position in [r[0] for r in self.ring]:
        position = position + 1
      entry = (position, node)
      bisect.insort(self.ring, entry)

  def remove_node(self, node):
    self.nodes.discard(node)
    self.ring = [entry for entry in self.ring if entry[1] != node]

  def get_node(self, key):
    assert self.ring
    node = None
    node_iter = self.get_nodes(key)
    node = node_iter.next()
    node_iter.close()
    return node

  def get_nodes(self, key):
    if not self.ring:
      return
    if len(self.nodes) == 1:
      # short circuit in simple 1-node case
      for node in self.nodes:
        yield node
        return
    nodes = set()
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
    last_index = (index - 1) % len(self.ring)
    while len(nodes) < len(self.nodes) and index != last_index:
      next_entry = self.ring[index]
      (position, next_node) = next_entry
      if next_node not in nodes:
        nodes.add(next_node)
        yield next_node

      index = (index + 1) % len(self.ring)
