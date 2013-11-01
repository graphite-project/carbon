try:
  from hashlib import md5
except ImportError:
  from md5 import md5
import bisect


class ConsistentHashRing:
  def __init__(self, nodes, replica_count=100):
    self.ring = []
    self.ring_len = len(self.ring)
    self.nodes = set()
    self.nodes_len = len(self.nodes)
    self.replica_count = replica_count
    for node in nodes:
      self.add_node(node)

  def compute_ring_position(self, key):
    big_hash = md5( str(key) ).hexdigest()
    small_hash = int(big_hash[:4], 16)
    return small_hash

  def add_node(self, node):
    self.nodes.add(node)
    self.nodes_len = len(self.nodes)
    for i in range(self.replica_count):
      replica_key = "%s:%d" % (node, i)
      position = self.compute_ring_position(replica_key)
      entry = (position, node)
      bisect.insort(self.ring, entry)
    self.ring_len = len(self.ring)

  def remove_node(self, node):
    self.nodes.discard(node)
    self.nodes_len = len(self.nodes)
    self.ring = [entry for entry in self.ring if entry[1] != node]
    self.ring_len = len(self.ring)

  def get_node(self, key):
    assert self.ring
    node = None
    node_iter = self.get_nodes(key)
    node = node_iter.next()
    node_iter.close()
    return node

  def get_nodes(self, key):
    assert self.ring
    nodes = set()
    uniq_nodes = []
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % self.ring_len
    last_index = (index - 1) % self.ring_len
    nodes_len = len(nodes)
    while nodes_len < self.nodes_len and index != last_index:
      next_entry = self.ring[index]
      (position, next_node) = next_entry
      if next_node[0] not in uniq_nodes:
        nodes.add(next_node)
        uniq_nodes.append(next_node[0])
        yield next_node

      index = (index + 1) % len(self.ring)
