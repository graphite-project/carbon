try:
  from hashlib import md5
except ImportError:
  from md5 import md5
import bisect
from carbon.conf import settings


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
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % self.ring_len
    entry = self.ring[index]
    return entry[1]

  def get_nodes(self, key):
    nodes = []
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % self.ring_len
    last_index = (index - 1) % self.ring_len
    nodes_len = len(nodes)
    while nodes_len < self.nodes_len and index != last_index:
      next_entry = self.ring[index]
      (position, next_node) = next_entry
      if next_node not in nodes:
        nodes.append(next_node)
        nodes_len += 1

      index = (index + 1) % self.ring_len

    return nodes
