try:
  from hashlib import md5
except ImportError:
  from md5 import md5
import bisect
import sys

try:
  import pyhash
  hasher = pyhash.fnv1a_32()
  def fnv32a(string, seed=0x811c9dc5):
    return hasher(string, seed=seed)
except ImportError:
  def fnv32a(string, seed=0x811c9dc5):
    """
    FNV-1a Hash (http://isthe.com/chongo/tech/comp/fnv/) in Python.
    Taken from https://gist.github.com/vaiorabbit/5670985
    """
    hval = seed
    fnv_32_prime = 0x01000193
    uint32_max = 2 ** 32
    for s in string:
      hval = hval ^ ord(s)
      hval = (hval * fnv_32_prime) % uint32_max
    return hval

class ConsistentHashRing:
  def __init__(self, nodes, replica_count=100, hash_type='carbon_ch'):
    self.ring = []
    self.nodes = set()
    self.replica_count = replica_count
    self.hash_type = hash_type
    for node in nodes:
      self.add_node(node)

  def compute_ring_position(self, key):
    if self.hash_type == 'fnv1a_ch':
      if sys.version_info >= (3, 0):
        big_hash = '{:x}'.format(int(fnv32a(key)))
      else:
        big_hash = '{:x}'.format(int(fnv32a(str(key))))
      small_hash = int(big_hash[:4], 16) ^ int(big_hash[4:], 16)
    else:
      if sys.version_info >= (3, 0):
        big_hash = md5(key.encode('utf-8')).hexdigest()
      else:
        big_hash = md5(key).hexdigest()
      small_hash = int(big_hash[:4], 16)
    return small_hash

  def add_node(self, node):
    self.nodes.add(node)
    for i in range(self.replica_count):
      if self.hash_type == 'fnv1a_ch':
        replica_key = "%d-%s" % (i, node[1])
      else:
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
    node = next(node_iter)
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
    search_entry = (position, ())
    index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
    last_index = (index - 1) % len(self.ring)
    while len(nodes) < len(self.nodes) and index != last_index:
      next_entry = self.ring[index]
      (position, next_node) = next_entry
      if next_node not in nodes:
        nodes.add(next_node)
        yield next_node

      index = (index + 1) % len(self.ring)
