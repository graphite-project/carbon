import os
import unittest
from carbon.hashing import ConsistentHashRing

class HashIntegrityTest(unittest.TestCase):

    def test_2_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(2):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_3_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(3):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_4_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(4):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_5_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(5):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_6_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(6):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_7_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(7):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_8_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(8):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_9_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([])
        for n in range(9):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


class FNVHashIntegrityTest(unittest.TestCase):

    def test_2_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(2):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_3_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(3):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_4_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(4):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_5_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(5):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_6_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(6):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_7_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(7):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_8_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(8):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


    def test_9_node_positional_itegrity(self):
        """Make a cluster, verify we don't have positional collisions"""
        ring = ConsistentHashRing([], hash_type='fnv1a_ch')
        for n in range(9):
            ring.add_node(("192.168.10.%s" % str(10+n),"%s" % str(10+n)))
        self.assertEqual(
                len([n[0] for n in ring.ring]),
            len(set([n[0] for n in ring.ring])))


class ConsistentHashRingTestFNV1A(unittest.TestCase):

    def test_chr_compute_ring_position_fnv1a(self):
        hosts = [("127.0.0.1", "ba603c36342304ed77953f84ac4d357b"),
                 ("127.0.0.2", "5dd63865534f84899c6e5594dba6749a"),
                 ("127.0.0.3", "866a18b81f2dc4649517a1df13e26f28")]
        hashring = ConsistentHashRing(hosts, hash_type='fnv1a_ch')
        self.assertEqual(hashring.compute_ring_position('hosts.worker1.cpu'),
                         59573)
        self.assertEqual(hashring.compute_ring_position('hosts.worker2.cpu'),
                         35749)

    def test_chr_get_node_fnv1a(self):
        hosts = [("127.0.0.1", "ba603c36342304ed77953f84ac4d357b"),
                 ("127.0.0.2", "5dd63865534f84899c6e5594dba6749a"),
                 ("127.0.0.3", "866a18b81f2dc4649517a1df13e26f28")]
        hashring = ConsistentHashRing(hosts, hash_type='fnv1a_ch')
        self.assertEqual(hashring.get_node('hosts.worker1.cpu'),
                         ('127.0.0.1', 'ba603c36342304ed77953f84ac4d357b'))
        self.assertEqual(hashring.get_node('hosts.worker2.cpu'),
                         ('127.0.0.3', '866a18b81f2dc4649517a1df13e26f28'))
