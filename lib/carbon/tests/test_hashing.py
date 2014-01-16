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
