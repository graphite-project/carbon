import time

class Interval:
  __slots__ = ('start', 'end', 'tuple', 'size')

  def __init__(self, start, end):
    if end - start < 0:
      raise ValueError("Invalid interval start=%s end=%s" % (start, end))

    self.start = start
    self.end = end
    self.tuple = (start, end)
    self.size = self.end - self.start

  def __eq__(self, other):
    return self.tuple == other.tuple

  def __hash__(self):
    return hash( self.tuple )

  def __cmp__(self, other):
    return cmp(self.start, other.start)

  def __len__(self):
    raise TypeError("len() doesn't support infinite values, use the 'size' attribute instead")

  def __nonzero__(self):
    return self.size != 0

  def __repr__(self):
    return '<Interval: %s>' % str(self.tuple)

  def intersect(self, other):
    start = max(self.start, other.start)
    end = min(self.end, other.end)

    if end > start:
      return Interval(start, end)

  def overlaps(self, other):
    earlier = self if self.start <= other.start else other
    later = self if earlier is other else other
    return earlier.end >= later.start

  def union(self, other):
    if not self.overlaps(other):
      raise TypeError("Union of disjoint intervals is not an interval")

    start = min(self.start, other.start)
    end = max(self.end, other.end)
    return Interval(start, end)

class FindQuery:
  def __init__(self, pattern, startTime, endTime):
    self.pattern = pattern
    self.startTime = startTime
    self.endTime = endTime
    self.isExact = is_pattern(pattern)
    self.interval = Interval(float('-inf') if startTime is None else startTime,
                             float('inf') if endTime is None else endTime)

  def __repr__(self):
    if self.startTime is None:
      startString = '*'
    else:
      startString = time.ctime(self.startTime)

    if self.endTime is None:
      endString = '*'
    else:
      endString = time.ctime(self.endTime)

    return '<FindQuery: %s from %s until %s>' % (self.pattern, startString, endString)

def is_pattern(s):
   return '*' in s or '?' in s or '[' in s or '{' in s

class Node(object):
  __slots__ = ('name', 'path', 'local', 'is_leaf')

  def __init__(self, path):
    self.path = path
    self.name = path.split('.')[-1]
    self.local = True
    self.is_leaf = False

  def __repr__(self):
    return '<%s[%x]: %s>' % (self.__class__.__name__, id(self), self.path)

class BranchNode(Node):
  pass


class LeafNode(Node):
  __slots__ = ('reader', 'intervals')

  def __init__(self, path, reader):
    Node.__init__(self, path)
    self.reader = reader
    self.intervals = reader.get_intervals()
    self.is_leaf = True

  def fetch(self, startTime, endTime):
    return self.reader.fetch(startTime, endTime)

  def __repr__(self):
    return '<LeafNode[%x]: %s (%s)>' % (id(self), self.path, self.reader)
