import re

from math import floor, ceil

from os.path import exists, getmtime
from twisted.internet.task import LoopingCall
from cachetools import TTLCache, LRUCache

from carbon import log
from carbon.conf import settings
from carbon.aggregator.buffers import BufferManager


def get_cache():
  ttl = settings.CACHE_METRIC_NAMES_TTL
  size = settings.CACHE_METRIC_NAMES_MAX
  if ttl > 0 and size > 0:
    return TTLCache(size, ttl)
  elif size > 0:
    return LRUCache(size)
  else:
    return dict()


class RuleManager(object):
  def __init__(self):
    self.rules = []
    self.rules_file = None
    self.read_task = LoopingCall(self.read_rules)
    self.rules_last_read = 0.0

  def clear(self):
    self.rules = []

  def read_from(self, rules_file):
    self.rules_file = rules_file
    self.read_rules()
    self.read_task.start(10, now=False)

  def read_rules(self):
    if not exists(self.rules_file):
      self.clear()
      return

    # Only read if the rules file has been modified
    try:
      mtime = getmtime(self.rules_file)
    except OSError:
      log.err("Failed to get mtime of %s" % self.rules_file)
      return
    if mtime <= self.rules_last_read:
      return

    # Read new rules
    log.aggregator("reading new aggregation rules from %s" % self.rules_file)
    new_rules = []
    for line in open(self.rules_file):
      line = line.strip()
      if line.startswith('#') or not line:
        continue

      rule = self.parse_definition(line)
      new_rules.append(rule)

    log.aggregator("clearing aggregation buffers")
    BufferManager.clear()
    self.rules = new_rules
    self.rules_last_read = mtime

  def parse_definition(self, line):
    try:
      left_side, right_side = line.split('=', 1)
      output_pattern, frequency = left_side.split()
      method, input_pattern = right_side.split()
      frequency = int(frequency.lstrip('(').rstrip(')'))
      return AggregationRule(input_pattern, output_pattern, method, frequency)

    except ValueError:
      log.err("Failed to parse rule in %s, line: %s" % (self.rules_file, line))
      raise


class AggregationRule(object):
  def __init__(self, input_pattern, output_pattern, method, frequency):
    self.input_pattern = input_pattern
    self.output_pattern = output_pattern
    self.method = method
    self.frequency = int(frequency)

    if method not in AGGREGATION_METHODS:
      raise ValueError("Invalid aggregation method '%s'" % method)

    self.aggregation_func = AGGREGATION_METHODS[method]
    self.build_regex()
    self.build_template()
    self.cache = get_cache()

  def get_aggregate_metric(self, metric_path):
    if metric_path in self.cache:
      try:
        return self.cache[metric_path]
      except KeyError:
        # The value can expire at any time, so we need to catch this.
        pass

    match = self.regex.match(metric_path)
    result = None

    if match:
      extracted_fields = match.groupdict()
      try:
        result = self.output_template % extracted_fields
      except TypeError:
        log.err("Failed to interpolate template %s with fields %s" % (
          self.output_template, extracted_fields))

    self.cache[metric_path] = result
    return result

  def build_regex(self):
    input_pattern_parts = self.input_pattern.split('.')
    regex_pattern_parts = []

    for input_part in input_pattern_parts:
      if '<<' in input_part and '>>' in input_part:
        i = input_part.find('<<')
        j = input_part.find('>>')
        pre = input_part[:i]
        post = input_part[j + 2:]
        field_name = input_part[i + 2:j]
        regex_part = '%s(?P<%s>.+?)%s' % (pre, field_name, post)

      else:
        i = input_part.find('<')
        j = input_part.find('>')
        if i > -1 and j > i:
          pre = input_part[:i]
          post = input_part[j + 1:]
          field_name = input_part[i + 1:j]
          regex_part = '%s(?P<%s>[^.]+?)%s' % (pre, field_name, post)
        elif input_part == '*':
          regex_part = '[^.]+'
        else:
          regex_part = input_part.replace('*', '[^.]*')

      regex_pattern_parts.append(regex_part)

    regex_pattern = '\\.'.join(regex_pattern_parts) + '$'
    self.regex = re.compile(regex_pattern)

  def build_template(self):
    self.output_template = self.output_pattern.replace('<', '%(').replace('>', ')s')


def avg(values):
  if values:
    return float(sum(values)) / len(values)


def count(values):
  if values:
    return len(values)


def percentile(factor):
  def func(values):
    if values:
      values = sorted(values)
      rank = factor * (len(values) - 1)
      rank_left = int(floor(rank))
      rank_right = int(ceil(rank))

      if rank_left == rank_right:
        return values[rank_left]
      else:
        return values[rank_left] * (rank_right - rank) + values[rank_right] * (rank - rank_left)

  return func


AGGREGATION_METHODS = {
  'sum': sum,
  'avg': avg,
  'min': min,
  'max': max,
  'p50': percentile(0.50),
  'p75': percentile(0.75),
  'p80': percentile(0.80),
  'p90': percentile(0.90),
  'p95': percentile(0.95),
  'p99': percentile(0.99),
  'p999': percentile(0.999),
  'count': count,
}

# Importable singleton
RuleManager = RuleManager()
