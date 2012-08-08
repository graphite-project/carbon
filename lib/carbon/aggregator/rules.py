import math
import time
import re
from functools import partial
from os.path import exists, getmtime
from twisted.internet.task import LoopingCall
from carbon import log
from carbon.aggregator.buffers import BufferManager

# <metric>.p10 (60) = p10 <<metric>>.histogram.<bucket>

class RuleManager:
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
    except:
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
      frequency = int( frequency.lstrip('(').rstrip(')') )
      return AggregationRule(input_pattern, output_pattern, method, frequency)

    except:
      log.err("Failed to parse line: %s" % line)
      raise


def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. N will be sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    N = sorted(N)
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1


class AggregationRule:
  def __init__(self, input_pattern, output_pattern, method, frequency):
    self.input_pattern = input_pattern
    self.output_pattern = output_pattern
    self.method = method
    self.frequency = int(frequency)

    if method not in AGGREGATION_METHODS:
      if method.startswith("p") and method[1:].isdigit():
        self.aggregation_func = partial(percentile,
          percent = (int(method[1:])*1.0)/100)
      else:
        raise ValueError("Invalid aggregation method '%s'" % method)
    else:
      self.aggregation_func = AGGREGATION_METHODS[method]
    self.build_regex()
    self.build_template()
    self.cache = {}

  def get_aggregate_metric(self, metric_path):
    if metric_path not in self.cache:
      match = self.regex.match(metric_path)
      result = None

      if match:
        extracted_fields = match.groupdict()
        try:
          result = self.output_template % extracted_fields
        except:
          log.err("Failed to interpolate template %s with fields %s" % (self.output_template, extracted_fields))

        self.cache[metric_path] = (match.groupdict(), result)

    return self.cache.get(metric_path)

  def build_regex(self):
    input_pattern_parts = self.input_pattern.split('.')
    regex_pattern_parts = []

    for input_part in input_pattern_parts:
      if '<<' in input_part and '>>' in input_part:
        i = input_part.find('<<')
        j = input_part.find('>>')
        pre = input_part[:i]
        post = input_part[j+2:]
        field_name = input_part[i+2:j]
        regex_part = '%s(?P<%s>.+)%s' % (pre, field_name, post)

      else:
        i = input_part.find('<')
        j = input_part.find('>')
        if i > -1 and j > i:
          pre = input_part[:i]
          post = input_part[j+1:]
          field_name = input_part[i+1:j]
          regex_part = '%s(?P<%s>[^.]+)%s' % (pre, field_name, post)
        elif input_part == '*':
          regex_part = '[^.]+'
        else:
          regex_part = input_part.replace('*', '[^.]*')

      regex_pattern_parts.append(regex_part)

    regex_pattern = '\\.'.join(regex_pattern_parts)
    self.regex = re.compile(regex_pattern)

  def build_template(self):
    self.output_template = self.output_pattern.replace('<', '%(').replace('>', ')s')


def avg(values):
  if values:
    return float( sum(values) ) / len(values)


AGGREGATION_METHODS = {
  'sum' : sum,
  'avg' : avg
}

# Importable singleton
RuleManager = RuleManager()
