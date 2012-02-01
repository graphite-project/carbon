import time
import re
from os.path import exists, getmtime
from twisted.internet.task import LoopingCall
from carbon.pipeline import Processor
from carbon import instrumentation

instrumentation.configure_stats('pipeline.rewrite_microseconds', ('total', 'min', 'max', 'avg'))

ONE_MILLION = 1000000 # I hate counting zeroes

class RewriteProcessor(Processor):
  plugin_name = 'rewrite'

  def process(self, metric, datapoint):
    t = time.time()
    for rule in RewriteRuleManager.rules:
      metric = rule.apply(metric)
    duration_micros = (time.time() - t) * ONE_MILLION
    instrumentation.append('pipeline.rewrite_microseconds', duration_micros)
    yield (metric, datapoint)


class RewriteRuleManager:
  def __init__(self):
    self.rules = []
    self.read_task = LoopingCall(self.read_rules)
    self.rules_last_read = 0.0

  def read_from(self, rules_file):
    self.rules_file = rules_file
    self.read_rules()
    self.read_task.start(10, now=False)

  def read_rules(self):
    if not exists(self.rules_file):
      self.rules = []
      return

    try:
      mtime = getmtime(self.rules_file)
    except:
      return

    if mtime <= self.rules_last_read:
      return

    rules = []

    for line in open(self.rules_file):
      line = line.strip()
      if line.startswith('#') or not line:
        continue
      elif line.startswith('[') and line.endswith(']'):
        continue # just ignore it, no more sections
      else:
        pattern, replacement = line.split('=', 1)
        pattern, replacement = pattern.strip(), replacement.strip()
        rules.append(RewriteRule(pattern, replacement))

    self.rules = rules
    self.rules_last_read = mtime


class RewriteRule:
  def __init__(self, pattern, replacement):
    self.pattern = pattern
    self.replacement = replacement
    self.regex = re.compile(pattern)

  def apply(self, metric):
    return self.regex.sub(self.replacement, metric)


# Ghetto singleton
RewriteRuleManager = RewriteRuleManager()
