import re
from carbon.pipeline import Processor


class RewriteProcessor(Processor):
  plugin_name = 'rewrite'

  def process(self, metric, datapoint):
    for rule in RewriteRuleManager.rules:
      metric = rule.apply(metric)
    yield (metric, datapoint)


class RewriteRuleManager:
  def __init__(self):
    self.rules = []

  def read_from(self, path):
    rules = []

    for line in open(path):
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


class RewriteRule:
  def __init__(self, pattern, replacement):
    self.pattern = pattern
    self.replacement = replacement
    self.regex = re.compile(pattern)

  def apply(self, metric):
    return self.regex.sub(self.replacement, metric)


# Ghetto singleton
RewriteRuleManager = RewriteRuleManager()
