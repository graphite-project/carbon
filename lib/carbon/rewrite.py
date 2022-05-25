import re
from collections import defaultdict
from os.path import exists, getmtime
from twisted.internet.task import LoopingCall
from carbon.pipeline import Processor
from carbon import log

# rulesets
PRE = 'pre'
POST = 'post'


class RewriteProcessor(Processor):
  plugin_name = 'rewrite'

  def __init__(self, ruleset):
    self.ruleset = ruleset

  def process(self, metric, datapoint):
    for rule in RewriteRuleManager.rules(self.ruleset):
      metric = rule.apply(metric)
    yield (metric, datapoint)


class _RewriteRuleManager:
  def __init__(self):
    self.rulesets = defaultdict(list)
    self.rules_file = None
    self.read_task = LoopingCall(self.read_rules)
    self.rules_last_read = 0.0

  def clear(self, ruleset=None):
    if ruleset:
      self.rulesets[ruleset] = []
    else:
      self.rulesets.clear()

  def rules(self, ruleset):
    return self.rulesets[ruleset]

  def read_from(self, rules_file):
    self.rules_file = rules_file
    self.read_rules()
    if not self.read_task.running:
      self.read_task.start(10, now=False)

  def read_rules(self):
    if not exists(self.rules_file):
      self.clear()
      return

    # Only read if the rules file has been modified
    try:
      mtime = getmtime(self.rules_file)
    except (OSError, IOError):
      log.err("Failed to get mtime of %s" % self.rules_file)
      return
    if mtime <= self.rules_last_read:
      return

    section = None
    for line in open(self.rules_file):
      line = line.strip()
      if line.startswith('#') or not line:
        continue

      if line.startswith('[') and line.endswith(']'):
        section = line[1:-1].lower()
        self.clear(section)
      elif '=' in line:
        pattern, replacement = line.split('=', 1)
        pattern, replacement = pattern.strip(), replacement.strip()
        try:
          rule = RewriteRule(pattern, replacement)
        except re.error:
          log.err("Invalid regular expression in rewrite rule: '{0}'".format(pattern))
          continue

        self.rulesets[section].append(rule)
      else:
        log.err("Invalid syntax: not a section heading or rule: '{0}'".format(line))

    self.rules_last_read = mtime


class RewriteRule:
  def __init__(self, pattern, replacement):
    self.pattern = pattern
    self.replacement = replacement
    self.regex = re.compile(pattern)

  def apply(self, metric):
    return self.regex.sub(self.replacement, metric)


# Ghetto singleton
RewriteRuleManager = _RewriteRuleManager()
