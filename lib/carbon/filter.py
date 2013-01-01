from os.path import getmtime
import time
import re
from twisted.internet.task import LoopingCall
from carbon import instrumentation
from carbon.pipeline import Processor
from carbon.conf import settings
from carbon import log


instrumentation.configure_counters([
  'filter.datapoints_filtered',
  'filter.datapoints_passed_include',
  'filter.datapoints_passed_default',
])

instrumentation.configure_stats('pipeline.filter_microseconds', ('total', 'min', 'max', 'avg'))

ONE_MILLION = 1000000 # I hate counting zeroes

class FilterProcessor(Processor):
  plugin_name = 'filter'

  def process(self, metric, datapoint):
    t = time.time()
    for metric_filter in FilterRuleManager.filters:
      if metric_filter.action == 'allow':
        if metric_filter.matches(metric):
          instrumentation.increment('filter.datapoints_passed_include')
          duration_micros = (time.time() - t) * ONE_MILLION
          instrumentation.append('pipeline.filter_microseconds', duration_micros)
          yield (metric, datapoint)
          return
      elif metric_filter.action == 'exclude':
        if metric_filter.matches(metric):
          instrumentation.increment('filter.datapoints_filtered')
          duration_micros = (time.time() - t) * ONE_MILLION
          instrumentation.append('pipeline.filter_microseconds', duration_micros)
          return
    instrumentation.increment('filter.datapoints_passed_default')
    duration_micros = (time.time() - t) * ONE_MILLION
    instrumentation.append('pipeline.filter_microseconds', duration_micros)
    yield (metric, datapoint)
    return

class FilterRuleManager:
  def __init__(self):
    self.filters = []
    self.filters_file = None
    self.read_task = LoopingCall(self.read_filters)
    self.filters_last_read = 0.0

  def read_from(self, filter_file):
    self.filters_file = filter_file
    self.read_filters()
    self.read_task.start(10, now=False)

  def read_filters_from_file(self, filename):
    path = settings.get_path(filename)
    filters = {}
    for line in open(path):
      line = line.strip()
      if line.startswith('#') or not line:
        continue
      try:
        action, regex_pattern = line.split(' ', 1)
      except:
        raise ConfigError("Invalid filter line: %s" % line)
      else:
        filters.setdefault(action, []).append( regex_pattern )

    return filters

  def read_filters(self):
    # Only read if the rules file has been modified
    try:
      mtime = getmtime(self.filters_file)
    except:
      log.err("Failed to get mtime of %s" % self.filters_file)
      return

    if mtime <= self.filters_last_read:
      return

    log.filter("reading new filter rules from %s" % self.filters_file)
    filters = self.read_filters_from_file(self.filters_file)

    self.filters = filters
    self.filters_last_read = mtime

# Importable singleton
FilterRuleManager = FilterRuleManager()

