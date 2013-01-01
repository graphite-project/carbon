import os
import time
import socket
from resource import getrusage, RUSAGE_SELF
from threading import Lock

from twisted.application.service import Service
from twisted.internet.task import LoopingCall

from carbon import state, events
from carbon.conf import settings

HOSTNAME = socket.gethostname().replace('.','_')
PAGESIZE = os.sysconf('SC_PAGESIZE')

# Module state
metric_data = {}
metric_data_lock = Lock()
custom_stats = {}
counter_metrics = set([
  'metrics_received',
])

def avg(values):
  if values:
    return sum(values) / len(values)

def latest(values):
  if values:
    return values[-1]

stat_functions = {
  'total' : sum,
  'min' : min,
  'max' : max,
  'avg' : avg,
  'latest' : latest,
}

metric_functions = {}

def get_stat_functions(metric):
  stat_names = custom_stats.get(metric, stat_functions.keys())
  return dict([(name, stat_functions[name]) for name in stat_names])

# API
def configure_stats(metric, stats):
  custom_stats[metric] = stats


def configure_counters(metrics):
  global counter_metrics
  counter_metrics |= set(metrics)


def configure_metric_function(metric, func):
  metric_functions[metric] = func


def increment(metric, value=1):
  metric_data_lock.acquire()
  try:
    metric_data[metric] += value
  except KeyError:
    metric_data[metric] = value
  finally:
    metric_data_lock.release()


def append(metric, value):
  metric_data_lock.acquire()
  try:
    if metric not in metric_data:
      metric_data[metric] = []
    metric_data[metric].append(value)
  finally:
    metric_data_lock.release()

# end API


class InstrumentationService(Service):
  def __init__(self):
    self.record_task = LoopingCall(self.record_metrics)
    self.metric_prefix = "%s.carbon-daemons.%s.%s." % (settings.CARBON_METRIC_PREFIX, HOSTNAME, state.settings.INSTANCE)

  def startService(self):
    if settings.CARBON_METRIC_INTERVAL > 0:
      self.record_task.start(settings.CARBON_METRIC_INTERVAL, False)
    Service.startService(self)

  def stopService(self):
    if self.record_task.running:
      self.record_task.stop()
    Service.stopService(self)

  def record_metrics(self):
    now = time.time()

    # accumulated data
    metric_data_lock.acquire()
    try:
      my_metric_data = metric_data.copy()
      metric_data.clear()
      for metric in counter_metrics:
        metric_data[metric] = 0
    finally:
      metric_data_lock.release()

    for metric, data in my_metric_data.items():
      if isinstance(data, list):
        for stat_metric, value in self._compute_stats(metric, data):
          metric_path = self.metric_prefix + stat_metric
          datapoint = (now, value)
          events.metricGenerated(metric_path, datapoint)

      else: # counters
        metric_path = self.metric_prefix + metric
        datapoint = (now, data)
        events.metricGenerated(metric_path, datapoint)

    # metric functions
    for metric, func in metric_functions.items():
      metric_path = self.metric_prefix + metric
      try:
        value = func()
      except:
        log.err()
      else:
        if value is not None:
          events.metricGenerated(metric_path, (now, value))

  def _compute_stats(self, metric, data):
    for stat, func in get_stat_functions(metric).items():
      value = func(data)
      if value is not None:
        yield (metric + '.' + stat, value)


# Toss in a few system metrics for good measure
rusage = getrusage(RUSAGE_SELF)
last_usage = rusage.ru_utime + rusage.ru_stime
last_usage_time = time.time()

def get_cpu_usage():
  global last_usage, last_usage_time

  rusage = getrusage(RUSAGE_SELF)
  current_usage = rusage.ru_utime + rusage.ru_stime
  now = time.time()

  usage_diff = current_usage - last_usage
  time_diff = now - last_usage_time

  if time_diff == 0: #shouldn't be possible, but I've actually seen a ZeroDivisionError from this
    time_diff = 0.000001

  cpu_usage_percent = (usage_diff / time_diff) * 100.0
  cpu_usage_percent = max(cpu_usage_percent, 0.0)

  last_usage = current_usage
  last_usage_time = now

  return cpu_usage_percent


def get_mem_usage():
  rss_pages = int( open('/proc/self/statm').read().split()[1] )
  return rss_pages * PAGESIZE


configure_metric_function('cpu_usage', get_cpu_usage)
configure_metric_function('mem_usage', get_mem_usage)
