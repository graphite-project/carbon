"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


import time

from carbon import log, instrumentation, state
from carbon.cache import MetricCache
from carbon.conf import settings, load_storage_rules
from carbon.decorators import synchronizedInThread

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.application.service import Service


stats = ('total', 'min', 'max', 'avg')
instrumentation.configure_stats('writer.create_microseconds', stats)
instrumentation.configure_stats('writer.write_microseconds', stats)
instrumentation.configure_stats('writer.datapoints_per_write', ('min', 'max', 'avg'))
instrumentation.configure_counters([
  'writer.create_ratelimit_exceeded',
  'writer.metrics_created',
  'writer.metric_create_errors',
  'writer.datapoints_written',
  'writer.write_operations',
  'writer.write_ratelimit_exceeded',
  'writer.write_errors',
  'writer.cache_full_events',
  'writer.cache_queries',
  'writer.bulk_cache_queries',
])

ONE_MILLION = 1000000  # I hate counting zeroes


class RateLimit(object):
  def __init__(self, limit, seconds):
    self.limit = limit
    self.seconds = seconds
    self.counter = 0
    self.next_reset = time.time() + seconds

  @property
  def exceeded(self):
    self._check()
    return self.counter > self.limit

  def _check(self):
    if time.time() >= self.next_reset:
      self.reset()

  def reset(self):
    self.counter = 0
    now = int(time.time())
    if now % self.seconds:
      current_interval = now - (now % self.seconds)
    else:
      current_interval = now
    self.next_reset = current_interval + self.seconds

  def increment(self, value=1):
    self._check()
    self.counter += value

  def wait(self):
    self._check()
    delay = self.next_reset - time.time()
    if delay > 0:
      time.sleep(delay)
      self.reset()


write_ratelimit = RateLimit(settings.MAX_WRITES_PER_SECOND, 1)
create_ratelimit = RateLimit(settings.MAX_CREATES_PER_MINUTE, 60)


@synchronizedInThread(reactor)
def _drain_metric():
  return MetricCache.drain_metric()


def write_cached_datapoints():
  database = state.database

  while True:
    # Only modify the cache on the main reactor thread
    metric, datapoints = _drain_metric()
    if not metric:
      break
    if write_ratelimit.exceeded:
      #log.writes("write ratelimit exceeded")
      instrumentation.increment('writer.write_ratelimit_exceeded')
      write_ratelimit.wait()

    if not database.exists(metric):
      if create_ratelimit.exceeded:
        instrumentation.increment('writer.create_ratelimit_exceeded')
        #log.creates("create ratelimit exceeded")
        # See if it's time to reset the counter in lieu of of a wait()
        continue  # we *do* want to drop the datapoint here.

      metadata = determine_metadata(metric)
      metadata_string = ' '.join(['%s=%s' % item for item in sorted(metadata.items())])
      try:
        t = time.time()
        database.create(metric, **metadata)
        create_micros = (time.time() - t) * ONE_MILLION
      except:
        log.creates("database create operation failed: %s" % metric)
        instrumentation.increment('writer.create_errors')
        raise
      else:
        create_ratelimit.increment()
        instrumentation.increment('writer.metrics_created')
        instrumentation.append('writer.create_microseconds', create_micros)
        log.creates("created new timeseries in %d microseconds: %s {%s}" %
                    (create_micros, metric, metadata_string))

    try:
      t = time.time()
      database.write(metric, datapoints)
      write_micros = (time.time() - t) * ONE_MILLION
    except:
      log.err("database write operation failed")
      instrumentation.increment('writer.write_errors')
    else:
      write_ratelimit.increment()
      instrumentation.increment('writer.datapoints_written', len(datapoints))
      instrumentation.append('writer.datapoints_per_write', len(datapoints))
      instrumentation.increment('writer.write_operations')
      instrumentation.append('writer.write_microseconds', write_micros)

      if settings.LOG_WRITES:
        log.writes("wrote %d datapoints to %s in %d microseconds" %
                   (len(datapoints), metric, write_micros))


def determine_metadata(metric):
  # Determine metadata from storage rules
  metadata = {}
  for rule in settings.STORAGE_RULES:
    if rule.matches(metric):
      rule.set_defaults(metadata)

  return metadata


def write_forever():
  while reactor.running:
    try:
      write_cached_datapoints()
    except:
      log.err()

    time.sleep(1)  # The writer thread only sleeps when the cache is empty or an error occurs


def reload_storage_rules():
  try:
    settings['STORAGE_RULES'] = load_storage_rules(settings)
  except:
    log.writes("Failed to reload storage schemas")
    log.err()


def shutdown_modify_update_speed():
  global write_ratelimit
  if settings.MAX_WRITES_PER_SECOND_SHUTDOWN != settings.MAX_WRITES_PER_SECOND:
    write_ratelimit = RateLimit(settings.MAX_WRITES_PER_SECOND_SHUTDOWN, 1)
    log.writes("Carbon shutting down.  Changed the update rate to: " + str(settings.MAX_UPDATES_PER_SECOND_ON_SHUTDOWN))


class WriterService(Service):
  def __init__(self):
    self.reload_task = LoopingCall(reload_storage_rules)

  def startService(self):
    self.reload_task.start(60, False)
    reactor.addSystemEventTrigger('before', 'shutdown', shutdown_modify_update_speed)
    reactor.callInThread(write_forever)
    Service.startService(self)

  def stopService(self):
    self.reload_task.stop()
    Service.stopService(self)
