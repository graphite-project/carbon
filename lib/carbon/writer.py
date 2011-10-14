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


import os
import time
from os.path import join, exists, dirname, basename

from carbon import state
from carbon.cache import MetricCache
from carbon.conf import settings
from carbon import log, events, instrumentation

from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.application.service import Service
'''
ceres needs a muthafuckin node cache 
'''
#XXX
ONE_MILLION = 1000000 # I hate counting zeroes


class RateLimit(object):
  def __init__(self, limit, seconds):
    self.limit = limit
    self.seconds = seconds
    self.counter = 0
    self.last_reset = self._current_interval()

  def _current_interval(self):
    now = int(time.time())
    return now - (now % self.seconds)

  def _maybe_reset(self):
    now = time.time()
    if now - self.last_reset >= self.seconds:
      self.last_reset = self._current_interval() + self.seconds

  def increment(self, value=1):
    self._maybe_reset()
    self.counter += value
    .

  def exceeded(self):
    self._maybe_reset()
    return self.counter >= self.limit

  def wait(self):
    ...


write_ratelimit = RateLimit(settings.MAX_WRITES_PER_SECOND, 1)
create_ratelimit = RateLimit(settings.MAX_CREATES_PER_MINUTE, 60)


def writeCachedDatapoints():
  for (metric, datapoints) in MetricCache.drain():
    if write_ratelimit.exceeded():
      write_ratelimit.wait()

    if not database.exists(metric):
      if create_ratelimit.exceeded():
        continue # we *do* want to drop the datapoint here.

      metadata = {}
      for rule in settings.STORAGE_RULES:
        if rule.matches(metric):
          rule.set_defaults(metadata)

      metadata_string = ' '.join(['%s=%s' % item for item in sorted(metadata.items())])
      try:
        t = time.time()
        database.create(metric, **metadata)
        create_micros = (time.time() - t) / ONE_MILLION
      except:
        log.creates("database create operation failed: %s" % metric)
        instrumentation.increment('writer.create_errors')
        raise
      else:
        create_ratelimit.increment()
        instrumentation.increment('writer.metrics_created')
        instrumentation.append("writer.create_microseconds", create_micros) #XXX total/min/max/avg

    try:
      t = time.time()
      database.write(metric, datapoints)
      write_micros = (time.time() - t) / ONE_MILLION
    except:
      log.err("database write operation failed")
      instrumentation.increment('writer.write_errors')
    else:
      write_ratelimit.increment()
      instrumentation.increment('writer.datapoints_written', len(datapoints))
      instrumentation.append('writer.write_microseconds', write_micros) #XXX total/min/max/avg

      if settings.LOG_WRITES:
        log.writes("wrote %d datapoints to %s in %d microseconds" %
                   (len(datapoints), metric, write_micros))


def writeForever():
  while reactor.running:
    try:
      writeCachedDataPoints()
    except:
      log.err()

    time.sleep(1) # The writer thread only sleeps when the cache is empty or an error occurs


def reloadStorageSchemas(): #XXX how the fuck does... conf.read_listeners() { settings['LISTENERS'] = CarbonConfiguration.read_file('listeners.conf') }
  global schemas
  try:
    schemas = loadStorageSchemas()
  except:
    log.msg("Failed to reload storage schemas")
    log.err()


class WriterService(Service):
    def __init__(self):
        self.reload_task = LoopingCall(reloadStorageSchemas)

    def startService(self):
        self.reload_task.start(60, False)
        reactor.callInThread(writeForever)
        Service.startService(self)

    def stopService(self):
        self.reload_task.stop()
        Service.stopService(self)
