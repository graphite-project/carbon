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
from os.path import exists, dirname, join, sep
from carbon.util import PluginRegistrar
from carbon import log


class TimeSeriesDatabase(object):
  "Abstract base class for Carbon database backends."
  __metaclass__ = PluginRegistrar
  plugins = {}

  def write(self, metric, datapoints):
    "Persist datapoints in the database for metric."
    raise NotImplemented()

  def exists(self, metric):
    "Return True if the given metric path exists, False otherwise."
    raise NotImplemented()

  def create(self, metric, retentions, xfilesfactor, aggregation_method):
    "Create an entry in the database for metric using options."
    raise NotImplemented()

  def getMetadata(self, metric, key):
    "Lookup metric metadata."
    raise NotImplemented()

  def setMetadata(self, metric, key, value):
    "Modify metric metadata."
    raise NotImplemented()

  def getFilesystemPath(self, metric):
    "Return filesystem path for metric, defaults to None."
    pass


try:
  import whisper
except ImportError:
  pass
else:
  class WhisperDatabase(TimeSeriesDatabase):
    plugin_name = 'whisper'

    def __init__(self, settings):
      self.data_dir = settings.LOCAL_DATA_DIR
      self.sparse_create = settings.WHISPER_SPARSE_CREATE
      self.fallocate_create = settings.WHISPER_FALLOCATE_CREATE
      if settings.WHISPER_AUTOFLUSH:
        log.msg("Enabling Whisper autoflush")
        whisper.AUTOFLUSH = True

      if settings.WHISPER_FALLOCATE_CREATE:
        if whisper.CAN_FALLOCATE:
          log.msg("Enabling Whisper fallocate support")
        else:
          log.err("WHISPER_FALLOCATE_CREATE is enabled but linking failed.")

      if settings.WHISPER_LOCK_WRITES:
        if whisper.CAN_LOCK:
          log.msg("Enabling Whisper file locking")
          whisper.LOCK = True
        else:
          log.err("WHISPER_LOCK_WRITES is enabled but import of fcntl module failed.")

      if settings.WHISPER_FADVISE_RANDOM:
        try:
          if whisper.CAN_FADVISE:
            log.msg("Enabling Whisper fadvise_random support")
            whisper.FADVISE_RANDOM = True
          else:
            log.err("WHISPER_FADVISE_RANDOM is enabled but import of ftools module failed.")
        except AttributeError:
          log.err("WHISPER_FADVISE_RANDOM is enabled but skipped because it is not compatible with the version of Whisper.")

    def write(self, metric, datapoints):
      path = self.getFilesystemPath(metric)
      whisper.update_many(path, datapoints)

    def exists(self, metric):
      return exists(self.getFilesystemPath(metric))

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
      path = self.getFilesystemPath(metric)
      directory = dirname(path)
      try:
        if not exists(directory):
          os.makedirs(directory)
      except OSError, e:
        log.err("%s" % e)

      whisper.create(path, retentions, xfilesfactor, aggregation_method,
                     self.sparse_create, self.fallocate_create)

    def getMetadata(self, metric, key):
      if key != 'aggregationMethod':
        raise ValueError("Unsupported metadata key \"%s\"" % key)

      wsp_path = self.getFilesystemPath(metric)
      return whisper.info(wsp_path)['aggregationMethod']

    def setMetadata(self, metric, key, value):
      if key != 'aggregationMethod':
        raise ValueError("Unsupported metadata key \"%s\"" % key)

      wsp_path = self.getFilesystemPath(metric)
      return whisper.setAggregationMethod(wsp_path, value)

    def getFilesystemPath(self, metric):
      metric_path = metric.replace('.', sep).lstrip(sep) + '.wsp'
      return join(self.data_dir, metric_path)
