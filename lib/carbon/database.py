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

from os.path import exists
from carbon.util import PluginRegistrar
from carbon.storage import getFilesystemPath
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


try:
  import whisper
except ImportError:
  pass
else:
  class WhisperDatabase(TimeSeriesDatabase):
    plugin_name = 'whisper'

    def __init__(self, settings):
      self.data_dir = settings.LOCAL_DATA_DIR
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

    def write(self, metric, datapoints):
      path = getFilesystemPath(metric)
      whisper.update_many(path, datapoints)

    def exists(self, metric):
      return exists(getFilesystemPath(metric))
