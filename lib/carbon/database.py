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

from carbon.util import PluginRegistrar


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
