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

from os.path import exists, dirname, join, sep
from carbon.util import PluginRegistrar, TaggedSeries
from carbon import log
from six import with_metaclass


class TimeSeriesDatabase(with_metaclass(PluginRegistrar, object)):
  "Abstract base class for Carbon database backends."
  plugins = {}

  "List of supported aggregation methods for the database."
  aggregationMethods = []

  def __init__(self, settings):
    self.graphite_url = settings.GRAPHITE_URL

  def write(self, metric, datapoints):
    "Persist datapoints in the database for metric."
    raise NotImplementedError()

  def exists(self, metric):
    "Return True if the given metric path exists, False otherwise."
    raise NotImplementedError()

  def create(self, metric, retentions, xfilesfactor, aggregation_method):
    "Create an entry in the database for metric using options."
    raise NotImplementedError()

  def getMetadata(self, metric, key):
    "Lookup metric metadata."
    raise NotImplementedError()

  def setMetadata(self, metric, key, value):
    "Modify metric metadata."
    raise NotImplementedError()

  def getFilesystemPath(self, metric):
    "Return filesystem path for metric, defaults to None."
    pass

  def validateArchiveList(self, archiveList):
    "Validate that the database can handle the given archiveList."
    pass

  def tag(self, *metrics):
    from carbon.http import httpRequest

    log.debug("Tagging %s" % ', '.join(metrics), type='tagdb')
    t = time.time()

    try:
      httpRequest(
        self.graphite_url + '/tags/tagMultiSeries',
        [('path', metric) for metric in metrics]
      )
      log.debug("Tagged %s in %s" % (', '.join(metrics), time.time() - t), type='tagdb')
    except Exception as err:
      log.msg("Error tagging %s: %s" % (', '.join(metrics), err), type='tagdb')


try:
  import whisper
except ImportError:
  pass
else:
  class WhisperDatabase(TimeSeriesDatabase):
    plugin_name = 'whisper'
    aggregationMethods = whisper.aggregationMethods

    def __init__(self, settings):
      super(WhisperDatabase, self).__init__(settings)

      self.data_dir = settings.LOCAL_DATA_DIR
      self.tag_hash_filenames = settings.TAG_HASH_FILENAMES
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
          log.err("WHISPER_FADVISE_RANDOM is enabled but skipped because it is not compatible " +
                  "with the version of Whisper.")

    def write(self, metric, datapoints):
      path = self.getFilesystemPath(metric)
      whisper.update_many(path, datapoints)

    def exists(self, metric):
      if exists(self.getFilesystemPath(metric)):
        return True
      # if we're using hashed filenames and a non-hashed file exists then move it to the new name
      if self.tag_hash_filenames and exists(self._getFilesystemPath(metric, False)):
        os.rename(self._getFilesystemPath(metric, False), self.getFilesystemPath(metric))
        return True
      return False

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
      path = self.getFilesystemPath(metric)
      directory = dirname(path)
      try:
        if not exists(directory):
          os.makedirs(directory)
      except OSError as e:
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
      return self._getFilesystemPath(metric, self.tag_hash_filenames)

    def _getFilesystemPath(self, metric, tag_hash_filenames):
      return join(
        self.data_dir,
        TaggedSeries.encode(metric, sep, hash_only=tag_hash_filenames) + '.wsp'
      )

    def validateArchiveList(self, archiveList):
      try:
        whisper.validateArchiveList(archiveList)
      except whisper.InvalidConfiguration as e:
        raise ValueError("%s" % e)


try:
  import ceres
except ImportError:
  pass
else:
  class CeresDatabase(TimeSeriesDatabase):
    plugin_name = 'ceres'
    aggregationMethods = ['average', 'sum', 'last', 'max', 'min']

    def __init__(self, settings):
      super(CeresDatabase, self).__init__(settings)

      self.data_dir = settings.LOCAL_DATA_DIR
      self.tag_hash_filenames = settings.TAG_HASH_FILENAMES
      ceres.setDefaultNodeCachingBehavior(settings.CERES_NODE_CACHING_BEHAVIOR)
      ceres.setDefaultSliceCachingBehavior(settings.CERES_SLICE_CACHING_BEHAVIOR)
      ceres.MAX_SLICE_GAP = int(settings.CERES_MAX_SLICE_GAP)

      if settings.CERES_LOCK_WRITES:
        if ceres.CAN_LOCK:
          log.msg("Enabling Ceres file locking")
          ceres.LOCK_WRITES = True
        else:
          log.err("CERES_LOCK_WRITES is enabled but import of fcntl module failed.")

      self.tree = ceres.CeresTree(self.data_dir)

    def encode(self, metric, tag_hash_filenames=None):
      if tag_hash_filenames is None:
        tag_hash_filenames = self.tag_hash_filenames
      return TaggedSeries.encode(metric, hash_only=tag_hash_filenames)

    def write(self, metric, datapoints):
      self.tree.store(self.encode(metric), datapoints)

    def exists(self, metric):
      if self.tree.hasNode(self.encode(metric)):
        return True
      # if we're using hashed filenames and a non-hashed file exists then move it to the new name
      if self.tag_hash_filenames and self.tree.hasNode(self.encode(metric, False)):
        os.rename(self._getFilesystemPath(metric, False), self.getFilesystemPath(metric))
        return True
      return False

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
      self.tree.createNode(self.encode(metric),
                           retentions=retentions,
                           timeStep=retentions[0][0],
                           xFilesFactor=xfilesfactor,
                           aggregationMethod=aggregation_method)

    def getMetadata(self, metric, key):
      return self.tree.getNode(self.encode(metric)).readMetadata()[key]

    def setMetadata(self, metric, key, value):
      node = self.tree.getNode(self.encode(metric))
      metadata = node.readMetadata()
      metadata[key] = value
      node.writeMetadata(metadata)

    def getFilesystemPath(self, metric):
      return self._getFilesystemPath(metric, self.tag_hash_filenames)

    def _getFilesystemPath(self, metric, tag_hash_filenames):
      return self.tree.getFilesystemPath(self.encode(metric, tag_hash_filenames))
