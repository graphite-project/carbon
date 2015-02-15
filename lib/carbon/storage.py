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

import os, re
from twisted.internet.task import LoopingCall
import whisper

from os.path import join, exists, sep
from carbon.conf import OrderedConfigParser, settings
from carbon.exceptions import CarbonConfigException
from carbon.util import pickle
from carbon import log


STORAGE_SCHEMAS_CONFIG = join(settings.CONF_DIR, 'storage-schemas.conf')
STORAGE_AGGREGATION_CONFIG = join(settings.CONF_DIR, 'storage-aggregation.conf')
STORAGE_LISTS_DIR = join(settings.CONF_DIR, 'lists')

HOT_STORAGE_SCHEMAS_CONFIG = join(settings.CONF_DIR, 'hot-storage-schemas.conf')
HOT_STORAGE_AGGREGATION_CONFIG = join(settings.CONF_DIR, 'hot-storage-aggregation.conf')


class Schema:
  def __init__(self):
      pass

  def test(self, metric):
    raise NotImplementedError()

  def matches(self, metric):
    return bool( self.test(metric) )


class DefaultSchema(Schema):

  def __init__(self, name, archives):
    self.name = name
    self.archives = archives

  def test(self, metric):
    return True


class PatternSchema(Schema):

  def __init__(self, name, pattern, archives):
    self.name = name
    self.pattern = pattern
    self.regex = re.compile(pattern)
    self.archives = archives

  def test(self, metric):
    return self.regex.search(metric)


class ListSchema(Schema):

  def __init__(self, name, listName, archives):
    self.name = name
    self.listName = listName
    self.archives = archives
    self.path = join(settings.WHITELISTS_DIR, listName)

    if exists(self.path):
      self.mtime = os.stat(self.path).st_mtime
      fh = open(self.path, 'rb')
      self.members = pickle.load(fh)
      fh.close()

    else:
      self.mtime = 0
      self.members = frozenset()

  def test(self, metric):
    if exists(self.path):
      current_mtime = os.stat(self.path).st_mtime

      if current_mtime > self.mtime:
        self.mtime = current_mtime
        fh = open(self.path, 'rb')
        self.members = pickle.load(fh)
        fh.close()

    return metric in self.members


class Archive:

  def __init__(self,secondsPerPoint,points):
    self.secondsPerPoint = int(secondsPerPoint)
    self.points = int(points)

  def __str__(self):
    return "Archive = (Seconds per point: %d, Datapoints to save: %d)" % (self.secondsPerPoint, self.points) 

  def getTuple(self):
    return (self.secondsPerPoint,self.points)

  @staticmethod
  def fromString(retentionDef):
    (secondsPerPoint, points) = whisper.parseRetentionDef(retentionDef)
    return Archive(secondsPerPoint, points)


class Storage:
    def __init__(self, storage_data_dir, storage_schemas_config, storage_aggregation_config, storage_list_dirs):
        self.storage_data_dir = storage_data_dir
        self.storage_schemas_config = storage_schemas_config
        self.storage_aggregation_config = storage_aggregation_config
        self.storage_list_dirs = storage_list_dirs
        self.storage_schemas = Storage.__loadStorageSchemas(self.storage_schemas_config)
        self.aggregation_schemas = Storage.__loadAggregationSchemas(self.storage_aggregation_config)

        self.storage_reload_task = LoopingCall(self.reloadStorageSchemas)
        self.aggregation_reload_task = LoopingCall(self.reloadAggregationSchemas)

    def getFilesystemPath(self, metric):
        metric_path = metric.replace('.',sep).lstrip(sep) + '.wsp'
        return join(self.storage_data_dir, metric_path)

    def reloadStorageSchemas(self):
        try:
            self.storage_schemas = Storage.__loadStorageSchemas(self.storage_schemas_config)
        except Exception:
            log.msg("Failed to reload storage SCHEMAS")
            log.err()


    def reloadAggregationSchemas(self):
        try:
            self.aggregation_schemas = Storage.__loadAggregationSchemas(self.storage_aggregation_config)
        except Exception:
            log.msg("Failed to reload aggregation SCHEMAS")
            log.err()

    def startRefreshes(self):
        self.storage_reload_task.start(60, False)
        self.aggregation_reload_task.start(60, False)

    def stopRefreshes(self):
        self.storage_reload_task.stop()
        self.aggregation_reload_task.stop()

    @staticmethod
    def __loadStorageSchemas(storage_schemas_config):
        schemaList = []
        config = OrderedConfigParser()
        config.read(storage_schemas_config)

        for section in config.sections():
            options = dict( config.items(section) )
            matchAll = options.get('match-all')
            pattern = options.get('pattern')
            listName = options.get('list')

            retentions = options['retentions'].split(',')
            archives = [ Archive.fromString(s) for s in retentions ]

            mySchema = None
            if matchAll:
                mySchema = DefaultSchema(section, archives)

            elif pattern:
                mySchema = PatternSchema(section, pattern, archives)

            elif listName:
                mySchema = ListSchema(section, listName, archives)

            archiveList = [a.getTuple() for a in archives]

            try:
                whisper.validateArchiveList(archiveList)
                if mySchema:
                    schemaList.append(mySchema)

            except whisper.InvalidConfiguration, e:
                log.msg("Invalid schemas found in %s: %s" % (section, e) )

        schemaList.append(defaultSchema)
        return schemaList


    @staticmethod
    def __loadAggregationSchemas(storage_aggregation_config):
        # NOTE: This abuses the Schema classes above, and should probably be refactored.
        schemaList = []
        config = OrderedConfigParser()

        try:
            config.read(storage_aggregation_config)
        except (IOError, CarbonConfigException):
            log.msg("%s not found or wrong perms, ignoring." % storage_aggregation_config)

        for section in config.sections():
            options = dict( config.items(section) )
            matchAll = options.get('match-all')
            pattern = options.get('pattern')
            listName = options.get('list')

            xFilesFactor = options.get('xfilesfactor')
            aggregationMethod = options.get('aggregationmethod')

            try:
                if xFilesFactor is not None:
                    xFilesFactor = float(xFilesFactor)
                    assert 0 <= xFilesFactor <= 1
                if aggregationMethod is not None:
                    assert aggregationMethod in whisper.aggregationMethods
            except ValueError:
                log.msg("Invalid schemas found in %s." % section)
                continue

            archives = (xFilesFactor, aggregationMethod)

            mySchema = None
            if matchAll:
                mySchema = DefaultSchema(section, archives)

            elif pattern:
                mySchema = PatternSchema(section, pattern, archives)

            elif listName:
                mySchema = ListSchema(section, listName, archives)

            if mySchema:
                schemaList.append(mySchema)

        schemaList.append(defaultAggregation)
        return schemaList

defaultArchive = Archive(60, 60 * 24 * 7) #default retention for unclassified data (7 days of minutely data)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))

storage = Storage(settings.LOCAL_DATA_DIR, STORAGE_SCHEMAS_CONFIG, STORAGE_AGGREGATION_CONFIG, STORAGE_LISTS_DIR)
hot_storage = Storage(settings.HOT_DATA_DIR, HOT_STORAGE_SCHEMAS_CONFIG, HOT_STORAGE_AGGREGATION_CONFIG, STORAGE_LISTS_DIR) \
    if settings.USE_HOT_STORAGE else None