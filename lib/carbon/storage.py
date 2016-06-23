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
import whisper

from os.path import join, exists
from carbon.conf import OrderedConfigParser, settings
from carbon.exceptions import CarbonConfigException
from carbon.util import pickle
from carbon import log, state


STORAGE_SCHEMAS_CONFIG = join(settings.CONF_DIR, 'storage-schemas.conf')
STORAGE_AGGREGATION_CONFIG = join(settings.CONF_DIR, 'storage-aggregation.conf')
STORAGE_LISTS_DIR = join(settings.CONF_DIR, 'lists')

def getFilesystemPath(metric):
  return state.database.getFilesystemPath(metric)


class Schema:
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


def loadStorageSchemas():
  schemaList = []
  config = OrderedConfigParser()
  config.read(STORAGE_SCHEMAS_CONFIG)

  for section in config.sections():
    options = dict( config.items(section) )
    pattern = options.get('pattern')

    retentions = options['retentions'].split(',')
    archives = [ Archive.fromString(s) for s in retentions ]

    if pattern:
      mySchema = PatternSchema(section, pattern, archives)
    else:
      log.err("Section missing 'pattern': %s" % section)
      continue

    archiveList = [a.getTuple() for a in archives]

    try:
      whisper.validateArchiveList(archiveList)
      schemaList.append(mySchema)
    except whisper.InvalidConfiguration, e:
      log.msg("Invalid schemas found in %s: %s" % (section, e) )

  schemaList.append(defaultSchema)
  return schemaList


def loadAggregationSchemas():
  # NOTE: This abuses the Schema classes above, and should probably be refactored.
  schemaList = []
  config = OrderedConfigParser()

  try:
    config.read(STORAGE_AGGREGATION_CONFIG)
  except (IOError, CarbonConfigException):
    log.msg("%s not found or wrong perms, ignoring." % STORAGE_AGGREGATION_CONFIG)

  for section in config.sections():
    options = dict( config.items(section) )
    pattern = options.get('pattern')

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

    if pattern:
      mySchema = PatternSchema(section, pattern, archives)
    else:
      log.err("Section missing 'pattern': %s" % section)
      continue

    schemaList.append(mySchema)

  schemaList.append(defaultAggregation)
  return schemaList

defaultArchive = Archive(60, 60 * 24 * 7) #default retention for unclassified data (7 days of minutely data)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))
