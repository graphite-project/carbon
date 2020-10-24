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

import re

from os.path import join
from carbon.conf import OrderedConfigParser, settings
from carbon.exceptions import CarbonConfigException
from carbon.util import parseRetentionDef
from carbon import log, state


STORAGE_SCHEMAS_CONFIG = join(settings.CONF_DIR, 'storage-schemas.conf')
STORAGE_AGGREGATION_CONFIG = join(settings.CONF_DIR, 'storage-aggregation.conf')
STORAGE_LISTS_DIR = join(settings.CONF_DIR, 'lists')


class Schema:
  def test(self, metric):
    raise NotImplementedError()

  def matches(self, metric):
    return bool(self.test(metric))


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

  def __init__(self, secondsPerPoint, points):
    self.secondsPerPoint = int(secondsPerPoint)
    self.points = int(points)

  def __str__(self):
    return "Archive = (Seconds per point: %d, Datapoints to save: %d)" % (
      self.secondsPerPoint, self.points)

  def getTuple(self):
    return (self.secondsPerPoint, self.points)

  @staticmethod
  def fromString(retentionDef):
    (secondsPerPoint, points) = parseRetentionDef(retentionDef)
    return Archive(secondsPerPoint, points)


def loadStorageSchemas():
  schemaList = []
  config = OrderedConfigParser()
  config.read(STORAGE_SCHEMAS_CONFIG)

  for section in config.sections():
    options = dict(config.items(section))
    pattern = options.get('pattern')

    try:
      retentions = options['retentions'].split(',')
    except KeyError:
      log.err("Schema %s missing 'retentions', skipping" % section)
      continue

    try:
      archives = [Archive.fromString(s) for s in retentions]
    except ValueError as exc:
      log.err("{msg} in section [{section}] in {fn}".format(
        msg=exc, section=section.title(), fn=STORAGE_SCHEMAS_CONFIG))
      raise SystemExit(1)

    if pattern:
      mySchema = PatternSchema(section, pattern, archives)
    else:
      log.err("Schema %s missing 'pattern', skipping" % section)
      continue

    archiveList = [a.getTuple() for a in archives]

    try:
      if state.database is not None:
        state.database.validateArchiveList(archiveList)
      schemaList.append(mySchema)
    except ValueError as e:
      log.msg("Invalid schemas found in %s: %s" % (section, e))

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
    options = dict(config.items(section))
    pattern = options.get('pattern')

    xFilesFactor = options.get('xfilesfactor')
    aggregationMethod = options.get('aggregationmethod')

    try:
      if xFilesFactor is not None:
        xFilesFactor = float(xFilesFactor)
        if not 0 <= xFilesFactor <= 1:
          raise AssertionError("xFilesFactor value out of [0,1] bounds")
      if aggregationMethod is not None:
        if state.database is not None:
          if aggregationMethod not in state.database.aggregationMethods:
            raise AssertionError("aggregationMethod not found in state.database.aggregationMethods")
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


# default retention for unclassified data (7 days of minutely data)
defaultArchive = Archive(60, 60 * 24 * 7)
defaultSchema = DefaultSchema('default', [defaultArchive])
defaultAggregation = DefaultSchema('default', (None, None))
