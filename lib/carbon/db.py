import importlib
import whisper
from os.path import sep, dirname, join, exists
from os import makedirs
from carbon.conf import settings
from carbon import log

class WhisperDB(object):
  """
  WhisperDB is the default Whisper database implementation for the 
  pluggable storage system.
  """
  __slots__ = ('dataDir',)

  def __init__(self, dataDir):
    self.dataDir = dataDir

  def __getFilesystemPath(self, metric):
    metric_path = metric.replace('.', sep).lstrip(sep) + '.wsp'
    return join(self.dataDir, metric_path)

  def info(self, metric):
    return whisper.info(self.__getFilesystemPath(metric))

  def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
    return whisper.setAggregationMethod(self.__getFilesystemPath(metric), aggregationMethod, xFilesFactor)

  def create(self, metric, retention_config, xFilesFactor=None, aggregationMethod=None, sparse=False, useFallocate=False):
    dbFilePath = self.__getFilesystemPath(metric)
    dbDir = dirname(dbFilePath)

    try:
      if not (exists(dbDir)):
        makedirs(dbDir, 0755)
    except Exception as e:
      log.creates("Error creating dir " + dbDir + " : " + e)
    return whisper.create(dbFilePath, retention_config, xFilesFactor, aggregationMethod, sparse, useFallocate)

  def update_many(self, metric, datapoints, retention_config):
    ''' We pass the retention_config so that the API supports it. Whisper silently ignores it. '''
    return whisper.update_many(self.__getFilesystemPath(metric), datapoints)

  def exists(self, metric):
    return exists(self.__getFilesystemPath(metric)), self.__getFilesystemPath(metric)

def NewWhisperDB():
  return WhisperDB(settings.LOCAL_DATA_DIR)

def get_db(initFunc):
  module_name, class_name = initFunc.rsplit('.', 1)
  module = importlib.import_module(module_name)
  return getattr(module, class_name)()

# application database

APP_DB = get_db(settings.DB_INIT_FUNC)
