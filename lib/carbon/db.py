import importlib
import whisper
from os.path import sep,dirname,join,exists
from os import makedirs
from carbon.conf import settings



# default implementation
class WhisperDB():
    __slots__ = ('dataDir')

    def __init__(self, dataDir):
        self.dataDir = dataDir


    # private method
    def getFilesystemPath(self, metric):
        metric_path = metric.replace('.', sep).lstrip(sep) + '.wsp'
        return join(self.dataDir, metric_path)

    # public API
    def info(self, metric):
        return whisper.info(self.getFilesystemPath(metric))

    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        return whisper.setAggregationMethod(self.getFilesystemPath(metric), aggregationMethod, xFilesFactor)

    def create(self, metric, archiveConfig, xFilesFactor=None, aggregationMethod=None, sparse=False,
               useFallocate=False):
        dbFilePath = self.getFilesystemPath(metric)
        dbDir = dirname(dbFilePath)

        try:
            if not (exists(dbDir)):
                makedirs(dbDir, 0755)
        except Exception as e:
            print("Error creating dir " + dbDir + " : " + e)
        return whisper.create(dbFilePath, archiveConfig, xFilesFactor, aggregationMethod, sparse, useFallocate)

    def update_many(self, metric, datapoints):
        return whisper.update_many(self.getFilesystemPath(metric), datapoints)

    def exists(self, metric):
        return exists(self.getFilesystemPath(metric))

def NewWhisperDB():
    return WhisperDB(settings.LOCAL_DATA_DIR)


def get_db(initFunc):
  module_name, class_name = initFunc.rsplit('.', 1)
  module = importlib.import_module(module_name)
  return getattr(module, class_name)()

# application database

APP_DB = get_db(settings.DB_INIT_FUNC)
