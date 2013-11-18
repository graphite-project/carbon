import whisper
import importlib
import os
import time
from os.path import join, dirname, exists, sep
from abc import ABCMeta,abstractmethod
from carbon.conf import settings
from carbon import log

# class DB is a generic DB layer to support graphite.  Plugins can provide an implementation satisfying the following functions
# by configuring DB_MODULE, DB_INIT_FUNC and DB_INIT_ARG

# the global variable APP_DB will be initialized as the return value of DB_MODULE.DB_INIT_FUNC(DB_INIT_ARG)
# we will throw an error if the provided value does not implement our abstract class DB below


class TSDB:
    __metaclass__= ABCMeta

    # returns info for the underlying db (including 'aggregationMethod')

    # info returned in the format
    #info = {
    #  'aggregationMethod' : aggregationTypeToMethod.get(aggregationType, 'average'),
    #  'maxRetention' : maxRetention,
    #  'xFilesFactor' : xff,
    #  'archives' : archives,
    #}
    # where archives is a list of
    # archiveInfo = {
    #  'offset' : offset,
    #  'secondsPerPoint' : secondsPerPoint,
    #  'points' : points,
    #  'retention' : secondsPerPoint    * points,
    #  'size' : points * pointSize,
    #}
    #
    @abstractmethod
    def info(self, metric):
        pass

    # aggregationMethod specifies the method to use when propogating data (see ``whisper.aggregationMethods``)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur.  If None, the existing xFilesFactor in path will not be changed
    @abstractmethod
    def setAggregationMethod(self, metric, aggregationMethod, xFilesFactor=None):
        pass

    # archiveList is a list of archives, each of which is of the form (secondsPerPoint,numberOfPoints)
    # xFilesFactor specifies the fraction of data points in a propagation interval that must have known values for a propagation to occur
    # aggregationMethod specifies the function to use when propogating data (see ``whisper.aggregationMethods``)
    @abstractmethod
    def create(self, metric, archiveConfig, xFilesFactor, aggregationMethod, isSparse, doFallocate):
        pass


    # datapoints is a list of (timestamp,value) points
    @abstractmethod
    def update_many(self, metric, datapoints):
        pass

    @abstractmethod
    def exists(self,metric):
        pass


    # fromTime is an epoch time
    # untilTime is also an epoch time, but defaults to now.
    #
    # Returns a tuple of (timeInfo, valueList)
    # where timeInfo is itself a tuple of (fromTime, untilTime, step)
    # Returns None if no data can be returned
    @abstractmethod
    def fetch(self,metric,startTime,endTime):
        pass

    # returns [ start, end ] where start,end are unixtime ints
    @abstractmethod
    def get_intervals(self,metric):
        pass

def getFilesystemPath(metric):
  metric_path = metric.replace('.',sep).lstrip(sep) + '.wsp'
  return join(settings.LOCAL_DATA_DIR, metric_path)

class WhisperDB:
    def info(self,metric):
        return whisper.info(getFilesystemPath(metric))

    def setAggregationMethod(self,metric, aggregationMethod, xFilesFactor=None):
        return whisper.setAggregationMethod(getFilesystemPath(metric),aggregationMethod,xFilesFactor)

    def create(self,metric,archiveConfig,xFilesFactor=None,aggregationMethod=None,sparse=False,useFallocate=False):
        dbFilePath = getFilesystemPath(metric)
        dbDir = dirname(dbFilePath)
        try:
            os.makedirs(dbDir, 0755)
        except OSError as e:
            log.err("%s" % e)
        log.creates("creating database file %s (archive=%s xff=%s agg=%s)" %
                    (dbFilePath, archiveConfig, xFilesFactor, aggregationMethod))
        return whisper.create(dbFilePath, archiveConfig,xFilesFactor,aggregationMethod,sparse,useFallocate)

    def update_many(self,metric,datapoints):
        return whisper.update_many(getFilesystemPath(metric), datapoints)

    def exists(self,metric):
        return exists(getFilesystemPath(metric))

    def fetch(self,metric,startTime,endTime):
        return whisper.fetch(getFilesystemPath(metric),startTime,endTime)

    def get_intervals(self,metric):
        filePath = getFilesystemPath(metric)
        start = time.time() - whisper.info(filePath)['maxRetention']
        end = max( os.stat(filePath).st_mtime, start )
        return [start,end]


# application database
APP_DB = WhisperDB() # default implementation

# if we've configured a module to override, put that one in place instead of the default whisper db
if (settings.DB_MODULE != "whisper" and settings.DB_INIT_FUNC != ""):
    m = importlib.import_module(settings.DB_MODULE)
    dbInitFunc = getattr(m,settings.DB_INIT_FUNC)
    APP_DB = dbInitFunc(settings.DB_INIT_ARG)
    assert isinstance(APP_DB,TSDB)