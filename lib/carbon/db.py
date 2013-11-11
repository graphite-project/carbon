"""Copyright 2013 Jay Booth

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import whisper
import importlib
from abc import ABCMeta,abstractmethod
from carbon.conf import settings
from carbon.storage import getFilesystemPath


# management.py:    value = whisper.info(wsp_path)['aggregationMethod']
# management.py:    old_value = whisper.setAggregationMethod(wsp_path, value)
# writer.py:        whisper.create(dbFilePath, archiveConfig, xFilesFactor, aggregationMethod, settings.WHISPER_SPARSE_CREATE, settings.WHISPER_FALLOCATE_CREATE)
# writer.py:        whisper.update_many(dbFilePath, datapoints)


class DB:
    __metaclass__= ABCMeta

    # returns info for the underlying db (including 'aggregationMethod')
    @abstractmethod
    def info(self, metric):
        pass

    @abstractmethod
    def setAggregationMethod(self, metric, value):
        pass

    @abstractmethod
    def create(self, metric, archiveConfig, xFilesFactor, aggregationMethod, isSparse, doFallocate):
        pass

    @abstractmethod
    def update_many(self, metric, datapoints):
        pass

class WhisperDB:
    def info(self,metric):
        return whisper.info(getFilesystemPath(metric))

    def setAggregationMethod(self,metric,value):
        return whisper.setAggregationMethod(getFilesystemPath(metric),value)

    def create(self,metric,archiveConfig,xFilesFactor,aggregationMethod,sparseCreate,fallocateCreate):
        return whisper.create(getFilesystemPath(metric), archiveConfig,xFilesFactor,aggregationMethod,sparseCreate,fallocateCreate)

    def update_many(self,metric,datapoints):
        return whisper.update_many(getFilesystemPath(metric), datapoints)


def newWhisperDB(arg):
    return WhisperDB()

# application database
APP_DB = WhisperDB()

# if we've configured a module to override, put that one in place instead
if (settings.DB_MODULE != "whisper"):
    m = importlib.import_module(settings.DB_MODULE)
    dbInitFunc = getattr(m,settings.DB_INIT_FUNC)
    DB = dbInitFunc(settings.DB_INIT_ARG)

assert isinstance(APP_DB,DB)