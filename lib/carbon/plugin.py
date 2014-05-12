from carbon.conf import settings
from carbon.hbase.hbasedb import HbaseTSDB
from carbon.whispertsdb import WhisperTSDB
import os
from os.path import join

import sys
sys.path.append('/usr/local/rnt/webapp')

from graphite import local_settings

CONF_DIR = local_settings.CONF_DIR

def HbaseDB():
    if not settings.has_key('THRIFT_HOST'):
        settings.readFrom(join(CONF_DIR, 'graphite-db.conf'), 'HbaseDB')
    return HbaseTSDB(settings.THRIFT_HOST,settings.THRIFT_PORT,settings.GRAPHITE_PREFIX)

def WhisperDB():
    if not settings.has_key('WHISPER_STORAGE_DIR'):
        settings.readFrom(join(CONF_DIR, 'graphite-db.conf'), 'WhisperDB')
    return WhisperTSDB(settings.WHISPER_STORAGE_DIR)
