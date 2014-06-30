from carbon.conf import settings
from carbon.whispertsdb import WhisperTSDB
import os
from os.path import join
try:
    from carbon.hbase.hbasedb import HbaseTSDB
except Exception as e:
    pass

import sys
sys.path.append('/opt/graphite/webapp')

from graphite import local_settings

CONF_DIR = local_settings.CONF_DIR

def HbaseDB():
    if not settings.has_key('THRIFT_HOST'):
        settings.readFrom(join(CONF_DIR, 'graphite-db.conf'), 'HbaseDB')
    return HbaseTSDB(settings.THRIFT_HOST,settings.THRIFT_PORT,settings.GRAPHITE_PREFIX, settings.HBASE_BATCH_SIZE)

def WhisperDB():
    if not settings.has_key('WHISPER_STORAGE_DIR'):
        settings.readFrom(join(CONF_DIR, 'graphite-db.conf'), 'WhisperDB')
    return WhisperTSDB(settings.WHISPER_STORAGE_DIR)
