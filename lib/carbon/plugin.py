from carbon.conf import settings
from carbon.whispertsdb import WhisperTSDB
from carbon.hbasedb import HbaseTSDB

from os.path import join as os_join
from sys import path as sys_path
sys_path.append('/usr/local/rnt/webapp')

from graphite import local_settings

CONF_DIR = local_settings.CONF_DIR

def HbaseDB():
    settings.readFrom(os_join(CONF_DIR, 'graphite-db.conf'), 'HbaseDB')
    return HbaseTSDB(host=settings.THRIFT_HOST,
                     port=settings.THRIFT_PORT,
                     table_prefix=settings.GRAPHITE_PREFIX,
                     batch_size=settings.HBASE_BATCH_SIZE,
                     transport=settings.THRIFT_TRANSPORT_TYPE,
                     send_interval=settings.CARBON_METRIC_INTERVAL)

def WhisperDB():
    if not settings.has_key('WHISPER_STORAGE_DIR'):
        settings.readFrom(join(CONF_DIR, 'graphite-db.conf'), 'WhisperDB')
    return WhisperTSDB(settings.WHISPER_STORAGE_DIR)
