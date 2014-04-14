from conf import Settings
from . import GRAPHITE_STORAGE_DIR
from graphitedata.hbase.hbasedb import HbaseTSDB
from graphitedata.whispertsdb import WhisperTSDB
from os.path import join

hbaseDefaults = dict(
    THRIFT_HOST="localhost",
    THRIFT_PORT=9090,
    TABLE_PREFIX="graphite_",
)
def HbaseDB():
    cfg = Settings(hbaseDefaults,"hbase")
    return HbaseTSDB(cfg.THRIFT_HOST,cfg.THRIFT_PORT,cfg.TABLE_PREFIX)

whisperDefaults = dict(
    WHISPER_STORAGE_DIR=join(GRAPHITE_STORAGE_DIR,"whisper")
)
def WhisperDB():
    config = Settings(whisperDefaults,"whisper")
    return WhisperTSDB(config.WHISPER_STORAGE_DIR)
