from carbon.conf import settings
from carbon.whispertsdb import WhisperTSDB
from carbon.hbasedb import HbaseTSDB
from os.path import join as os_join

CONF_DIR = settings.CONF_DIR

def HbaseDB():
    settings.readFrom(os_join(CONF_DIR, 'graphite-db.conf'), 'HbaseDB')
    hostl = [host for host in settings.THRIFT_HOST_LIST if host]
    return HbaseTSDB(host_list=hostl,
                     port=settings.THRIFT_PORT,
                     table_prefix=settings.GRAPHITE_PREFIX,
                     batch_size=settings.HBASE_BATCH_SIZE,
                     transport=settings.THRIFT_TRANSPORT_TYPE,
                     send_interval=settings.CARBON_METRIC_INTERVAL,
                     reset_interval=settings.HBASE_RESET_INTERVAL,
                     protocol=settings.THRIFT_PROTOCOL)

def WhisperDB():
    if not settings.has_key('WHISPER_STORAGE_DIR'):
        settings.readFrom(join(CONF_DIR, 'graphite-db.conf'), 'WhisperDB')
    return WhisperTSDB(settings.WHISPER_STORAGE_DIR)
