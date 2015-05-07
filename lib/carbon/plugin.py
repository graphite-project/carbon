from carbon.conf import settings
from carbon.whispertsdb import WhisperTSDB
from carbon.hbasedb import HbaseTSDB
from os.path import join as os_join

CONF_DIR = settings.CONF_DIR

def HbaseDB():
    settings.readFrom(os_join(CONF_DIR, 'graphite-db.conf'), 'HbaseDB')
    hostl = [host for host in settings.THRIFT_HOST_LIST if host]
    return HbaseTSDB(host_list=hostl,
                     thrift_host=settings.THRIFT_HOST,
                     port=int(settings.THRIFT_PORT),
                     table_prefix=settings.GRAPHITE_PREFIX,
                     batch_size=int(settings.HBASE_BATCH_SIZE),
                     transport=settings.THRIFT_TRANSPORT_TYPE,
                     send_interval=int(settings.CARBON_METRIC_INTERVAL),
                     reset_interval=int(settings.HBASE_RESET_INTERVAL),
                     protocol=settings.THRIFT_PROTOCOL,
                     single_host=bool(settings.USE_SINGLE_HOST))

def WhisperDB():
    if not settings.has_key('WHISPER_STORAGE_DIR'):
        settings.readFrom(join(CONF_DIR, 'graphite-db.conf'), 'WhisperDB')
    return WhisperTSDB(settings.WHISPER_STORAGE_DIR)
