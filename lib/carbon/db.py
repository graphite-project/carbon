import importlib
from carbon.conf import settings
from graphitedata.tsdb import TSDB
from graphitedata.whispertsdb import WhisperTSDB



# application database
APP_DB = WhisperTSDB(settings.LOCAL_DATA_DIR) # default implementation

# if we've configured a module to override, put that one in place instead of the default whisper db
if (settings.DB_MODULE != "whisper" and settings.DB_INIT_FUNC != ""):
    m = importlib.import_module(settings.DB_MODULE)
    dbInitFunc = getattr(m,settings.DB_INIT_FUNC)
    APP_DB = dbInitFunc(settings.DB_INIT_ARG)
    assert isinstance(APP_DB,TSDB)