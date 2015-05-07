import traceback
import whisper
from carbon import log
from carbon.storage import getFilesystemPath
from carbon.db import APP_DB



def getMetadata(metric, key):
  if key != 'aggregationMethod':
    return dict(error="Unsupported metadata key \"%s\"" % key)

  wsp_path = getFilesystemPath(metric)
  try:
    value = APP_DB.info(metric)['aggregationMethod']
    return dict(value=value)
  except:
    log.err()
    return dict(error=traceback.format_exc())


def setMetadata(metric, key, value):
  if key != 'aggregationMethod':
    return dict(error="Unsupported metadata key \"%s\"" % key)

  wsp_path = getFilesystemPath(metric)
  try:
    old_value = APP_DB.setAggregationMethod(metric, value)
    return dict(old_value=old_value, new_value=value)
  except:
    log.err()
    return dict(error=traceback.format_exc())
