import traceback
from carbon import log, state


def getMetadata(metric, key):
  try:
    value = state.database.getMetadata(metric, key)
    return dict(value=value)
  except Exception:
    log.err()
    return dict(error=traceback.format_exc())


def setMetadata(metric, key, value):
  try:
    old_value = state.database.setMetadata(metric, key, value)
    return dict(old_value=old_value, new_value=value)
  except Exception:
    log.err()
    return dict(error=traceback.format_exc())
