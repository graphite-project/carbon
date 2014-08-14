import os
import traceback
import whisper
from carbon import log
from carbon.storage import getFilesystemPath



def getMetadata(metric, key):
  if key != 'aggregationMethod':
    return dict(error="Unsupported metadata key \"%s\"" % key)

  wsp_path = getFilesystemPath(metric)
  try:
    value = whisper.info(wsp_path)['aggregationMethod']
    return dict(value=value)
  except:
    log.err()
    return dict(error=traceback.format_exc())


def setMetadata(metric, key, value):
  if key != 'aggregationMethod':
    return dict(error="Unsupported metadata key \"%s\"" % key)

  wsp_path = getFilesystemPath(metric)
  try:
    old_value = whisper.setAggregationMethod(wsp_path, value)
    return dict(old_value=old_value, new_value=value)
  except:
    log.err()
    return dict(error=traceback.format_exc())

class WhisperCmd(object):

  @classmethod
  def fetch(cls, path, start, end):
    wsp_path = getFilesystemPath(path)
    return whisper.fetch(wsp_path, start, end)

  @classmethod
  def create(cls, path, archiveList, xFilesFactor=None, aggregationMethod=None):
    wsp_path = getFilesystemPath(path)
    return whisper.create(wsp_path, archiveList, xFilesFactor, aggregationMethod)

  @classmethod
  def delete(cls, path):
    wsp_path = getFilesystemPath(path)
    return os.removedirs(wsp_path)

  @classmethod
  def info(cls, path):
    wsp_path = getFilesystemPath(path)
    return whisper.info(wsp_path)

