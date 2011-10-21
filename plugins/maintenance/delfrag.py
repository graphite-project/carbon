#!/usr/bin/python

import ceres
from os.path import getsize

sliceThreshold = int(params.pop('sliceThreshold'))
minDatapoints = int(params.pop('minDatapoints'))

def node_found(node):
  if len(list(node.slices)) > sliceThreshold:
    try:
      for slice in node.slices:
        datapoints = getsize(slice.fsPath)/ceres.DATAPOINT_SIZE
        if datapoints <= minDatapoints:
          os.unlink(slice.fsPath)
          log("DELFRAG: unlinked %s (# datapoint %s)" % (slice.fsPath, datapoints))
    except:
      log("%s failed to delfrag." % slice.fsPath)
      pass
