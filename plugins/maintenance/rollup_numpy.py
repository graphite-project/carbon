import time
import numpy as np
from ceres import CeresSlice, SliceDeleted

#######################################################
# Put your custom aggregation logic in this function! #
#######################################################
def aggregate(node, datapoints):
  "Put your custom aggregation logic here."
  #values = [value for (timestamp,value) in datapoints if value is not None]
  #datapoints = np.ma.masked_array(datapoints,np.isnan(datapoints))
  #mm = np.mean(mdat,axis=1)
  metadata = node.readMetadata()
  method = metadata.get('aggregationMethod', 'avg')

  if method in ('avg', 'average'):
     return np.nanmean(datapoints[:,1])

  elif method == 'sum':
    return np.sum(datapoints[:,1])

  elif method == 'min':
    return np.min(datapoints[:,1])

  elif method == 'max':
    return np.max(datapoints[:,1])

  elif method == 'median':
    return np.median(datapoints[:,1])


def node_found(node):
  archives = []
  t = int( time.time() )
  metadata = node.readMetadata()

  for (precision, retention) in metadata['retentions']:
    archiveEnd =  t - (t % precision)
    archiveStart = archiveEnd - (precision * retention)
    t = archiveStart
    archives.append({
      'precision' : precision,
      'retention' : retention,
      'startTime' : archiveStart,
      'endTime' : archiveEnd,
      'slices' : [s for s in node.slices if s.timeStep == precision]
    })

  for i, archive in enumerate(archives):
    if i == len(archives) - 1:
      do_rollup(node, archive, None)
    else:
      do_rollup(node, archive, archives[i+1])



def do_rollup(node, fineArchive, coarseArchive):
  overflowSlices = [s for s in fineArchive['slices'] if s.startTime < fineArchive['startTime']]
  if not overflowSlices:
    return

  if coarseArchive is None: # delete the old datapoints
    for slice in overflowSlices:
      try:
        slice.deleteBefore(fineArchive['startTime'])
      except SliceDeleted:
        pass

  else:
    overflowDatapoints = []
    for slice in overflowSlices:
      datapoints = slice.read(slice.startTime, fineArchive['startTime'])
      overflowDatapoints.extend( list(datapoints) )
    
    overflowDatapoints.sort()
    #print overflowDatapoints
    np_overflowDatapoints = np.array(overflowDatapoints, dtype=np.float64)
    #print np_overflowDatapoints
    #print np_overflowDatapoints[:,1]
    #print np.average(np_overflowDatapoints[:,1])
    fineStep = fineArchive['precision']
    coarseStep = coarseArchive['precision']
    deletePriorTo = coarseArchive['startTime'] + (coarseStep * coarseArchive['retention'])
    
    metadata = node.readMetadata()
    xff = metadata.get('xFilesFactor')

    # We define a window corresponding to exactly one coarse datapoint
    # Then we use it to select datapoints for aggregation
    for i in range(coarseArchive['retention']):
      windowStart = coarseArchive['startTime'] + (i * coarseStep)
      windowEnd = windowStart + coarseStep
      np_fineDatapoints = np_overflowDatapoints[(np_overflowDatapoints[:,0]>=windowStart)&(np_overflowDatapoints[:,0] < windowEnd)]

      if np_fineDatapoints.size>0:
        np_knownValues = np.array([value for (timestamp,value) in np_fineDatapoints if value is not None])
        if np_knownValues.size==0:
          continue
        knownPercent = float(len(np_knownValues)) / len(np_fineDatapoints)
        if knownPercent < xff:  # we don't have enough data to aggregate!
          continue
          
        coarseValue = aggregate(node, np_fineDatapoints)

        coarseDatapoint = (windowStart, coarseValue)

        np_fineValues = np_fineDatapoints[:,1]  #[d[1] for d in fineDatapoints]

        written = False
        for slice in coarseArchive['slices']:
          if slice.startTime <= windowStart and slice.endTime >= windowStart:
            slice.write([coarseDatapoint])
            written = True
            break

          # We could pre-pend to an adjacent slice starting after windowStart
          # but that would be much more expensive in terms of I/O operations.
          # In the common case, append-only is best.

        if not written:
          newSlice = CeresSlice.create(node, windowStart, coarseStep)
          newSlice.write([coarseDatapoint])
          coarseArchive['slices'].append(newSlice)
          deletePriorTo = min(deletePriorTo, windowStart)

    # Delete the overflow from the fine archive
    for slice in overflowSlices:
      try:
        slice.deleteBefore( deletePriorTo ) # start of most recent coarse datapoint
      except SliceDeleted:
        pass



