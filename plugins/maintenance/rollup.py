import time
from ceres import CeresSlice, SliceDeleted

#######################################################
# Put your custom aggregation logic in this function! #
#######################################################
def aggregate(node, datapoints):
  "Put your custom aggregation logic here."
  values = [value for (timestamp,value) in datapoints]
  metadata = node.readMetadata()
  method = metadata.get('aggregationMethod', 'avg')

  if method in ('avg', 'average'):
    return float(sum(values)) / len(values) # values is guaranteed to be nonempty

  elif method == 'sum':
    return sum(values)

  elif method == 'min':
    return min(values)

  elif method == 'max':
    return max(values)

  elif method == 'median':
    values.sort()
    return values[ len(values) / 2 ]


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
    fineStep = fineArchive['precision']
    coarseStep = coarseArchive['precision']
    deletePriorTo = coarseArchive['startTime'] + (coarseStep * coarseArchive['retention'])

    # We define a window corresponding to exactly one coarse datapoint
    # Then we use it to select datapoints for aggregation
    for i in range(coarseArchive['retention']):
      windowStart = coarseArchive['startTime'] + (i * coarseStep)
      windowEnd = windowStart + coarseStep
      fineDatapoints = [d for d in overflowDatapoints if d[0] >= windowStart and d[0] < windowEnd]

      if fineDatapoints:
        coarseValue = aggregate(node, fineDatapoints)
        coarseDatapoint = (windowStart, coarseValue)
        fineValues = [d[1] for d in fineDatapoints]

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
