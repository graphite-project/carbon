import ceres
import time
import os
from os.path import getsize, basename


# Read in the required params and set them as global variables
namespace = globals()
for p in ('maxSlicesPerNode', 'maxSliceGap', 'mode'):
  if p not in params:
    raise MissingRequiredParam(p)
  value = str(params.pop(p))
  if value.isdigit():
    value = int(value)
  namespace[p] = value

if mode not in ('analyze', 'operate'):
  raise ValueError("Invalid mode '%s' must be either 'analyze' or 'operate'" % mode)
operating = mode == 'operate'

# tracking stuff for analze mode
operation_log = []
gaps_found = 0
gaps_filled_in = 0
bytes_filled_in = 0
excess_deletes = 0


def node_found(node):
  global gaps_found
  global gaps_filled_in
  global bytes_filled_in
  global excess_deletes
  node.readMetadata()

  # First we delete excess slices
  slices = list(node.slices) # order is most recent first
  excess_slices = slices[maxSlicesPerNode:]
  if excess_slices:
    excess_deletes += 1
    operation_log.append('%s: delete %d excess slices losing data prior to %s' %
                         (node.nodePath, len(excess_slices), time.ctime(excess_slices[0].endTime)))
    if operating:
      for slice in excess_slices:
        log('deleting excess slice: %s' % slice.fsPath)
        os.unlink(slice.fsPath)

  # Now we fill in sufficiently small gaps
  # Need to take a fresh look at slices because we may have just deleted some
  node.clearSliceCache()
  slices = list(node.slices)
  slices.reverse() # this time we iterate from oldest to newest
  for i in range(1, len(slices)): # iterate adjacent pairs
    slice_a = slices[i-1]
    slice_b = slices[i]
    if slice_a is None: # avoid considering slices we've deleted
      continue

    gap = (slice_b.startTime - slice_a.endTime) / node.timeStep
    if gap:
      gaps_found += 1
      gap_bytes = gap * ceres.DATAPOINT_SIZE
      if gap > maxSliceGap:
        gaps_filled_in += 1
        log('found %d data gap following %s' % (gap, slice_a.fsPath))
        operation_log.append('%s: fill %d datapoint gap and merge %s and %s' %
                             (node.nodePath, gap, basename(slice_a.fsPath), basename(slice_b.fsPath)))

        if operating:
          with file(slice_b.fsPath, 'rb') as fh:
            b_data = fh.read()
          log('read %d bytes from next slice %s' % (len(b_data), slice_b.fsPath))

          with file(slice_a.fsPath, 'ab') as fh:
            fh.seek(0, 2) # seek to the end
            fh.write(gap * ceres.PACKED_NAN)
            fh.write(b_data)

          log('data merged into prior slice, deleting %s' % slice_b.fsPath)
          os.unlink(slice_b.fsPath)

        # We indicate in the slices list that this slice has been deleted by setting it to None
        # Need to do it regardless of whether or not we're in operate mode so analyze is accurate
        slices[i] = None


def maintenance_start(tree):
  global start_time
  start_time = time.time()


def maintenance_complete(tree):
  run_time = time.time() - start_time
  if operating:
    log("Operate mode: Finished performing defrag operations")
  else:
    log("Analysis mode: No slice files were harmed in the making of this report.")

  log("--------- Operation Log ---------")
  for op in operation_log:
    log(op)

  log("------------ Summary ------------")
  log("     Gaps found: %d" % gaps_found)
  log(" Gaps filled in: %d" % gaps_filled_in)
  log("Bytes filled in: %d" % bytes_filled_in)
  log("Excess Slices Deleted: %d" % excess_deletes)
