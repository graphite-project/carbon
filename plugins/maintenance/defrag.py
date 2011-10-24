import ceres
from time import ctime
from os.path import getsize


# Read in the required params and set them as global variables
namespace = globals()
for p in ('maxSlicesPerNode', 'maxSliceGap', 'mode'):
  if p not in params:
    raise MissingRequiredParam(p)
  value = params.pop(p)
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


def node_found(node):
  global gaps_found
  global gaps_filled_in
  global bytes_filled_in

  # Delete excess slices
  slices = list(node.slices) # order is most recent first

  # Delete excess slices
  excess_slices = slices[maxSlicesPerNode:]
  if excess_slices:
    operation_log.append('%s: delete %d excess slices losing data prior to %s' %
                         (node.nodePath, len(excess_slices), ctime(excess_slices[0].endTime)))
    if operating:
      for slice in excess_slices:
        log('deleting excess slice: %s' % slice.fsPath)
        os.unlink(slice.fsPath)

  # Now we fill in the gaps
  slices.reverse() # now we iterate from oldest to newest
  for i in range(1, len(slices)):
    slice_a = slices[i-1]
    slice_b = slices[i]
    gap = (slice_b.startTime - slice_a.endTime) / node.timeStep
    if gap:
      gaps_found += 1
      gap_bytes = gap * DATAPOINT_SIZE
      if gap > maxSliceGap:
        gaps_filled_in += 1
        padding = nan
        with file(slice_a.fsPath, 'ab') as fh:
          fh.seek(0, 2) # seek to the end
          fh.write(gap * ceres.PACKED_NAN)

        b_data = slice_b.read_all() #XXX
        slice_a.write(b_data) #XXX
        os.unlink(slice_b.fsPath)
        del slices[i] #XXX


def maintenance_complete(tree):
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

'''
maxSlicesPerNode - we'll just delete anything older than these blindly
maxSliceGap - when we find two slices ...
mode - analyze or operate

operate mode will
  first delete old slices
  then make a list of all the gaps
  then filter out the ones larger than maxSliceGap
  merging the two slice files bounding each gap consists of
    append NANs to the prior slice until it fills the gap completely
    then append the entire contents of the latter slice to the prior slice

analyze mode will simply report a summary of
  what operations would've been performed
  how much gap (datapoints) got filled in
  how fewer slices there are now
  how many slices wouldve been deleted
  how bytes usage wouldve changed
  run time
'''
