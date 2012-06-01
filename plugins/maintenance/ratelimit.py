import time

if 'rate' not in params:
  raise MissingRequiredParam('rate')

nodes_per_second = int( params['rate'] )
node_count = 0
last_second = 0

def node_found(node):
  global node_count
  global last_second
  now = time.time()
  this_second = int(now)

  if this_second != last_second:
    node_count = 0

  last_second = this_second
  node_count += 1

  if node_count == nodes_per_second: 
    remaining = (this_second + 1) - now
    if remaining > 0:
      time.sleep(remaining)
