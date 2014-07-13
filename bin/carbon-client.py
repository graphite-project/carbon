#!/usr/bin/env python
"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import sys
import imp
from os.path import dirname, join, abspath, exists, split
from optparse import OptionParser
import os, time

# Figure out where we're installed
BIN_DIR = dirname(abspath(__file__))
ROOT_DIR = dirname(BIN_DIR)
CONF_DIR = join(ROOT_DIR, 'conf')
default_relayrules = join(CONF_DIR, 'relay-rules.conf')

# Make sure that carbon's 'lib' dir is in the $PYTHONPATH if we're running from
# source.
LIB_DIR = join(ROOT_DIR, 'lib')
sys.path.insert(0, LIB_DIR)

try:
  from twisted.internet import epollreactor
  epollreactor.install()
except ImportError:
  pass

from twisted.internet import stdio, reactor, defer
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import LineReceiver
from carbon.routers import ConsistentHashingRouter, RelayRulesRouter
from carbon.client import CarbonClientManager
from carbon.conf import settings
from carbon import log, events


option_parser = OptionParser(usage="%prog [options] <host:port:instance> <host:port:instance> ...")
option_parser.add_option('--debug', action='store_true', help="Log debug info to stdout")
option_parser.add_option('--keyfunc', help="Use a custom key function (path/to/module.py:myFunc)")
option_parser.add_option('--datadir', help="Directory containing files to load", default=None)
option_parser.add_option('--replication', type='int', default=1, help='Replication factor')
option_parser.add_option('--routing', default='consistent-hashing',
  help='Routing method: "consistent-hashing" (default) or "relay"')
option_parser.add_option('--relayrules', default=default_relayrules,
  help='relay-rules.conf file to use for relay routing')

options, args = option_parser.parse_args()

if not args:
  print 'At least one host:port destination required\n'
  option_parser.print_usage()
  raise SystemExit(1)

if options.routing not in ('consistent-hashing', 'relay'):
  print "Invalid --routing value, must be one of:"
  print "  consistent-hashing"
  print "  relay"
  raise SystemExit(1)

destinations = []
for arg in args:
  parts = arg.split(':', 2)
  host = parts[0]
  port = int(parts[1])
  if len(parts) > 2:
    instance = parts[2]
  else:
    instance = None
  destinations.append( (host, port, instance) )

if options.debug:
  log.logToStdout()
  log.setDebugEnabled(True)
  defer.setDebugging(True)

if options.routing == 'consistent-hashing':
  router = ConsistentHashingRouter(options.replication)
elif options.routing == 'relay':
  if exists(options.relayrules):
    config_dir, relayrules = split(abspath(options.relayrules))
    settings.use_config_directory(config_dir)
    router = RelayRulesRouter(relayrules)
  else:
    print "relay rules file %s does not exist" % options.relayrules
    raise SystemExit(1)

client_manager = CarbonClientManager(router)
reactor.callWhenRunning(client_manager.startService)

if options.keyfunc:
  router.setKeyFunctionFromModule(options.keyfunc)

firstConnectAttempts = [client_manager.startClient(dest) for dest in destinations]
firstConnectsAttempted = defer.DeferredList(firstConnectAttempts)


class StdinMetricsReader(LineReceiver):
  delimiter = '\n'

  def lineReceived(self, line):
    #log.clients("[DEBUG] lineReceived(): %s" % line)
    try:
      (metric, value, timestamp) = line.split()
      datapoint = (float(timestamp), float(value))
      assert datapoint[1] == datapoint[1] # filter out NaNs
      client_manager.sendDatapoint(metric, datapoint)
    except:
      log.err(None, 'Dropping invalid line: %s' % line)

  def connectionLost(self, reason):
    log.clients('stdin disconnected')
    def startShutdown(results):
      log.clients("startShutdown(%s)" % str(results))
      allStopped = client_manager.stopAllClients()
      allStopped.addCallback(shutdown)
    firstConnectsAttempted.addCallback(startShutdown)

  def startShutdown(results):
    log.clients("startShutdown(%s)" % str(results))
    allStopped = client_manager.stopAllClients()
    allStopped.addCallback(shutdown)
    firstConnectsAttempted.addCallback(startShutdown)


class FileLoader(object):
  def __init__(self, datadir):
    self.filelist = []
    self.datadir = datadir

  def getNextFile(self):
    if len(self.filelist) == 0:
      self.filelist = os.listdir(self.datadir)[-100:]
      if len(self.filelist) < 10:
        # relax if there is not that much data
        time.sleep(1)
    return os.path.join(self.datadir, self.filelist.pop())

  def loadMetricsFile(self):
    try:
      fname = self.getNextFile()
      if os.path.splitext(fname)[1] == '.log':
        log.clients("Feeding %s" % fname)
        f = open(fname)
        while True:
          line = f.readline()
          if not line:
            break
          try:
            (metric, value, timestamp) = line.strip().split()
            datapoint = (float(timestamp), float(value))
            client_manager.sendDatapoint(metric, datapoint)
          except:
            log.err(None, 'Dropping invalid line: %s' % line)
        f.close()
        try:
          os.remove(fname)
        except OSError:
          pass
    except IndexError:
      log.debug("No data, sleeping 1 sec")
      time.sleep(1)

if options.datadir is None:
  stdio.StandardIO( StdinMetricsReader() )
else:
  fl = FileLoader(options.datadir)
  lc = LoopingCall(fl.loadMetricsFile)
  lc.start(0)

exitCode = 0
def shutdown(results):
  global exitCode
  for success, result in results:
    if not success:
      exitCode = 1
      break
  if reactor.running:
    reactor.stop()

reactor.run()
raise SystemExit(exitCode)
