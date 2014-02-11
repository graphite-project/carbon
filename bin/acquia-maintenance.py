#!/usr/bin/env python
"""
Runs maintenance tasks using the carbon_cassandra_plugin.

This file is a fork of `ceres-maintenace` which uses a combination of reading 
from disk, using a CeresTree and creating CeresNodes directly. It follows the 
same principles as `ceres-maintenace`.

**Notes:** 

1. Current way to run this script::

  export GRAPHITE_ROOT=/vagrant/src/carbon
  ./acquia-maintenance.py --configdir=/opt/graphite/conf/carbon-daemons/writer/ acquia_rollup
  
2. The bottom of this script contains a hard coded IP address and keyspace 
for cassandra. 
"""

import os
from optparse import OptionParser
import sys
import time
import traceback

from carbon_cassandra_plugin import carbon_cassandra_db

# Make carbon imports available for some functionality
root_dir = os.environ['GRAPHITE_ROOT'] = os.environ.get('GRAPHITE_ROOT', '/opt/graphite/')
lib_dir = os.path.join(root_dir, 'lib')
sys.path.append(lib_dir)

try:
  import carbon
except ImportError:
  print ("Failed to import carbon, specify your installation location "
         "with the GRAPHITE_ROOT environment variable.")
  sys.exit(1)


EVENTS = (
  'maintenance_start',
  'maintenance_complete',
  'node_found',
  'directory_found',
  'directory_empty',
)


class Plugin:
  context = {
    'params' : {}
  }

  def __init__(self, path):
    self.path = path
    self.name = os.path.basename( os.path.splitext(path)[0] )
    self.namespace = {}
    self.namespace.update(Plugin.context)
    self.event_handlers = {}

  def load(self):
    execfile(self.path, self.namespace)
    for event in EVENTS:
      if event in self.namespace:
        self.event_handlers[event] = self.namespace[event]

  def handle_event(self, event, *args, **kwargs):
    handler = self.event_handlers.get(event)
    if handler:
      handler(*args, **kwargs)


class PluginFinder:
  def __init__(self, plugin_dir):
    self.plugin_dir = os.path.abspath( os.path.expanduser(plugin_dir) )

  def find_plugins(self, plugin_refs):
    for ref in plugin_refs:
      if ref.startswith('~'):
        ref = os.path.expanduser(ref)

      if os.path.isfile(ref):
        yield Plugin(ref)

      else:
        filename = "%s.py" % ref
        plugin_path = os.path.join(self.plugin_dir, filename)

        if os.path.isfile(plugin_path):
          yield Plugin(plugin_path)
        else:
          raise PluginNotFound("The plugin '%s' could not be found in %s" % (ref, self.plugin_dir))


class PluginNotFound(Exception):
  pass


class EventDispatcher:
  def __init__(self):
    self.handlers = {}

  def add_handler(self, event, handler):
    if event not in self.handlers:
      self.handlers[event] = []
    self.handlers[event].append(handler)

  def dispatch(self, event, *args, **kwargs):
    for handler in self.handlers.get(event, []):
      try:
        handler(*args, **kwargs)
      except:
        log("--- Error in %s event-handler ---" % event)
        log( traceback.format_exc() )
        log('-' * 80)

  __call__ = dispatch


def daemonize():
  if os.fork() > 0: sys.exit(0)
  os.setsid()
  if os.fork() > 0: sys.exit(0)
  si = open('/dev/null', 'r')
  so = open('/dev/null', 'a+')
  se = open('/dev/null', 'a+', 0)
  os.dup2(si.fileno(), sys.stdin.fileno())
  os.dup2(so.fileno(), sys.stdout.fileno())
  os.dup2(se.fileno(), sys.stderr.fileno())


# Utility functions (exist in the plugin namespace)
logfile = open('/dev/null', 'w')

def log(message):
  logfile.write("[%s]  %s\n" % (time.ctime(), message.strip()))
  logfile.flush()


schemas = None
def get_storage_config(path):
  global schemas
  if schemas is None:
    schemas = loadStorageSchemas()

  for schema in schemas:
    if schema.matches(path):
      return schema

  raise Exception("No storage schema matched the metric '%s', check your storage-schemas.conf file." % path)


class MissingRequiredParam(Exception):
  def __init__(self, param):
    Exception.__init__(self)
    self.param = param


class PluginFail(Exception):
  pass

def _walk(tree, dispatch, nodePath):
  """Recursively walk the self and childs nodes in `tree` below `nodePath` 
  calling the `dispatch` function for each visit.
  """
  
  childs = tree.selfAndChildPaths(nodePath)
  if not childs:
    dispatch('directory_empty', nodePath)
    return
  
  for child, isMetric in childs:
    if isMetric:
      if child != nodePath:
        dispatch('node_found', tree.getNode(child))
      else:
        _walk(tree, dispatch, child)
    else:
      if (child != nodePath) and (nodePath != "*"):
        dispatch('directory_found', child)
      _walk(tree, dispatch, child)
  return
  
if __name__ == '__main__':
  default_plugindir = os.path.join(root_dir, 'plugins', 'maintenance')
  parser = OptionParser(usage='''%prog [options] plugin [plugin2 ...] [key=val ...]''')
  parser.add_option('--daemon', action='store_true')
  parser.add_option('--verbose', action='store_true', help="Increase truthiness")
  parser.add_option('--log', help="Write to the given log file instead of stdout")
  parser.add_option('--root', default='/opt/graphite/storage/ceres/',
                    help="Specify were to perform maintenance (default: /opt/graphite/storage/ceres/)")
  parser.add_option('--plugindir', default=default_plugindir,
                    help="Specify path to the plugin directory (default: %s)" % default_plugindir)
  parser.add_option('--configdir', default=None, help="Path to the carbon-daemon instance configuration directory")

  options, args = parser.parse_args()

  if not options.configdir:
    sys.exit("You must specify a --configdir for the carbon-daemon instance config directory "
             "that has the storage-rules.conf you want ceres-maintenance to use.")

  # Magic plugin vars
  Plugin.context['log'] = log
  Plugin.context['MissingRequiredParam'] = MissingRequiredParam
  Plugin.context['PluginFail'] = PluginFail

  from carbon.conf import settings, load_storage_rules
  settings.use_config_directory(options.configdir)
  Plugin.context['storage_rules'] = settings['STORAGE_RULES'] = load_storage_rules(settings)

  # User-defined plugin vars
  plugin_args = []
  for arg in args:
    if '=' in arg:
      key, value = arg.split('=')
      Plugin.context['params'][key] = value
    else:
      plugin_args.append(arg)

  if len(plugin_args) < 1:
    print "At least one plugin is required."
    parser.print_usage()
    sys.exit(1)

  # Load the plugins and setup event handlers
  finder = PluginFinder(options.plugindir)
  try:
    plugins = finder.find_plugins(plugin_args)
  except PluginNotFound, e:
    print e.message, ' searched in %s' % options.plugindir
    sys.exit(1)
  dispatcher = EventDispatcher()

  def dispatch(event, *args):
    if options.verbose:
      log("%s :: %s" % (event, args))
    dispatcher(event, *args)

  for plugin in plugins:
    try:
      plugin.load()
    except MissingRequiredParam, e:
      print "Failed to load %s plugin: required param '%s' must be specified" % (plugin.name, e.param)
      sys.exit(1)
    except PluginFail, e:
      print "Failed to load %s plugin: %s" % (plugin.name, e.message)
      sys.exit(1)
    for event, handler in plugin.event_handlers.items():
      dispatcher.add_handler(event, handler)

  # Daemonize & logify
  if options.daemon:
    daemonize()

  if options.log:
    logfile = open(options.log, 'a')

  if not (options.daemon or options.log):
    logfile = sys.stdout
  
  # TODO: add the server names and keyspace as command line args. 
  tree = carbon_cassandra_db.DataTree("/", "graphite", ["172.16.1.18"])

    # Begin walking the tree
  dispatch('maintenance_start', tree)
  _walk(tree, dispatch, "*")
  dispatch('maintenance_complete', tree)



