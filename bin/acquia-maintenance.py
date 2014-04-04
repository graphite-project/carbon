#!/usr/bin/env python
"""
Runs maintenance tasks using the carbon_cassandra_plugin.

This file is a fork of `ceres-maintenace` which uses a combination of reading
from disk, using a CeresTree and creating CeresNodes directly. It follows the
same principles as `ceres-maintenace`.

**Notes:**

1. Current way to run this script::

  export GRAPHITE_ROOT=/vagrant/src/carbon
  ./acquia-maintenance.py --configdir=/opt/graphite/conf/carbon-daemons/writer/ \
      acquia_rollup --keyspace=graphite --serverlist=127.0.0.1,127.0.0.2

2. `optparse` is deprecated as of Python 2.7
"""

import os
from optparse import OptionParser
import sys
import time
import traceback

from pycassa.system_manager import SystemManager

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

"""Events a plug in will be called to process"""
EVENTS = (
  'maintenance_start',
  'maintenance_complete',
  'node_found',
)


class Plugin:
    """Wraps a plug in, handling dispatch to its' event handlers."""
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
        return
        
    def handle_event(self, event, *args, **kwargs):
        handler = self.event_handlers.get(event)
        if handler:
            handler(*args, **kwargs)
        return
        
class PluginFinder:
    """Yields :class:`Plugin` objects wrapped in the python files in the 
    supplied directories."""
    
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
                    raise PluginNotFound("The plugin '%s' could not be "\
                        "found in %s" % (ref, self.plugin_dir))
        return

class PluginNotFound(Exception):
      pass


class EventDispatcher:
    """Extra layer of something that calls handlers and catches 
    exceptions."""
    def __init__(self, verbose):
        self.handlers = {}
        self.verbose = verbose
        
    def add_handler(self, event, handler):
        if event not in self.handlers:
            self.handlers[event] = []
            self.handlers[event].append(handler)
        return
        
    def dispatch(self, event, *args, **kwargs):
        if self.verbose:
            log("%s :: %s" % (eventType, args))
        for handler in self.handlers.get(event, []):
            try:
                handler(*args, **kwargs)
            except (Exception):
                log("--- Error in %s event-handler ---" % event)
                log( traceback.format_exc() )
                log('-' * 80)
        return
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
# amorton: this is replaced in the entry point if logging is enabled
logfile = open('/dev/null', 'w')

def log(message):
    logfile.write("[%s]  %s\n" % (time.ctime(), message.strip()))
    logfile.flush()
    return

class MissingRequiredParam(Exception):
    """amorton: think this is something a plugin can raise if it is not 
    configure correctly."""
    def __init__(self, param):
        Exception.__init__(self)
        self.param = param


class PluginFail(Exception):
    """amorton: think this is something a plugin can raise if it doe snot 
    want to load. Not sure why it needs a special error."""
    pass


class NodePathVisitor(object):
    """Visitor when walking the nodes in the tree.
    
    Calls through to the `dispatcher` and decides if we should visit the 
    children. 
    """
    
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        
    def __call__(self, parentPath, childPath, isMetric):
        """Called to visit the node at `childPath`. 
        
        Returns true if the children should be visited. 
        """
        if isMetric:
            if childPath != parentPath:
                self.dispatcher('node_found', tree.getNode(childPath))
            else:
                # visit the children of this childPath
                return True
        else:
            # got a branch node, visit children
            return True
            
def _walkRange(tree, visitor, useDC, startToken, endToken):
    """Visit the data nodes in between `startToken` and `endToken` and call the 
    `visitor` with the nodePath and isMetric flag.
  
    `useDC` is passed to selfAndChildPaths() on the `tree`
      
    We walk token ranges when the rollup script is running on multiple 
    cassandra nodes, so we are breaking the work up amongst many nodes.
    """

    pathIter = tree.selfAndChildPaths(None, dcName=useDC, 
        startToken=startToken, endToken=endToken)

    for childPath, isMetric in pathIter:
        # we do not know what parent to tell the visitor.
        # well we could get it from the child, but I dont want to.
        visitor(None, childPath, isMetric)
    return
  
def _walkTree(tree, visitor, nodePath, useDC):
    """Recursively walk the self and childs nodes in `tree` below `nodePath`
    calling the `visitor` function for each visit with nodePath and isMetric.
  
    If the visitor returns True a recursive call is made to visit the children
    for the current nodePath.
    
    We use walk the whole tree when we are running a single rollup 
    script that will rollup the entire data base.
    """

    pathIter = tree.selfAndChildPaths(nodePath, dcName=useDC)
  
    for childPath, isMetric in pathIter:
        if visitor(nodePath, childPath, isMetric):
            _walkTree(tree, visitor, childPath, useDC=useDC)
    return

def _tokenRangesForNodes(keyspace, serverList, targetNodes):
    """Get a list of the token ranges owned by the nodes in `targetNodes`.

    The list can be used to partition the maintenance process, e.g. run a 
    rollup daemon on each cassandra node that only works with carbon nodes in 
    the cassandra nodes primary token ranges. 

    Return a list of of [ (startToken, endToken, nodeIP)]
    """
  
    sysManager = None
    for server in serverList:
        sysManager = SystemManager(server)
        try:
            sysManager.describe_cluster_name()
        except (Exception) as e:
            sysManager = None
        else:
            break
    if not sysManager:
        raise RuntimeError("Could not connect to cassandra nodes %s" % (
            serverList,))
  
    # Get a list of the token ranges in the cluster.
    # create {endToken : tokenRange} from the list of TokenRanges
    # TokenRange is from pycasssa
    allRanges = {
        tokenRange.end_token : tokenRange
        for tokenRange in sysManager.describe_ring(keyspace)
    }
  
    # get the tokens assigned to the nodes we care about
    # either initial_token or the random tokens with vnodes
    # dict {'endToken' : ip_address}
    targetNodesSet = set(targetNodes)
    assignments = { 
        endToken : nodeIP
        for endToken, nodeIP in sysManager.describe_token_map().iteritems()
        if nodeIP in targetNodesSet
    }
  
    # merge to find the token ranges for the nodes we care about.
    # the token range will tell us what data to read for the rollups
    tokenRanges = []
    seenRanges = set()
    for endToken, nodeIP in assignments.iteritems():
        try:
            thisRange = allRanges[endToken]
        except (KeyError) as e:
            raise RuntimeError("Could not match assigned token %s from %s "\
                "to a token range from describe_ring()" % (endToken, nodeIP))
      
        assert nodeIP in thisRange.endpoints
        assert thisRange not in seenRanges
        seenRanges.add(thisRange)
      
        tokenRanges.append((thisRange.start_token, thisRange.end_token, 
            nodeIP))
  
    assert len(tokenRanges) == len(assignments)
    return tokenRanges
  
  
def _split_csv(option, opt, value, parser):
  """Callback function to parse a list args from CSV format."""
  setattr(parser.values, option.dest, value.split(','))

def opt_parser():
    """Create the opt parser for the script
    """
    default_plugindir = os.path.join(root_dir, 'plugins', 'maintenance')
    parser = OptionParser(usage='''%prog [options] plugin [plugin2 ...] "\
        "[key=val ...]''')
    
    parser.add_option('--daemon', action='store_true')
    parser.add_option('--verbose', action='store_true', 
        help="Increase truthiness")
    parser.add_option('--log', 
        help="Write to the given log file instead of stdout")
    parser.add_option('--root', default='/opt/graphite/storage/ceres/',
        help="Specify were to perform maintenance "\
        "(default: /opt/graphite/storage/ceres/)")
    parser.add_option('--plugindir', default=default_plugindir,
        help="Specify path to the plugin directory "\
        "(default: %s)" % default_plugindir)
    parser.add_option('--configdir', default=None, 
        help="Path to the carbon-daemon instance configuration directory")
    parser.add_option('--keyspace', default='graphite',
        help="Keyspace in which to initialize carbon.")
    parser.add_option('--serverlist',default=["localhost",], type='string',
        action='callback',callback=_split_csv,
        help="List of servers in Cassandra cluster: localhost1,localhost2.")
    parser.add_option('--dc-name', default=None, 
        help="Name of the Cassandra Data Centre to rollup nodes from.")
    parser.add_option('--rollup-targets', default=[], type='string',
        action='callback', callback=_split_csv, 
        help="Cassandra Node IPs to rollup metrics for.")
    return parser

if __name__ == '__main__':
    # Leaving this here because it has some things that use globals 
    
    options, args = opt_parser().parse_args()

    if not options.configdir:
        sys.exit("You must specify a --configdir for the carbon-daemon "\
            "instance config directory that has the storage-rules.conf you "\
            "want ceres-maintenance to use.")

    # Magic plugin vars
    Plugin.context['log'] = log
    Plugin.context['MissingRequiredParam'] = MissingRequiredParam
    Plugin.context['PluginFail'] = PluginFail

    # not sure why this import is here and not at the top, to scared to move
    from carbon.conf import settings, load_storage_rules
    settings.use_config_directory(options.configdir)
    Plugin.context['storage_rules'] = settings['STORAGE_RULES'] \
        = load_storage_rules(settings)

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
    dispatcher = EventDispatcher(options.verbose)


    for plugin in plugins:
        try:
            plugin.load()
        except MissingRequiredParam, e:
            print "Failed to load %s plugin: required param '%s' must "\
                "be specified" % (plugin.name, e.param)
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

    # pass the DC name so we can specify dcName=True when calling 
    # selfAndChildPaths later. 
    tree = carbon_cassandra_db.DataTree("/", options.keyspace, 
        options.serverlist, localDCName=options.dc_name)

    # Begin walking the tree
    dispatcher('maintenance_start', tree)
    
    visitor = NodePathVisitor(dispatcher)
    if options.rollup_targets: 
        # work on a sub set of the data nodes whose nodePath is in the the 
        # token ranges owned by the cassandra nodes in rollup_targets
        for startToken, endToken, nodeIP in _tokenRangesForNodes(
            options.keyspace, options.serverlist, options.rollup_targets):
      
            _walkRange(tree, visitor, 
                True if options.dc_name else False, startToken, endToken)
    else:
        # work on all the data nodes, or all those in the named DC
        _walkTree(tree, visitor, "*", 
            True if options.dc_name else False)
        dispatcher('maintenance_complete', tree)
