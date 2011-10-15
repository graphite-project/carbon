import sys
import os
import pwd
from optparse import OptionParser
from os.path import abspath, basename, dirname, join
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
try:
  import cPickle as pickle
  USING_CPICKLE = True
except:
  import pickle
  USING_CPICKLE = False

from twisted.python.util import initgroups
from twisted.scripts.twistd import runApp
from twisted.scripts._twistd_unix import daemonize


daemonize = daemonize # Backwards compatibility


def dropprivs(user):
  uid, gid = pwd.getpwnam(user)[2:4]
  initgroups(uid, gid)
  os.setregid(gid, gid)
  os.setreuid(uid, uid)
  return (uid, gid)


def run_twistd_plugin(filename):
    from twisted.scripts.twistd import ServerOptions

    bin_dir = dirname(abspath(filename))
    root_dir = dirname(bin_dir)
    os.environ.setdefault('GRAPHITE_ROOT', root_dir)

    # First, parse command line options as the legacy carbon scripts used to
    # do.
    parser = OptionParser(usage="%prog [options] <instance> <start|stop|status>")
    parser.add_option(
        "--debug", action="store_true",
        help="Run in the foreground, log to stdout")
    parser.add_option(
        "--profile",
        help="Record performance profile data to the given file")
    parser.add_option(
        "--pidfile", default=None,
        help="Write pid to the given file")
    parser.add_option(
        "--config",
        default=None,
        help="Use the given instance configuration directory")
    parser.add_option(
        "--logdir",
        default=None,
        help="Write logs in the given directory")

    (options, args) = parser.parse_args()

    if len(args) != 2:
        print "Exactly 2 arguments required, %d given" % len(args)
        parser.print_usage()
        raise SystemExit(1)

    instance, action = args

    if action not in ("start", "stop", "status"):
        print "Invalid action '%s'" % action
        parser.print_usage()
        raise SystemExit(1)

    # Now forward applicable options to either twistd or to the plugin itself.
    twistd_options = ["--no_save"]

    # If no reactor was selected yet, try to use the epoll reactor if
    # available.
    try:
        from twisted.internet import epollreactor
        twistd_options.append("--reactor=epoll")
    except:
        pass

    if options.debug:
        twistd_options.extend(["--nodaemon"])
    if options.profile:
        twistd_options.append("--profile")
    if options.pidfile:
        twistd_options.extend(["--pidfile", options.pidfile])

    # Now for the plugin-specific options.
    twistd_options.append('carbon-daemon')

    if options.debug:
        twistd_options.append("--debug")

    for option_name, option_value in vars(options).items():
        if (option_value is not None and
            option_name not in ("debug", "profile", "pidfile")):
            twistd_options.extend(["--%s" % option_name.replace("_", "-"),
                                   option_value])

    # Finally, append extra args so that twistd has a chance to process them.
    twistd_options.extend(args)

    config = ServerOptions()
    config.parseOptions(twistd_options)

    runApp(config)


def parseDestinations(destination_strings):
  destinations = []

  for dest_string in destination_strings:
    parts = dest_string.strip().split(':')
    if len(parts) == 2:
      server, port = parts
      instance = None
    elif len(parts) == 3:
      server, port, instance = parts
    else:
      raise ValueError("Invalid destination string \"%s\"" % dest_string)

    destinations.append( (server, int(port), instance) )

  return destinations


# Yes this is duplicated in whisper. Yes, duplication is bad.
# But the code is needed in both places and we do not want to create
# a dependency on whisper especiaily as carbon moves toward being a more
# generic storage service that can use various backends.
UnitMultipliers = {
  's' : 1,
  'm' : 60,
  'h' : 60 * 60,
  'd' : 60 * 60 * 24,
  'y' : 60 * 60 * 24 * 365,
}

def parseRetentionDefs(retentionDefs):
  return [parseRetentionDef(d.strip()) for d in retentionDefs.split(',')]

def parseRetentionDef(retentionDef):
  (precision, points) = retentionDef.strip().split(':')

  if precision.isdigit():
    precisionUnit = 's'
    precision = int(precision)
  else:
    precisionUnit = precision[-1]
    precision = int( precision[:-1] )

  if points.isdigit():
    pointsUnit = None
    points = int(points)
  else:
    pointsUnit = points[-1]
    points = int( points[:-1] )

  if precisionUnit not in UnitMultipliers:
    raise ValueError("Invalid unit: '%s'" % precisionUnit)

  if pointsUnit not in UnitMultipliers and pointsUnit is not None:
    raise ValueError("Invalid unit: '%s'" % pointsUnit)

  precision = precision * UnitMultipliers[precisionUnit]

  if pointsUnit:
    points = points * UnitMultipliers[pointsUnit] / precision

  return (precision, points)


# This whole song & dance is due to pickle being insecure
# yet performance critical for carbon. We leave the insecure
# mode (which is faster) as an option (USE_INSECURE_UNPICKLER).
# The SafeUnpickler classes were largely derived from
# http://nadiana.com/python-pickle-insecure
if USING_CPICKLE:
  class SafeUnpickler(object):
    PICKLE_SAFE = {
      'copy_reg' : set(['_reconstructor']),
      '__builtin__' : set(['object']),
    }

    @classmethod
    def find_class(cls, module, name):
      if not module in cls.PICKLE_SAFE:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe module %s' % module)
      __import__(module)
      mod = sys.modules[module]
      if not name in cls.PICKLE_SAFE[module]:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe class %s' % name)
      return getattr(mod, name)

    @classmethod
    def loads(cls, pickle_string):
      pickle_obj = pickle.Unpickler(StringIO(pickle_string))
      pickle_obj.find_global = cls.find_class
      return pickle_obj.load()

else:
  class SafeUnpickler(pickle.Unpickler):
    PICKLE_SAFE = {
      'copy_reg' : set(['_reconstructor']),
      '__builtin__' : set(['object']),
    }
    def find_class(self, module, name):
      if not module in self.PICKLE_SAFE:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe module %s' % module)
      __import__(module)
      mod = sys.modules[module]
      if not name in self.PICKLE_SAFE[module]:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe class %s' % name)
      return getattr(mod, name)
 
    @classmethod
    def loads(cls, pickle_string):
      return cls(StringIO(pickle_string)).load()
 

def get_unpickler(insecure=False):
  if insecure:
    return pickle
  else:
    return SafeUnpickler
