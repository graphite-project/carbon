import sys
from os import setregid, setreuid, environ
from pwd import getpwnam

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

try:
  from msgpack import packb
except:
  pass


def dropprivs(user):
  uid, gid = getpwnam(user)[2:4]
  initgroups(uid, gid)
  setregid(gid, gid)
  setreuid(uid, uid)
  return (uid, gid)


def run_twistd_plugin(filename):
    from carbon.conf import get_parser
    from twisted.scripts.twistd import ServerOptions

    bin_dir = dirname(abspath(filename))
    root_dir = dirname(bin_dir)
    environ.setdefault('GRAPHITE_ROOT', root_dir)

    program = basename(filename).split('.')[0]

    # First, parse command line options as the legacy carbon scripts used to
    # do.
    parser = get_parser(program)
    (options, args) = parser.parse_args()

    if not args:
      parser.print_usage()
      return

    # This isn't as evil as you might think
    __builtins__["instance"] = options.instance
    __builtins__["program"] = program

    # Then forward applicable options to either twistd or to the plugin itself.
    twistd_options = ["--no_save"]

    # If no reactor was selected yet, try to use the epoll reactor if
    # available.
    try:
        from twisted.internet import epollreactor
        twistd_options.append("--reactor=epoll")
    except:
        pass

    if options.debug or options.nodaemon:
        twistd_options.extend(["--nodaemon"])
    if options.profile:
        twistd_options.append("--profile")
    if options.pidfile:
        twistd_options.extend(["--pidfile", options.pidfile])
    if options.umask:
        twistd_options.extend(["--umask", options.umask])

    # Now for the plugin-specific options.
    twistd_options.append(program)

    if options.debug:
        twistd_options.append("--debug")

    for option_name, option_value in vars(options).items():
        if (option_value is not None and
            option_name not in ("debug", "profile", "pidfile", "umask", "nodaemon")):
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

# This whole song & dance is due to pickle being insecure
# yet performance critical for carbon. We leave the insecure
# mode (which is faster) as an option (USE_INSECURE_UNPICKLER).
# The SafeUnpickler classes were largely derived from
# http://nadiana.com/python-pickle-insecure
if USING_CPICKLE:
  class SafeUnpickler(object):
    PICKLE_SAFE = {
      'copy_reg' : set(['_reconstructor']),
      '__builtin__' : set(['list']),
      'collections': set(['deque']),
      'graphite.render.datalib': set(['TimeSeries']),
      'graphite.intervals': set(['Interval', 'IntervalSet']),
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
      try:
        ret = pickle_obj.load()
      except Exception as e:
        ret = None
        pass
      return ret

else:
  class SafeUnpickler(pickle.Unpickler):
    PICKLE_SAFE = {
      'copy_reg' : set(['_reconstructor']),
      '__builtin__' : set(['list']),
      'collections': set(['deque']),
      'graphite.render.datalib': set(['TimeSeries']),
      'graphite.intervals': set(['Interval', 'IntervalSet']),
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

def pack_data(datapoints, pack_type=None, safe_pickle=False):

    #msgpack serialization
    if pack_type == "msgpack":
      try:
        data = packb(datapoints)
      except Exception:
        raise ValueError('msgpack package not imported')

    #Default to pickle
    else:
      data = pickle.dumps(datapoints, protocol=-1)

    return data

def aggregate(aggregationMethod, knownValues):
    if aggregationMethod == 'average':
        return float(sum(knownValues)) / float(len(knownValues))
    elif aggregationMethod == 'sum':
        return float(sum(knownValues))
    elif aggregationMethod == 'last':
        return knownValues[len(knownValues) - 1]
    elif aggregationMethod == 'max':
        return max(knownValues)
    elif aggregationMethod == 'min':
        return min(knownValues)
    else:
        raise Exception("Unrecognized aggregation method %s" %
                        aggregationMethod)
