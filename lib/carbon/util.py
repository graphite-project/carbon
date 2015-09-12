import sys
import os
import pwd
import __builtin__

from os.path import abspath, basename, dirname, join
try:
  from cStringIO import StringIO
except ImportError:
  from StringIO import StringIO
try:
  import cPickle as pickle
  USING_CPICKLE = True
except ImportError:
  import pickle
  USING_CPICKLE = False

from time import sleep, time
from twisted.python.util import initgroups
from twisted.scripts.twistd import runApp


def dropprivs(user):
  uid, gid = pwd.getpwnam(user)[2:4]
  initgroups(uid, gid)
  os.setregid(gid, gid)
  os.setreuid(uid, uid)
  return (uid, gid)


def run_twistd_plugin(filename):
    from carbon.conf import get_parser
    from twisted.scripts.twistd import ServerOptions

    bin_dir = dirname(abspath(filename))
    root_dir = dirname(bin_dir)
    os.environ.setdefault('GRAPHITE_ROOT', root_dir)

    program = basename(filename).split('.')[0]

    # First, parse command line options as the legacy carbon scripts used to
    # do.
    parser = get_parser(program)
    (options, args) = parser.parse_args()

    if not args:
      parser.print_usage()
      return

    # This isn't as evil as you might think
    __builtin__.instance = options.instance
    __builtin__.program = program

    # Then forward applicable options to either twistd or to the plugin itself.
    twistd_options = ["--no_save"]

    # If no reactor was selected yet, try to use the epoll reactor if
    # available.
    try:
        from twisted.internet import epollreactor
        twistd_options.append("--reactor=epoll")
    except ImportError:
        pass

    if options.debug or options.nodaemon:
        twistd_options.extend(["--nodaemon"])
    if options.profile:
        twistd_options.extend(["--profile", options.profile])
    if options.profiler:
        twistd_options.extend(["--profiler", options.profiler])
    if options.pidfile:
        twistd_options.extend(["--pidfile", options.pidfile])
    if options.umask:
        twistd_options.extend(["--umask", options.umask])
    if options.syslog:
        twistd_options.append("--syslog")

    # Now for the plugin-specific options.
    twistd_options.append(program)

    if options.debug:
        twistd_options.append("--debug")

    for option_name, option_value in vars(options).items():
        if (option_value is not None and
            option_name not in ("debug", "profile", "profiler", "pidfile", "umask", "nodaemon", "syslog")):
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


class TokenBucket(object):
  '''This is a basic tokenbucket rate limiter implementation for use in
  enforcing various configurable rate limits'''
  def __init__(self, capacity, fill_rate):
    '''Capacity is the total number of tokens the bucket can hold, fill rate is
    the rate in tokens (or fractional tokens) to be added to the bucket per
    second.'''
    self.capacity = float(capacity)
    self._tokens = float(capacity)
    self.fill_rate = float(fill_rate)
    self.timestamp = time()

  def drain(self, cost, blocking=False):
    '''Given a number of tokens (or fractions) drain will return True and
    drain the number of tokens from the bucket if the capacity allows,
    otherwise we return false and leave the contents of the bucket.'''
    if cost <= self.tokens:
      self._tokens -= cost
      return True
    else:
      if blocking:
        tokens_needed = cost - self._tokens
        seconds_per_token = 1 / self.fill_rate
        seconds_left = seconds_per_token * self.fill_rate
        sleep(self.timestamp + seconds_left - time())
        self._tokens -= cost
        return True
      return False

  def setCapacityAndFillRate(self, new_capacity, new_fill_rate):
    delta = float(new_capacity) - self.capacity
    self.capacity = float(new_capacity)
    self.fill_rate = float(new_fill_rate)
    self._tokens = delta + self._tokens

  @property
  def tokens(self):
    '''The tokens property will return the current number of tokens in the
    bucket.'''
    if self._tokens < self.capacity:
      now = time()
      delta = self.fill_rate * (now - self.timestamp)
      self._tokens = min(self.capacity, self._tokens + delta)
      self.timestamp = now
    return self._tokens


class PluginRegistrar(type):
  """Clever subclass detection hack that makes plugin loading trivial.
  To use this, define an abstract base class for plugin implementations
  that defines the plugin API. Give that base class a __metaclass__ of
  PluginRegistrar, and define a 'plugins = {}' class member. Subclasses
  defining a 'plugin_name' member will then appear in the plugins dict.
  """
  def __init__(classObj, name, bases, members):
    super(PluginRegistrar, classObj).__init__(name, bases, members)
    if hasattr(classObj, 'plugin_name'):
      classObj.plugins[classObj.plugin_name] = classObj


def load_module(module_path, member=None):
  module_name = splitext(basename(module_path))[0]
  module_file = open(module_path, 'U')
  description = ('.py', 'U', imp.PY_SOURCE)
  module = imp.load_module(module_name, module_file, module_path, description)
  if member:
    return getattr(module, member)
  else:
    return module
