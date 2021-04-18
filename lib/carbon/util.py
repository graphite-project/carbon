import sys
import os
import pwd
import re

try:
  import builtins as __builtin__
except ImportError:
  import __builtin__

from hashlib import sha256
from os.path import abspath, basename, dirname
import socket
from time import sleep, time
from twisted.python.util import initgroups
from twisted.scripts.twistd import runApp
from carbon.log import setDebugEnabled
try:
    from OpenSSL import SSL
except ImportError:
    SSL = None


# BytesIO is needed on py3 as StringIO does not operate on byte input anymore
# We could use BytesIO on py2 as well but it is slower than StringIO
if sys.version_info >= (3, 0):
  from io import BytesIO as StringIO
else:
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


def dropprivs(user):
  uid, gid = pwd.getpwnam(user)[2:4]
  initgroups(uid, gid)
  os.setregid(gid, gid)
  os.setreuid(uid, uid)
  return (uid, gid)


def enableTcpKeepAlive(transport, enable, settings):
  if not enable or not hasattr(transport, 'getHandle'):
    return

  fd = transport.getHandle()
  if SSL:
      if isinstance(fd, SSL.Connection):
          return
  if fd.type != socket.SOCK_STREAM:
    return

  transport.setTcpKeepAlive(1)
  for attr in ['TCP_KEEPIDLE', 'TCP_KEEPINTVL', 'TCP_KEEPCNT']:
    flag = getattr(socket, attr, None)
    value = getattr(settings, attr, None)
    if not flag or value is None:
      continue
    fd.setsockopt(socket.SOL_TCP, flag, value)


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
        from twisted.internet import epollreactor  # noqa: F401
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
    if options.logger:
        twistd_options.extend(["--logger", options.logger])
    if options.logger:
        twistd_options.extend(["--logfile", options.logfile])
    if options.syslog:
        twistd_options.append("--syslog")

    # Now for the plugin-specific options.
    twistd_options.append(program)

    if options.debug:
        twistd_options.append("--debug")
        setDebugEnabled(True)

    for option_name, option_value in vars(options).items():
        if (option_value is not None and option_name not in (
                "debug", "profile", "profiler", "pidfile", "umask", "nodaemon", "syslog",
                "logger", "logfile")):
            twistd_options.extend(["--%s" % option_name.replace("_", "-"), option_value])

    # Finally, append extra args so that twistd has a chance to process them.
    twistd_options.extend(args)

    config = ServerOptions()
    config.parseOptions(twistd_options)

    runApp(config)


def parseDestination(dest_string):
    s = dest_string.strip()
    bidx = s.rfind(']:')    # find closing bracket and following colon.
    cidx = s.find(':')
    if s.startswith('[') and bidx is not None:
        server = s[1:bidx]
        port = s[bidx + 2:]
    elif cidx is not None:
        server = s[:cidx]
        port = s[cidx + 1:]
    else:
        raise ValueError("Invalid destination string \"%s\"" % dest_string)

    if ':' in port:
        port, _, instance = port.partition(':')
    else:
        instance = None

    return server, int(port), instance


def parseDestinations(destination_strings):
    return [parseDestination(dest_string) for dest_string in destination_strings]


# Yes this is duplicated in whisper. Yes, duplication is bad.
# But the code is needed in both places and we do not want to create
# a dependency on whisper especially as carbon moves toward being a more
# generic storage service that can use various backends.
UnitMultipliers = {
  's': 1,
  'm': 60,
  'h': 60 * 60,
  'd': 60 * 60 * 24,
  'w': 60 * 60 * 24 * 7,
  'y': 60 * 60 * 24 * 365,
}


def getUnitString(s):
  if s not in UnitMultipliers:
    raise ValueError("Invalid unit '%s'" % s)
  return s


def parseRetentionDef(retentionDef):
  import re
  (precision, points) = retentionDef.strip().split(':')

  if precision.isdigit():
    precision = int(precision) * UnitMultipliers[getUnitString('s')]
  else:
    precision_re = re.compile(r'^(\d+)([a-z]+)$')
    match = precision_re.match(precision)
    if match:
      precision = int(match.group(1)) * UnitMultipliers[getUnitString(match.group(2))]
    else:
      raise ValueError("Invalid precision specification '%s'" % precision)

  if points.isdigit():
    points = int(points)
  else:
    points_re = re.compile(r'^(\d+)([a-z]+)$')
    match = points_re.match(points)
    if match:
      points = int(match.group(1)) * UnitMultipliers[getUnitString(match.group(2))] / precision
    else:
      raise ValueError("Invalid retention specification '%s'" % points)

  return (precision, points)


# This whole song & dance is due to pickle being insecure
# yet performance critical for carbon. We leave the insecure
# mode (which is faster) as an option (USE_INSECURE_UNPICKLER).
# The SafeUnpickler classes were largely derived from
# http://nadiana.com/python-pickle-insecure
if USING_CPICKLE:
  class SafeUnpickler(object):
    PICKLE_SAFE = {
      'copy_reg': set(['_reconstructor']),
      '__builtin__': set(['object']),
    }

    @classmethod
    def find_class(cls, module, name):
      if module not in cls.PICKLE_SAFE:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe module %s' % module)
      __import__(module)
      mod = sys.modules[module]
      if name not in cls.PICKLE_SAFE[module]:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe class %s' % name)
      return getattr(mod, name)  # skipcq: PTC-W0034

    @classmethod
    def loads(cls, pickle_string):
      pickle_obj = pickle.Unpickler(StringIO(pickle_string))
      pickle_obj.find_global = cls.find_class
      return pickle_obj.load()

else:
  class SafeUnpickler(pickle.Unpickler):
    PICKLE_SAFE = {
      'copy_reg': set(['_reconstructor']),
      '__builtin__': set(['object']),
    }

    def find_class(self, module, name):
      if module not in self.PICKLE_SAFE:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe module %s' % module)
      __import__(module)
      mod = sys.modules[module]
      if name not in self.PICKLE_SAFE[module]:
        raise pickle.UnpicklingError('Attempting to unpickle unsafe class %s' % name)
      return getattr(mod, name)  # skipcq: PTC-W0034

    @classmethod
    def loads(cls, pickle_string):
      if sys.version_info >= (3, 0):
        return cls(StringIO(pickle_string), encoding='utf-8').load()
      else:
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

    if not blocking:
      return False

    tokens_needed = cost - self._tokens
    seconds_per_token = 1 / self.fill_rate
    seconds_left = seconds_per_token * tokens_needed
    time_to_sleep = self.timestamp + seconds_left - time()
    if time_to_sleep > 0:
      sleep(time_to_sleep)

    self._tokens -= cost
    return True

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


class TaggedSeries(object):
  prohibitedTagChars = ';!^='

  @classmethod
  def validateTagAndValue(cls, tag, value):
    """validate the given tag / value based on the specs in the documentation"""
    if len(tag) == 0:
      raise Exception('Tag may not be empty')
    if len(value) == 0:
      raise Exception('Value for tag "{tag}" may not be empty'.format(tag=tag))

    for char in cls.prohibitedTagChars:
      if char in tag:
        raise Exception(
          'Character "{char}" is not allowed in tag "{tag}"'.format(char=char, tag=tag))

    if ';' in value:
      raise Exception(
        'Character ";" is not allowed in value "{value}" of tag {tag}'.format(value=value, tag=tag))

    if value[0] == '~':
      raise Exception('Tag values are not allowed to start with "~" in tag "{tag}"'.format(tag=tag))

  @classmethod
  def parse(cls, path):
    # if path is in openmetrics format: metric{tag="value",...}
    if path[-2:] == '"}' and '{' in path:
      return cls.parse_openmetrics(path)

    # path is a carbon path with optional tags: metric;tag=value;...
    return cls.parse_carbon(path)

  @classmethod
  def parse_openmetrics(cls, path):
    """parse a path in openmetrics format: metric{tag="value",...}

    https://github.com/RichiH/OpenMetrics
    """
    (metric, rawtags) = path[0:-1].split('{', 2)
    if not metric:
      raise Exception('Cannot parse path %s, no metric found' % path)

    tags = {}

    while len(rawtags) > 0:
      m = re.match(r'([^=]+)="((?:[\\]["\\]|[^"\\])+)"(:?,|$)', rawtags)
      if not m:
        raise Exception('Cannot parse path %s, invalid segment %s' % (path, rawtags))

      tag = m.group(1)
      value = m.group(2).replace(r'\"', '"').replace(r'\\', '\\')

      cls.validateTagAndValue(tag, value)

      tags[tag] = value
      rawtags = rawtags[len(m.group(0)):]

    tags['name'] = cls.sanitize_name_as_tag_value(metric)
    return cls(metric, tags)

  @classmethod
  def parse_carbon(cls, path):
    """parse a carbon path with optional tags: metric;tag=value;..."""
    segments = path.split(';')

    metric = segments[0]
    if not metric:
      raise Exception('Cannot parse path %s, no metric found' % path)

    tags = {}

    for segment in segments[1:]:
      tag = segment.split('=', 1)
      if len(tag) != 2 or not tag[0]:
        raise Exception('Cannot parse path %s, invalid segment %s' % (path, segment))

      cls.validateTagAndValue(*tag)

      tags[tag[0]] = tag[1]

    tags['name'] = cls.sanitize_name_as_tag_value(metric)
    return cls(metric, tags)

  @staticmethod
  def sanitize_name_as_tag_value(name):
    """take a metric name and sanitize it so it is guaranteed to be a valid tag value"""
    sanitized = name.lstrip('~')

    if len(sanitized) == 0:
      raise Exception('Cannot use metric name %s as tag value, results in an empty string' % (name))

    return sanitized

  @staticmethod
  def format(tags):
    return tags.get('name', '') + ''.join(sorted([
      ';%s=%s' % (tag, value)
      for tag, value in tags.items()
      if tag != 'name'
    ]))

  @staticmethod
  def encode(metric, sep='.', hash_only=False):
    """
    Helper function to encode tagged series for storage in whisper etc

    When tagged series are detected, they are stored in a separate hierarchy of folders under a
    top-level _tagged folder, where subfolders are created by using the first 3 hex digits of the
    sha256 hash of the tagged metric path (4096 possible folders), and second-level subfolders are
    based on the following 3 hex digits (another 4096 possible folders) for a total of 4096^2
    possible subfolders. The metric files themselves are created with any . in the metric path
    replaced with -, to avoid any issues where metrics, tags or values containing a '.' would end
    up creating further subfolders. This helper is used by both whisper and ceres, but by design
    each carbon database and graphite-web finder is responsible for handling its own encoding so
    that different backends can create their own schemes if desired.

    The hash_only parameter can be set to True to use the hash as the filename instead of a
    human-readable name.  This avoids issues with filename length restrictions, at the expense of
    being unable to decode the filename and determine the original metric name.

    A concrete example:

    .. code-block:: none

      some.metric;tag1=value2;tag2=value.2

      with sha256 hash starting effaae would be stored in:

      _tagged/eff/aae/some-metric;tag1=value2;tag2=value-2.wsp (whisper)
      _tagged/eff/aae/some-metric;tag1=value2;tag2=value-2 (ceres)

    """
    if ';' in metric:
      metric_hash = sha256(metric.encode('utf8')).hexdigest()
      return sep.join([
        '_tagged',
        metric_hash[0:3],
        metric_hash[3:6],
        metric_hash if hash_only else metric.replace('.', '_DOT_')
      ])

    # metric isn't tagged, just replace dots with the separator and trim any leading separator
    return metric.replace('.', sep).lstrip(sep)

  @staticmethod
  def decode(path, sep='.'):
    """
    Helper function to decode tagged series from storage in whisper etc
    """
    if path.startswith('_tagged'):
      return path.split(sep, 3)[-1].replace('_DOT_', '.')

    # metric isn't tagged, just replace the separator with dots
    return path.replace(sep, '.')

  def __init__(self, metric, tags, series_id=None):
    self.metric = metric
    self.tags = tags
    self.id = series_id

  @property
  def path(self):
    return self.__class__.format(self.tags)
