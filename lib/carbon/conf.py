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

import os
import sys
import pwd
import errno

from os.path import join, dirname, normpath, exists, isdir
from optparse import OptionParser

try:
    from ConfigParser import ConfigParser
# ConfigParser is renamed to configparser in py3
except ImportError:
    from configparser import ConfigParser

from carbon import log, state
from carbon.database import TimeSeriesDatabase
from carbon.routers import DatapointRouter
from carbon.exceptions import CarbonConfigException

from twisted.python import usage


defaults = dict(
  USER="",
  MAX_CACHE_SIZE=float('inf'),
  MAX_UPDATES_PER_SECOND=500,
  MAX_CREATES_PER_MINUTE=float('inf'),
  MIN_TIMESTAMP_RESOLUTION=0,
  MIN_TIMESTAMP_LAG=0,
  LINE_RECEIVER_INTERFACE='0.0.0.0',
  LINE_RECEIVER_PORT=2003,
  ENABLE_UDP_LISTENER=False,
  UDP_RECEIVER_INTERFACE='0.0.0.0',
  UDP_RECEIVER_PORT=2003,
  PICKLE_RECEIVER_INTERFACE='0.0.0.0',
  PICKLE_RECEIVER_PORT=2004,
  MAX_RECEIVER_CONNECTIONS=float('inf'),
  CACHE_QUERY_INTERFACE='0.0.0.0',
  CACHE_QUERY_PORT=7002,
  LOG_UPDATES=True,
  LOG_CREATES=True,
  LOG_CACHE_HITS=True,
  LOG_CACHE_QUEUE_SORTS=True,
  DATABASE='whisper',
  WHISPER_AUTOFLUSH=False,
  WHISPER_SPARSE_CREATE=False,
  WHISPER_FALLOCATE_CREATE=False,
  WHISPER_LOCK_WRITES=False,
  WHISPER_FADVISE_RANDOM=False,
  CERES_MAX_SLICE_GAP=80,
  CERES_NODE_CACHING_BEHAVIOR='all',
  CERES_SLICE_CACHING_BEHAVIOR='latest',
  CERES_LOCK_WRITES=False,
  MAX_DATAPOINTS_PER_MESSAGE=500,
  MAX_AGGREGATION_INTERVALS=5,
  FORWARD_ALL=True,
  MAX_QUEUE_SIZE=1000,
  QUEUE_LOW_WATERMARK_PCT=0.8,
  TIME_TO_DEFER_SENDING=0.0001,
  ENABLE_AMQP=False,
  AMQP_METRIC_NAME_IN_BODY=False,
  AMQP_VERBOSE=False,
  AMQP_SPEC=None,
  BIND_PATTERNS=['#'],
  GRAPHITE_URL='http://127.0.0.1:80',
  ENABLE_TAGS=True,
  SKIP_TAGS_FOR_NONTAGGED=True,
  TAG_UPDATE_INTERVAL=100,
  TAG_BATCH_SIZE=100,
  TAG_QUEUE_SIZE=10000,
  TAG_HASH_FILENAMES=True,
  TAG_RELAY_NORMALIZED=False,
  ENABLE_MANHOLE=False,
  MANHOLE_INTERFACE='127.0.0.1',
  MANHOLE_PORT=7222,
  MANHOLE_USER="",
  MANHOLE_PUBLIC_KEY="",
  MANHOLE_HOST_KEY_DIR="",
  RELAY_METHOD='rules',
  DYNAMIC_ROUTER=False,
  DYNAMIC_ROUTER_MAX_RETRIES=5,
  ROUTER_HASH_TYPE=None,
  REPLICATION_FACTOR=1,
  DIVERSE_REPLICAS=True,
  DESTINATIONS=[],
  DESTINATION_PROTOCOL="pickle",
  DESTINATION_TRANSPORT="none",
  DESTINATION_SSL_CA=None,
  DESTINATION_POOL_REPLICAS=False,
  USE_FLOW_CONTROL=True,
  USE_INSECURE_UNPICKLER=False,
  USE_WHITELIST=False,
  CARBON_METRIC_PREFIX='carbon',
  CARBON_METRIC_INTERVAL=60,
  CACHE_WRITE_STRATEGY='sorted',
  WRITE_BACK_FREQUENCY=None,
  MIN_RESET_STAT_FLOW=1000,
  MIN_RESET_RATIO=0.9,
  MIN_RESET_INTERVAL=121,
  TCP_KEEPALIVE=True,
  TCP_KEEPIDLE=10,
  TCP_KEEPINTVL=30,
  TCP_KEEPCNT=2,
  USE_RATIO_RESET=False,
  LOG_LISTENER_CONN_LOST=False,
  LOG_LISTENER_CONN_SUCCESS=True,
  LOG_AGGREGATOR_MISSES=True,
  AGGREGATION_RULES='aggregation-rules.conf',
  REWRITE_RULES='rewrite-rules.conf',
  RELAY_RULES='relay-rules.conf',
  ENABLE_LOGROTATION=True,
  METRIC_CLIENT_IDLE_TIMEOUT=None,
  CACHE_METRIC_NAMES_MAX=0,
  CACHE_METRIC_NAMES_TTL=0,
  RAVEN_DSN=None,
  PICKLE_RECEIVER_MAX_LENGTH=2**20,
)


def _process_alive(pid):
    if exists("/proc"):
        return exists("/proc/%d" % pid)
    else:
        try:
            os.kill(int(pid), 0)
            return True
        except OSError as err:
            return err.errno == errno.EPERM


class OrderedConfigParser(ConfigParser):
  """Hacky workaround to ensure sections are always returned in the order
   they are defined in. Note that this does *not* make any guarantees about
   the order of options within a section or the order in which sections get
   written back to disk on write()."""
  _ordered_sections = []

  def read(self, path):
    # Verifies a file exists *and* is readable
    if not os.access(path, os.R_OK):
        raise CarbonConfigException("Error: Missing config file or wrong perms on %s" % path)

    result = ConfigParser.read(self, path)
    sections = []
    with open(path) as f:
      for line in f:
        line = line.strip()

        if line.startswith('[') and line.endswith(']'):
          sections.append(line[1:-1])

    self._ordered_sections = sections

    return result

  def sections(self):
    return list(self._ordered_sections)  # return a copy for safety


class Settings(dict):
  __getattr__ = dict.__getitem__

  def __init__(self):
    dict.__init__(self)
    self.update(defaults)

  def readFrom(self, path, section):
    parser = ConfigParser()
    if not parser.read(path):
      raise CarbonConfigException("Failed to read config file %s" % path)

    if not parser.has_section(section):
      return

    for key, value in parser.items(section):
      key = key.upper()

      # Detect type from defaults dict
      if key in defaults:
        valueType = type(defaults[key])
      else:
        valueType = str

      if valueType is list:
        value = [v.strip() for v in value.split(',')]

      elif valueType is bool:
        value = parser.getboolean(section, key)

      else:
        # Attempt to figure out numeric types automatically
        try:
          value = int(value)
        except ValueError:
          try:
            value = float(value)
          except ValueError:
            pass

      self[key] = value


settings = Settings()
settings.update(defaults)


class CarbonCacheOptions(usage.Options):

    optFlags = [
        ["debug", "", "Run in debug mode."],
    ]

    optParameters = [
        ["config", "c", None, "Use the given config file."],
        ["instance", "", "a", "Manage a specific carbon instance."],
        ["logdir", "", None, "Write logs to the given directory."],
        ["whitelist", "", None, "List of metric patterns to allow."],
        ["blacklist", "", None, "List of metric patterns to disallow."],
    ]

    def postOptions(self):
        global settings

        program = self.parent.subCommand

        # Use provided pidfile (if any) as default for configuration. If it's
        # set to 'twistd.pid', that means no value was provided and the default
        # was used.
        pidfile = self.parent["pidfile"]
        if pidfile.endswith("twistd.pid"):
            pidfile = None
        self["pidfile"] = pidfile

        # Enforce a default umask of '022' if none was set.
        if "umask" not in self.parent or self.parent["umask"] is None:
            self.parent["umask"] = 0o022

        # Read extra settings from the configuration file.
        program_settings = read_config(program, self)
        settings.update(program_settings)
        settings["program"] = program

        # Normalize and expand paths
        def cleanpath(path):
          return os.path.normpath(os.path.expanduser(path))
        settings["STORAGE_DIR"] = cleanpath(settings["STORAGE_DIR"])
        settings["LOCAL_DATA_DIR"] = cleanpath(settings["LOCAL_DATA_DIR"])
        settings["WHITELISTS_DIR"] = cleanpath(settings["WHITELISTS_DIR"])
        settings["PID_DIR"] = cleanpath(settings["PID_DIR"])
        settings["LOG_DIR"] = cleanpath(settings["LOG_DIR"])
        settings["pidfile"] = cleanpath(settings["pidfile"])

        # Set process uid/gid by changing the parent config, if a user was
        # provided in the configuration file.
        if settings.USER:
            self.parent["uid"], self.parent["gid"] = (
                pwd.getpwnam(settings.USER)[2:4])

        # Set the pidfile in parent config to the value that was computed by
        # C{read_config}.
        self.parent["pidfile"] = settings["pidfile"]

        storage_schemas = join(settings["CONF_DIR"], "storage-schemas.conf")
        if not exists(storage_schemas):
            print("Error: missing required config %s" % storage_schemas)
            sys.exit(1)

        if settings.CACHE_WRITE_STRATEGY not in ('timesorted', 'sorted', 'max', 'naive'):
            log.err("%s is not a valid value for CACHE_WRITE_STRATEGY, defaulting to %s" %
                    (settings.CACHE_WRITE_STRATEGY, defaults['CACHE_WRITE_STRATEGY']))
        else:
            log.msg("Using %s write strategy for cache" % settings.CACHE_WRITE_STRATEGY)

        # Database-specific settings
        database = settings.DATABASE
        if database not in TimeSeriesDatabase.plugins:
            print("No database plugin implemented for '%s'" % database)
            raise SystemExit(1)

        database_class = TimeSeriesDatabase.plugins[database]
        state.database = database_class(settings)

        settings.CACHE_SIZE_LOW_WATERMARK = settings.MAX_CACHE_SIZE * 0.95

        if "action" not in self:
            self["action"] = "start"
        self.handleAction()

        # If we are not running in debug mode or non-daemon mode, then log to a
        # directory, otherwise log output will go to stdout. If parent options
        # are set to log to syslog, then use that instead.
        if not self["debug"]:
            if self.parent.get("syslog", None):
                prefix = "%s-%s[%d]" % (program, self["instance"], os.getpid())
                log.logToSyslog(prefix)
            elif not self.parent["nodaemon"]:
                logdir = settings.LOG_DIR
                if not isdir(logdir):
                    os.makedirs(logdir)
                    if settings.USER:
                        # We have not yet switched to the specified user,
                        # but that user must be able to create files in this
                        # directory.
                        os.chown(logdir, self.parent["uid"], self.parent["gid"])
                log.logToDir(logdir)

        if self["whitelist"] is None:
            self["whitelist"] = join(settings["CONF_DIR"], "whitelist.conf")
        settings["whitelist"] = self["whitelist"]

        if self["blacklist"] is None:
            self["blacklist"] = join(settings["CONF_DIR"], "blacklist.conf")
        settings["blacklist"] = self["blacklist"]

    def parseArgs(self, *action):
        """If an action was provided, store it for further processing."""
        if len(action) == 1:
            self["action"] = action[0]

    def handleAction(self):
        """Handle extra argument for backwards-compatibility.

        * C{start} will simply do minimal pid checking and otherwise let twistd
              take over.
        * C{stop} will kill an existing running process if it matches the
              C{pidfile} contents.
        * C{status} will simply report if the process is up or not.
        """
        action = self["action"]
        pidfile = self.parent["pidfile"]
        program = settings["program"]
        instance = self["instance"]

        if action == "stop":
            if not exists(pidfile):
                print("Pidfile %s does not exist" % pidfile)
                raise SystemExit(0)
            pf = open(pidfile, 'r')
            try:
                pid = int(pf.read().strip())
                pf.close()
            except ValueError:
                print("Failed to parse pid from pidfile %s" % pidfile)
                pf.close()
                try:
                    print("removing corrupted pidfile %s" % pidfile)
                    os.unlink(pidfile)
                except IOError:
                    print("Could not remove pidfile %s" % pidfile)
                raise SystemExit(1)
            except IOError:
                print("Could not read pidfile %s" % pidfile)
                raise SystemExit(1)
            print("Sending kill signal to pid %d" % pid)
            try:
                os.kill(pid, 15)
            except OSError as e:
                if e.errno == errno.ESRCH:
                    print("No process with pid %d running" % pid)
                else:
                    raise

            raise SystemExit(0)

        elif action == "status":
            if not exists(pidfile):
                print("%s (instance %s) is not running" % (program, instance))
                raise SystemExit(1)
            pf = open(pidfile, "r")
            try:
                pid = int(pf.read().strip())
                pf.close()
            except ValueError:
                print("Failed to parse pid from pidfile %s" % pidfile)
                pf.close()
                try:
                    print("removing corrupted pidfile %s" % pidfile)
                    os.unlink(pidfile)
                except IOError:
                    print("Could not remove pidfile %s" % pidfile)
                raise SystemExit(1)
            except IOError:
                print("Failed to read pid from %s" % pidfile)
                raise SystemExit(1)

            if _process_alive(pid):
                print("%s (instance %s) is running with pid %d" %
                      (program, instance, pid))
                raise SystemExit(0)
            else:
                print("%s (instance %s) is not running" % (program, instance))
                raise SystemExit(1)

        elif action == "start":
            if exists(pidfile):
                pf = open(pidfile, 'r')
                try:
                    pid = int(pf.read().strip())
                    pf.close()
                except ValueError:
                    print("Failed to parse pid from pidfile %s" % pidfile)
                    pf.close()
                    try:
                        print("removing corrupted pidfile %s" % pidfile)
                        os.unlink(pidfile)
                    except IOError:
                        print("Could not remove pidfile %s" % pidfile)
                    raise SystemExit(1)
                except IOError:
                    print("Could not read pidfile %s" % pidfile)
                    raise SystemExit(1)
                if _process_alive(pid):
                    print("%s (instance %s) is already running with pid %d" %
                          (program, instance, pid))
                    raise SystemExit(1)
                else:
                    print("Removing stale pidfile %s" % pidfile)
                    try:
                        os.unlink(pidfile)
                    except IOError:
                        print("Could not remove pidfile %s" % pidfile)
            # Try to create the PID directory
            else:
                if not os.path.exists(settings["PID_DIR"]):
                    try:
                        os.makedirs(settings["PID_DIR"])
                    except OSError as exc:  # Python >2.5
                        if exc.errno == errno.EEXIST and os.path.isdir(settings["PID_DIR"]):
                           pass
                        else:
                           raise

            print("Starting %s (instance %s)" % (program, instance))

        else:
            print("Invalid action '%s'" % action)
            print("Valid actions: start stop status")
            raise SystemExit(1)


class CarbonAggregatorOptions(CarbonCacheOptions):

    optParameters = [
        ["rules", "", None, "Use the given aggregation rules file."],
        ["rewrite-rules", "", None, "Use the given rewrite rules file."],
    ] + CarbonCacheOptions.optParameters

    def postOptions(self):
        CarbonCacheOptions.postOptions(self)
        if self["rules"] is None:
            self["rules"] = join(settings["CONF_DIR"], settings['AGGREGATION_RULES'])
        settings["aggregation-rules"] = self["rules"]

        if self["rewrite-rules"] is None:
            self["rewrite-rules"] = join(settings["CONF_DIR"],
                                         settings['REWRITE_RULES'])
        settings["rewrite-rules"] = self["rewrite-rules"]


class CarbonRelayOptions(CarbonCacheOptions):

    optParameters = [
        ["rules", "", None, "Use the given relay rules file."],
        ["aggregation-rules", "", None, "Use the given aggregation rules file."],
    ] + CarbonCacheOptions.optParameters

    def postOptions(self):
        CarbonCacheOptions.postOptions(self)
        if self["rules"] is None:
            self["rules"] = join(settings["CONF_DIR"], settings['RELAY_RULES'])
        settings["relay-rules"] = self["rules"]

        if self["aggregation-rules"] is None:
            self["aggregation-rules"] = join(settings["CONF_DIR"], settings['AGGREGATION_RULES'])
        settings["aggregation-rules"] = self["aggregation-rules"]

        router = settings["RELAY_METHOD"]
        if router not in DatapointRouter.plugins:
            print("In carbon.conf, RELAY_METHOD must be one of %s. "
                  "Invalid value: '%s'" % (', '.join(DatapointRouter.plugins), router))
            raise SystemExit(1)


def get_default_parser(usage="%prog [options] <start|stop|status>"):
    """Create a parser for command line options."""
    parser = OptionParser(usage=usage)
    parser.add_option(
        "--debug", action="store_true",
        help="Run in the foreground, log to stdout")
    parser.add_option(
        "--syslog", action="store_true",
        help="Write logs to syslog")
    parser.add_option(
        "--nodaemon", action="store_true",
        help="Run in the foreground")
    parser.add_option(
        "--profile",
        help="Record performance profile data to the given file")
    parser.add_option(
        "--profiler",
        help="Specify the profiler to use")
    parser.add_option(
        "--pidfile", default=None,
        help="Write pid to the given file")
    parser.add_option(
        "--umask", default=None,
        help="Use the given umask when creating files")
    parser.add_option(
        "--config",
        default=None,
        help="Use the given config file")
    parser.add_option(
      "--whitelist",
      default=None,
      help="Use the given whitelist file")
    parser.add_option(
      "--blacklist",
      default=None,
      help="Use the given blacklist file")
    parser.add_option(
        "--logdir",
        default=None,
        help="Write logs in the given directory")
    parser.add_option(
        "--instance",
        default='a',
        help="Manage a specific carbon instance")
    parser.add_option(
        "--logfile",
        default=None,
        help="Log to a specified file, - for stdout")
    parser.add_option(
        "--logger",
        default=None,
        help="A fully-qualified name to a log observer factory to use for the initial log "
             "observer. Takes precedence over --logfile and --syslog (when available).")
    return parser


def get_parser(name):
    parser = get_default_parser()
    if "carbon-aggregator" in name:
        parser.add_option(
            "--rules",
            default=None,
            help="Use the given aggregation rules file.")
        parser.add_option(
            "--rewrite-rules",
            default=None,
            help="Use the given rewrite rules file.")
    elif name == "carbon-relay":
        parser.add_option(
            "--rules",
            default=None,
            help="Use the given relay rules file.")
    return parser


def parse_options(parser, args):
    """
    Parse command line options and print usage message if no arguments were
    provided for the command.
    """
    (options, args) = parser.parse_args(args)

    if not args:
        parser.print_usage()
        raise SystemExit(1)

    if args[0] not in ("start", "stop", "status"):
        parser.print_usage()
        raise SystemExit(1)

    return options, args


def read_config(program, options, **kwargs):
    """
    Read settings for 'program' from configuration file specified by
    'options["config"]', with missing values provided by 'defaults'.
    """
    settings = Settings()
    settings.update(defaults)

    # Initialize default values if not set yet.
    for name, value in kwargs.items():
        settings.setdefault(name, value)

    graphite_root = kwargs.get("ROOT_DIR")
    if graphite_root is None:
        graphite_root = os.environ.get('GRAPHITE_ROOT')
    if graphite_root is None:
        raise CarbonConfigException("Either ROOT_DIR or GRAPHITE_ROOT "
                                    "needs to be provided.")

    # Default config directory to root-relative, unless overridden by the
    # 'GRAPHITE_CONF_DIR' environment variable.
    settings.setdefault("CONF_DIR",
                        os.environ.get("GRAPHITE_CONF_DIR",
                                       join(graphite_root, "conf")))
    if options["config"] is None:
        options["config"] = join(settings["CONF_DIR"], "carbon.conf")
    else:
        # Set 'CONF_DIR' to the parent directory of the 'carbon.conf' config
        # file.
        settings["CONF_DIR"] = dirname(normpath(options["config"]))

    # Storage directory can be overridden by the 'GRAPHITE_STORAGE_DIR'
    # environment variable. It defaults to a path relative to GRAPHITE_ROOT
    # for backwards compatibility though.
    settings.setdefault("STORAGE_DIR",
                        os.environ.get("GRAPHITE_STORAGE_DIR",
                                       join(graphite_root, "storage")))

    def update_STORAGE_DIR_deps():
        # By default, everything is written to subdirectories of the storage dir.
        settings.setdefault(
            "PID_DIR", settings["STORAGE_DIR"])
        settings.setdefault(
            "LOG_DIR", join(settings["STORAGE_DIR"], "log", program))
        settings.setdefault(
            "LOCAL_DATA_DIR", join(settings["STORAGE_DIR"], "whisper"))
        settings.setdefault(
            "WHITELISTS_DIR", join(settings["STORAGE_DIR"], "lists"))

    # Read configuration options from program-specific section.
    section = program[len("carbon-"):]
    config = options["config"]

    if not exists(config):
        raise CarbonConfigException("Error: missing required config %r" % config)

    settings.readFrom(config, section)
    settings.setdefault("instance", options["instance"])
    update_STORAGE_DIR_deps()

    # If a specific instance of the program is specified, augment the settings
    # with the instance-specific settings and provide sane defaults for
    # optional settings.
    if options["instance"]:
        settings.readFrom(config,
                          "%s:%s" % (section, options["instance"]))
        settings["pidfile"] = (
            options["pidfile"] or
            join(settings["PID_DIR"], "%s-%s.pid" % (program, options["instance"])))
        settings["LOG_DIR"] = (
            options["logdir"] or
            join(settings["LOG_DIR"], "%s-%s" % (program, options["instance"])))
    else:
        settings["pidfile"] = (
            options["pidfile"] or join(settings["PID_DIR"], '%s.pid' % program))
        settings["LOG_DIR"] = (options["logdir"] or settings["LOG_DIR"])

    update_STORAGE_DIR_deps()
    return settings
