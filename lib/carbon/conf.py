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
import re

from os.path import join, basename, dirname, normpath, exists, isdir
from glob import glob

from carbon.storage import StorageRule
from carbon.database import TimeSeriesDatabase
from carbon import log, util, state

from twisted.python import usage


LISTENER_TYPES = (
  'plaintext-receiver',
  'pickle-receiver',
)

defaults = dict(
  # aggregation.conf
  MAX_AGGREGATION_INTERVALS=5,
  AGGREGATION_FREQUENCY_MULTIPLIER=1,
  ENABLE_AGGREGATION_FILTERING=False,

  # amqp.conf
  ENABLE_AMQP=False,
  AMQP_HOST='localhost',
  AMQP_PORT=5672,
  AMQP_USER='guest',
  AMQP_PASSWORD='guest',
  AMQP_VHOST='/',
  AMQP_EXCHANGE='graphite',
  AMQP_METRIC_NAME_IN_BODY=False,
  AMQP_VERBOSE=False,
  BIND_PATTERNS='#',

  # daemon.conf
  USER='',
  PIPELINE=[],
  USE_INSECURE_UNPICKLER=False,

  # db.conf
  DATABASE='whisper',
  LOCAL_DATA_DIR='/opt/graphite/storage/whisper',

  # listeners.conf
  LISTENERS=[],

  # management.conf
  CARBON_METRIC_PREFIX='carbon',
  CARBON_METRIC_INTERVAL=60,
  ENABLE_MANHOLE=False,
  MANHOLE_INTERFACE='127.0.0.1',
  MANHOLE_PORT= 7022,
  MANHOLE_USER='admin',
  MANHOLE_PUBLIC_KEY=None,

  # relay.conf
  DESTINATIONS=[],
  MAX_DATAPOINTS_PER_MESSAGE=500,
  MAX_QUEUE_SIZE=10000,
  RELAY_METHOD='rules',
  REPLICATION_FACTOR=1,
  USE_FLOW_CONTROL=True,

  # writer.conf
  MAX_CACHE_SIZE=2000000,
  MAX_WRITES_PER_SECOND=600,
  MAX_CREATES_PER_MINUTE=50,
  LOG_WRITES=True,
  CACHE_QUERY_PORT=7002,
  CACHE_QUERY_INTERFACE='0.0.0.0',
  WHITELISTS_DIR='/opt/graphite/storage/lists',
)


class CarbonConfiguration(dict):
  def __init__(self):
    self.update(defaults)
    self.config_dir = None

  def __getitem__(self, key):
    if key in self:
      return dict.__getitem__(self, key)
    else:
      raise ConfigError("Missing expected configuration \"%s\"" % key)

  def __setitem__(self, key, value):
    dict.__setitem__(self, key, value)

  def __getattribute__(self, attr):
    try:
      return object.__getattribute__(self, attr)
    except AttributeError:
      return self[attr]

  def use_config_directory(self, config_dir):
    self.config_dir = config_dir
    config_paths = glob(join(config_dir, '*.conf'))
    self.config_files = [basename(path) for path in config_paths]

  def file_exists(self, filename):
    if self.config_dir is None:
      raise ConfigError("use_config_directory() has not been called yet")
    return filename in self.config_files

  def get_path(self, filename):
    if self.config_dir is None:
      raise ConfigError("use_config_directory() has not been called yet")

    path = join(self.config_dir, filename)
    if not exists(path):
      raise ConfigError("No such file %s" % path)
    return path
 
  def read_file(self, filename, ordered_items=False, store=True):
    path = self.get_path(filename)
    settings = {}
    names = [] # keep track of order of global names
    context = settings # start out in the global context

    # Parsing logic
    for line in open(path):
      line = line.strip()

      if line.startswith('#') or not line:
        continue

      if line.startswith('[') and line.endswith(']'):
        context_name = line[1:-1]
        names.append(context_name)
        context = settings[context_name] = {}
        continue

      elif '=' in line:
        key, value = line.split('=', 1)
        key, value = key.strip(), value.strip()

        if context is settings: # globals
          names.append(key)

        if key in defaults:
          valueType = type(defaults[key])
        else:
          valueType = str

        if valueType is list:
          value = [v.strip() for v in value.split(',')]

        elif valueType is bool:
          if value.lower() == 'true':
            value = True
          elif value.lower() == 'false':
            value = False
          else:
            raise ConfigError("%s must be either \"true\" or \"false\"" % key, filename=filename)

        elif valueType in (int, long, float):
          value = valueType(value)

        context[key] = value

      else:
        raise ConfigError("Invalid line: %s" % line, filename=filename)

    # Absorb settings from this file
    if store:
      self.update(settings)

    # Return only what this file defined
    if ordered_items:
      return [(name, settings[name]) for name in names]
    else:
      return settings

  def read_filters(self, filename):
    path = self.get_path(filename)
    filters = []
    for line in open(path):
      line = line.strip()
      if line.startswith('#') or not line:
        continue
      try:
        action, regex_pattern = line.split(' ', 1)
      except:
        raise ConfigError("Invalid filter line: %s" % line)
      else:
        filters.append( Filter(action, regex_pattern) )

    return filters


class Filter(object):
  def __init__(self, action, regex_pattern):
    if action not in ('include', 'exclude'):
      raise ValueError("Invalid filter action '%s'" % action)
    self.action = action
    self.regex = re.compile(regex_pattern)

  def allow(self, metric):
    matches = bool(self.regex.search(metric))
    if self.action == 'include':
      return matches
    else:
      return not matches


# The global settings singleton
settings = CarbonConfiguration()
state.settings = settings


class ConfigError(Exception):
  def __init__(self, message, filename=None):
    if filename:
      self.message = "%s: %s" % (filename, message)
    else:
      self.message = message
    self.filename = filename

  def __repr__(self):
    return '<ConfigError(%s)>' % self.message
  __str__ = __repr__



class CarbonDaemonOptions(usage.Options):
    optFlags = [
        ["debug", "", "Run in debug mode."],
        ]

    optParameters = [
        ["config", "c", None, "Use configs from the given directory"],
        ["logdir", "", None, "Write logs to the given directory."],
        ]

    def postOptions(self):
        # Use provided pidfile (if any) as default for configuration. If it's
        # set to 'twistd.pid', that means no value was provided and the default
        # was used.
        pidfile = self.parent["pidfile"]
        if pidfile.endswith("twistd.pid"):
            pidfile = None
        self["pidfile"] = pidfile

        # Enforce a default umask of '022' if none was set.
        if not self.parent.has_key("umask") or self.parent["umask"] is None:
            self.parent["umask"] = 022

        # Read extra settings from the configuration file.
        read_configs(self['instance'], self)

        # Set process uid/gid by changing the parent config, if a user was
        # provided in the configuration file.
        if settings.USER:
            self.parent["uid"], self.parent["gid"] = (
                pwd.getpwnam(settings.USER)[2:4])

        # Set the pidfile in parent config to the value that was computed by
        # C{read_configs}.
        self.parent["pidfile"] = settings["pidfile"]

        if not "action" in self:
            self["action"] = "start"
        self.handleAction()

        # If we are not running in debug mode or non-daemon mode, then log to a
        # directory, otherwise log output will go to stdout.
        if not self["debug"]:
            if self.parent.get("syslog", None):
                log.logToSyslog(self.parent["prefix"])
            elif not self.parent["nodaemon"]:
                logdir = settings.LOG_DIR
                if not isdir(logdir):
                    os.makedirs(logdir)
                log.logToDir(logdir)

    def parseArgs(self, *args):
        if len(args) == 2:
            self["instance"] = args[0]
            self["action"] = args[1]

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
        instance = self["instance"]

        if action == "stop":
            if not exists(pidfile):
                print "Pidfile %s does not exist" % pidfile
                raise SystemExit(0)
            pf = open(pidfile, 'r')
            try:
                pid = int(pf.read().strip())
                pf.close()
            except:
                print "Could not read pidfile %s" % pidfile
                raise SystemExit(1)
            print "Sending kill signal to pid %d" % pid
            try:
                os.kill(pid, 15)
            except OSError, e:
                if e.errno == errno.ESRCH:
                    print "No process with pid %d running" % pid
                else:
                    raise

            raise SystemExit(0)

        elif action == "status":
            if not exists(pidfile):
                print "carbon-daemon %s is not running" % instance
                raise SystemExit(1)
            pf = open(pidfile, "r")
            try:
                pid = int(pf.read().strip())
                pf.close()
            except:
                print "Failed to read pid from %s" % pidfile
                raise SystemExit(1)

            if _process_alive(pid):
                print ("carbon-daemon %s is running with pid %d" %
                       (instance, pid))
                raise SystemExit(0)
            else:
                print "carbon-daemon %s is not running" % instance
                raise SystemExit(1)

        elif action == "start":
            if exists(pidfile):
                pf = open(pidfile, 'r')
                try:
                    pid = int(pf.read().strip())
                    pf.close()
                except:
                    print "Could not read pidfile %s" % pidfile
                    raise SystemExit(1)
                if _process_alive(pid):
                    print ("Carbon instance '%s' is already running with pid %d" %
                           (instance, pid))
                    raise SystemExit(1)
                else:
                    print "Removing stale pidfile %s" % pidfile
                    try:
                        os.unlink(pidfile)
                    except:
                        print "Could not remove pidfile %s" % pidfile
        else:
            print "Invalid action '%s'" % action
            print "Valid actions: start stop status"
            raise SystemExit(1)


def read_configs(instance, options):
    """
    Read settings for 'instance' from configuration dir specified by
    'options["config"]', with missing values provided by 'defaults'.
    """
    graphite_root = os.environ['GRAPHITE_ROOT']

    # Default config directory to root-relative, unless overriden by the
    # 'GRAPHITE_CONF_DIR' environment variable.
    settings.setdefault("CONF_DIR",
                        os.environ.get("GRAPHITE_CONF_DIR",
                                       join(graphite_root, "conf")))
    if options["config"] is None:
        options["config"] = join(settings["CONF_DIR"], "carbon-daemons", instance)
    else:
        # Set 'CONF_DIR' to the parent directory of the 'carbon.conf' config
        # file.
        settings["CONF_DIR"] = dirname(normpath(options["config"]))

    # Storage directory can be overriden by the 'GRAPHITE_STORAGE_DIR'
    # environment variable. It defaults to a path relative to GRAPHITE_ROOT
    # for backwards compatibility though.
    settings.setdefault("STORAGE_DIR",
                        os.environ.get("GRAPHITE_STORAGE_DIR",
                                       join(graphite_root, "storage")))

    # By default, everything is written to subdirectories of the storage dir.
    settings.setdefault(
        "PID_DIR", settings["STORAGE_DIR"])
    settings.setdefault(
        "LOG_DIR", join(settings["STORAGE_DIR"], "log", "carbon-daemons", instance))

    # Read --config options
    config_dir = options["config"]
    if not exists(config_dir):
        raise ValueError("Config directory %s does not exist" % config_dir)

    # Start reading config files
    settings.use_config_directory(config_dir)

    daemon_settings = settings.read_file('daemon.conf')
    pipeline = daemon_settings['PIPELINE']
    if not pipeline:
      raise ConfigError("Empty pipeline? You lazy bastard...")

    if pipeline.count('write') + pipeline.count('relay') != 1:
      raise ConfigError("Exactly one 'write' or 'relay' must exist "
                        "in PIPELINE", filename='daemon.conf')

    settings['LISTENERS'] = settings.read_file('listeners.conf').values()
    if not settings['LISTENERS']:
      raise ConfigError("At least one listener must be defined "
                        "in listeners.conf")

    # Apply listener defaults
    for listener in settings['LISTENERS']:
        listener['port'] = int(listener['port'])
        listener.setdefault('interface', '0.0.0.0')
        listener.setdefault('protocol', 'tcp')
        if listener['protocol'] not in ('tcp', 'udp'):
           raise ConfigError("Invalid protocol \"%s\"" % listener['protocol'])
        if listener['type'] not in LISTENER_TYPES:
            raise ConfigError("Invalid listener type \"%s\"" % listener['type'])
        if listener['protocol'] == 'udp' and listener['type'] != 'plaintext-receiver':
            raise ConfigError("UDP listeners only support type=plaintext-receiver")

    # Type-specific configs
    destiny = pipeline[-1]
    if destiny == 'write':
      settings['DAEMON_TYPE'] = 'write'
      read_writer_configs()
    elif destiny == 'relay':
      settings['DAEMON_TYPE'] = 'relay'
      read_relay_configs()
    else:
      raise ConfigError("Invalid pipeline destination \"" + destiny +
                        "\" must be \"write\" or \"relay\"")

    # Pull in optional configs
    optional_configs = [
      'aggregation.conf',
      'amqp.conf',
      'management.conf',
    ]
    for filename in optional_configs:
      if settings.file_exists(filename):
        settings.read_file(filename)

    settings["pidfile"] = (
        options["pidfile"] or
        join(settings["PID_DIR"], '%s.pid' % instance))
    settings["LOG_DIR"] = (options["logdir"] or settings["LOG_DIR"])

    return settings


def read_writer_configs():
  db_settings = settings.read_file('db.conf')
  writer_settings = settings.read_file('writer.conf')

  db = db_settings['DATABASE']

  settings['STORAGE_RULES'] = load_storage_rules(settings)
  settings['CACHE_SIZE_LOW_WATERMARK'] = settings.MAX_CACHE_SIZE * 0.95

  # Database-specific settings
  if db not in TimeSeriesDatabase.plugins:
    raise ConfigError("No database plugin implemented for '%s'" % db)

  DatabasePlugin = TimeSeriesDatabase.plugins[db]
  state.database = DatabasePlugin(settings)


def load_storage_rules(settings):
  storage_rules = settings.read_file('storage-rules.conf', ordered_items=True)
  if [k for (k,v) in storage_rules if not isinstance(v, dict)]:
    raise ConfigError("Global settings not allowed in storage-rules.conf")

  # There is almost certainly a better way to set this up.
  default_storage_rule = {
    'match-all' : 'true',
    'retentions' : '1m:1w', # minutely data for a week
    'xfilesfactor' : 0.5,
    'aggregation-method' : 'average',
  }

  if 'default' in storage_rules:
    default_storage_rule.update(storage_rules.pop('default'))

  storage_rules.append(('default', default_storage_rule))
  return [StorageRule(values) for name, values in storage_rules]


def read_relay_configs():
  relay_settings = settings.read_file('relay.conf')

  method = relay_settings['RELAY_METHOD']
  if method not in ('consistent-hashing', 'relay-rules'):
    raise ConfigError("Invalid RELAY_METHOD \"" + method + "\" must be "
                      "one of: consistent-hashing, relay-rules")

  if not relay_settings['DESTINATIONS']:
    raise ConfigError("relay.conf DESTINATIONS cannot be empty")

  settings['DESTINATIONS'] = util.parseDestinations(relay_settings['DESTINATIONS'])


def _process_alive(pid):
    if exists("/proc"):
        return exists("/proc/%d" % pid)
    else:
        try:
            os.kill(int(pid), 0)
            return True
        except OSError, err:
            return err.errno == errno.EPERM


