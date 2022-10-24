import os
import time
from sys import stdout
from zope.interface import implementer
from twisted.python.log import startLoggingWithObserver, textFromEventDict, msg, err, ILogObserver  # NOQA
from twisted.python.syslog import SyslogObserver
from twisted.python.logfile import DailyLogFile


class CarbonLogFile(DailyLogFile):
  """Overridden to support logrotate.d"""
  def __init__(self, *args, **kwargs):
    DailyLogFile.__init__(self, *args, **kwargs)
    # avoid circular dependencies
    from carbon.conf import settings
    self.enableRotation = settings.ENABLE_LOGROTATION

  def _openFile(self):
    """
    Fix Umask Issue https://twistedmatrix.com/trac/ticket/7026
    """
    openMode = self.defaultMode or 0o777
    self._file = os.fdopen(os.open(
      self.path, os.O_CREAT | os.O_RDWR, openMode), 'rb+', 1)
    self.closed = False
    # Try our best to update permissions for files which already exist.
    if self.defaultMode:
      try:
        os.chmod(self.path, self.defaultMode)
      except OSError:
        pass
    # Seek is needed for uniformity of stream positioning
    # for read and write between Linux and BSD systems due
    # to differences in fopen() between operating systems.
    self._file.seek(0, os.SEEK_END)
    self.lastDate = self.toDate(os.stat(self.path)[8])

  def shouldRotate(self):
    if self.enableRotation:
      return DailyLogFile.shouldRotate(self)
    else:
      return False

  def write(self, data):
    if not self.enableRotation:
      if not os.path.exists(self.path):
        self.reopen()
      else:
        path_stat = os.stat(self.path)
        fd_stat = os.fstat(self._file.fileno())
        if not (path_stat.st_ino == fd_stat.st_ino and path_stat.st_dev == fd_stat.st_dev):
          self.reopen()
    DailyLogFile.write(self, data)

  # Backport from twisted >= 10
  def reopen(self):
    self.close()
    self._openFile()


@implementer(ILogObserver)
class CarbonLogObserver(object):

  def __init__(self):
    self._raven_client = None

  def raven_client(self):
    if self._raven_client is not None:
      return self._raven_client

    # Import here to avoid dependency hell.
    try:
      import raven
    except ImportError:
      return None
    from carbon.conf import settings

    if settings.RAVEN_DSN is None:
      return None
    self._raven_client = raven.Client(dsn=settings.RAVEN_DSN)
    return self._raven_client

  def log_to_raven(self, event):
    if not event.get('isError') or 'failure' not in event:
      return
    client = self.raven_client()
    if client is None:
      return
    f = event['failure']
    client.captureException(
      (f.type, f.value, f.getTracebackObject())
    )

  def log_to_dir(self, logdir):
    self.logdir = logdir
    self.console_logfile = CarbonLogFile('console.log', logdir)
    self.custom_logs = {}
    self.observer = self.logdir_observer

  def log_to_syslog(self, prefix):
    observer = SyslogObserver(prefix).emit

    def syslog_observer(event):
      event["system"] = event.get("type", "console")
      observer(event)
    self.observer = syslog_observer

  def __call__(self, event):
    self.log_to_raven(event)
    return self.observer(event)

  @staticmethod
  def stdout_observer(event):
    stdout.write(formatEvent(event, includeType=True) + '\n')
    stdout.flush()

  def logdir_observer(self, event):
    message = formatEvent(event)
    log_type = event.get('type')

    if log_type is not None and log_type not in self.custom_logs:
      self.custom_logs[log_type] = CarbonLogFile(log_type + '.log', self.logdir)

    logfile = self.custom_logs.get(log_type, self.console_logfile)
    logfile.write(message + '\n')
    logfile.flush()

  # Default to stdout
  observer = stdout_observer


carbonLogObserver = CarbonLogObserver()


def formatEvent(event, includeType=False):
  event['isError'] = 'failure' in event
  message = textFromEventDict(event)

  if includeType:
    typeTag = '[%s] ' % event.get('type', 'console')
  else:
    typeTag = ''

  timestamp = time.strftime("%d/%m/%Y %H:%M:%S")
  return "%s :: %s%s" % (timestamp, typeTag, message)


logToDir = carbonLogObserver.log_to_dir

logToSyslog = carbonLogObserver.log_to_syslog


def logToStdout():
  startLoggingWithObserver(carbonLogObserver)


def cache(message, **context):
  context['type'] = 'cache'
  msg(message, **context)


def clients(message, **context):
  context['type'] = 'clients'
  msg(message, **context)


def creates(message, **context):
  context['type'] = 'creates'
  msg(message, **context)


def updates(message, **context):
  context['type'] = 'updates'
  msg(message, **context)


def listener(message, **context):
  context['type'] = 'listener'
  msg(message, **context)


def relay(message, **context):
  context['type'] = 'relay'
  msg(message, **context)


def aggregator(message, **context):
  context['type'] = 'aggregator'
  msg(message, **context)


def query(message, **context):
  context['type'] = 'query'
  msg(message, **context)


def debug(message, **context):
  if debugEnabled:
    msg(message, **context)


debugEnabled = False


def setDebugEnabled(enabled):
  global debugEnabled
  debugEnabled = enabled
