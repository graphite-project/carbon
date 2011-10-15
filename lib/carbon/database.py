#TODO(chrismd): unify this with the webapp's storage plugin system after the 1.1 merge

import os
from os.path import join, exists, dirname


class PluginRegistrar(type):
  """Clever subclass detection hack makes finding user-defined
  database implementations trivial."""
  def __init__(classObj, name, bases, members):
    super(PluginRegistrar, classObj).__init__(name, bases, members)
    if hasattr(classObj, 'plugin_name'):
      classObj.plugins[classObj.plugin_name] = classObj


class TimeSeriesDatabase(object):
  "Abstract base class for Carbon database backends"
  __metaclass__ = PluginRegistrar
  plugins = {}

  #def read(self, metric, start_time, end_time):
  #  after 1.1 merge

  def write(self, metric, datapoints):
    "Persist datapoints in the database for metric"
    raise NotImplemented()

  def exists(self, metric):
    "Return True if the given metric path exists, False otherwise."
    raise NotImplemented()

  def create(self, metric, **options):
    "Create an entry in the database for metric using options"
    raise NotImplemented()

  def get_metadata(self, metric, key):
    "Lookup metric metadata"
    raise NotImplemented()

  def set_metadata(self, metric, key, value):
    "Modify metric metadata"
    raise NotImplemented()


# "native" plugins below
try:
  import whisper
except ImportError:
  pass
else:
  class WhisperDatabase(TimeSeriesDatabase):
    plugin_name = 'whisper'

    def __init__(self, settings):
      self.settings = settings
      self.data_dir = settings['LOCAL_DATA_DIR']
      whisper.AUTOFLUSH = settings['whisper'].get('AUTOFLUSH', False)

    def _get_filesystem_path(self, metric):
      return join(self.data_dir, *metric.split('.')) + '.wsp'

    def write(self, metric, datapoints):
      path = self._get_filesystem_path(metric)
      whisper.update_many(path, datapoints)

    def exists(self, metric):
      return exists(self._get_filesystem_path(metric))

    def create(self, metric, **options):
      path = self._get_filesystem_path(metric)
      directory = dirname(path)
      os.system("mkdir -p -m 755 '%s'" % directory)

      # convert argument naming convention
      options['archiveList'] = options.pop('retentions')
      options['xFilesFactor'] = options.pop('xfilesfactor')
      options['aggregationMethod'] = options.pop('aggregation-method')

      whisper.create(path, **options)
      os.chmod(path, 0755)

    def get_metadata(self, metric, key):
      if key != 'aggregationMethod':
        raise ValueError("Whisper only supports the 'aggregationMethod' metadata key,"
                         " invalid key: " + key)
      path = self._get_filesystem_path(metric)
      return whisper.info(path)['aggregationMethod']

    def set_metadata(self, metric, key, value):
      if key != 'aggregationMethod':
        raise ValueError("Whisper only supports the 'aggregationMethod' metadata key,"
                         " invalid key: " + key)
      path = self._get_filesystem_path(metric)
      return whisper.setAggregationMethod(path, value)



try:
  import ceres
except ImportError:
  pass
else:
  class CeresDatabase(TimeSeriesDatabase):
    plugin_name = 'ceres'

    def __init__(self, settings):
      self.settings = settings
      self.data_dir = settings['LOCAL_DATA_DIR']
      self.tree = ceres.CeresTree(self.data_dir)
      ceres_settings = settings['ceres']
      behavior = ceres_settings.get('DEFAULT_SLICE_CACHING_BEHAVIOR')
      if behavior:
        ceres.setDefaultSliceCachingBehavior(behavior)
      if 'MAX_SLICE_GAP' in ceres_settings:
        ceres.MAX_SLICE_GAP = int(ceres_settings['MAX_SLICE_GAP'])

    def write(self, metric, datapoints):
      self.tree.store(metric, datapoints)

    def exists(self, metric):
      return self.tree.hasNode(metric)

    def create(self, metric, **options):
      # convert argument naming convention
      options['retentions'] = options.pop('retentions')
      options['timeStep'] = options['retentions'][0][0]
      options['xFilesFactor'] = options.pop('xfilesfactor')
      options['aggregationMethod'] = options.pop('aggregation-method')
      self.tree.createNode(metric, **options)

    def get_metadata(self, metric, key):
      return self.tree.getNode(metric).readMetadata()[key]

    def set_metadata(self, metric, key, value):
      node = self.tree.getNode(metric)
      metadata = node.readMetadata()
      metadata[key] = value
      node.writeMetadata(metadata)
