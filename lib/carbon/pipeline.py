from carbon.util import PluginRegistrar
from carbon.conf import settings
from carbon import state


class Processor(object):
  __metaclass__ = PluginRegistrar
  plugins = {}
  NO_OUTPUT = ()

  def process(self, metric, datapoint):
    raise NotImplemented()


def run_pipeline(self, metric, datapoint, processors=None):
  if processors is None:
    processors = state.pipeline_processors
  elif not processors:
    return

  processor = processors[0]
  for out_metric, out_datapoint in processor(metric, datapoint):
    self.run(out_metric, out_datapoint, processors[1:])
