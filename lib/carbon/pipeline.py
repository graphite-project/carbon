from carbon.util import PluginRegistrar
from carbon import state, log


class Processor(object):
  __metaclass__ = PluginRegistrar
  plugins = {}
  NO_OUTPUT = ()

  def pipeline_ready(self):
    "override me if you want"

  def process(self, metric, datapoint):
    raise NotImplemented()


def run_pipeline(metric, datapoint, processors=None):
  if processors is None:
    processors = state.pipeline_processors
  elif not processors:
    return

  processor = processors[0]
  try:
    for out_metric, out_datapoint in processor.process(metric, datapoint):
      try:
        run_pipeline(out_metric, out_datapoint, processors[1:])
      except Exception:
        log.err()
  except Exception:
    log.err()
