from carbon.util import PluginRegistrar
from carbon import state, log
from six import with_metaclass


class Processor(with_metaclass(PluginRegistrar, object)):
  plugins = {}
  NO_OUTPUT = ()

  def pipeline_ready(self):
    "override me if you want"

  def process(self, metric, datapoint):
    raise NotImplementedError()


def run_pipeline_generated(metric, datapoint):
  # For generated points, use a special pipeline to avoid points
  # infinitely being trapped.
  run_pipeline(metric, datapoint, state.pipeline_processors_generated)


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
