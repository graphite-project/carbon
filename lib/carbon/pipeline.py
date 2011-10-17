from carbon.util import PluginRegistrar
from carbon.conf import settings
from carbon import state, log


class Processor(object):
  __metaclass__ = PluginRegistrar
  plugins = {}
  NO_OUTPUT = ()

  def process(self, metric, datapoint):
    raise NotImplemented()


def run_pipeline(metric, datapoint, processors=None):
  log.msg("run_pipeline(metric=%s, datapoint=%s, processors=%s)" % (metric, datapoint, processors))
  if processors is None:
    processors = state.pipeline_processors
    log.msg("using state.pipeline_processors=%s" % str(state.pipeline_processors))
  elif not processors:
    log.msg("no remaining processors, pipeline out.")
    return

  processor = processors[0]
  log.msg("executifying processor: %s" % processor)
  try:
    for out_metric, out_datapoint in processor.process(metric, datapoint):
      log.msg("OUT metric=%s datapoint=%s" % (out_metric, out_datapoint))
      try:
        run_pipeline(out_metric, out_datapoint, processors[1:])
      except:
        log.msg("UNACCEPTABLE!!!")
        log.err()
  except:
    log.msg("UNACCEPTABLE!!!")
    log.err()
