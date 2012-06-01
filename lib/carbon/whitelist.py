from carbon import instrumentation
from carbon.pipeline import Processor
from carbon.regexlist import RegexList

instrumentation.configure_counters([
  'whitelist.datapoints_accepted',
  'whitelist.datapoints_rejected',
  ])

class WhitelistProcessor(Processor):
  plugin_name = 'whitelist'

  def process(self, metric, datapoint):
    if BlackList and metric in BlackList:
      instrumentation.increment('whitelist.datapoints_rejected')
      return
    if WhiteList and metric not in WhiteList:
      instrumentation.increment('whitelist.datapoints_accepted')
      return
    yield (metric, datapoint)

WhiteList = RegexList()
BlackList = RegexList()
