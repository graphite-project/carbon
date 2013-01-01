__doc__ = """
This module exists for the purpose of tracking global state used across
several modules.
"""

cacheTooFull = False
client_manager = None
connectedMetricReceiverProtocols = set()
database = None
events = None
instrumentation = None
metricReceiversPaused = False
pipeline_processors = []
settings = None
