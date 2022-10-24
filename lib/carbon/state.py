__doc__ = """
This module exists for the purpose of tracking global state used across
several modules.
"""

metricReceiversPaused = False
cacheTooFull = False
client_manager = None
connectedMetricReceiverProtocols = set()
pipeline_processors = []
pipeline_processors_generated = []
database = None
listeningPorts = []
