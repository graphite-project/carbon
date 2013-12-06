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
