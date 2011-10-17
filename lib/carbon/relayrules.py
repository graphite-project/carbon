import re
from carbon.conf import settings, ConfigError
from carbon.util import parseDestinations


class RelayRule:
  def __init__(self, condition, destinations):
    self.condition = condition
    self.destinations = destinations

  def matches(self, metric):
    return bool( self.condition(metric) )


def loadRelayRules(config_file):
  rules = []
  rule_definitions = settings.read_file(config_file, store=False)

  default_rule = None
  for rule_name, definition in rule_definitions.items():
    if 'destinations' not in definition:
      raise ConfigError("Relay rule [" + rule_name + "] missing required "
                        "\"destinations\" key")

    destination_strings = definition['destinations'].split(',')
    destinations = parseDestinations(destination_strings)

    if 'pattern' in definition:
      if 'default' in definition:
        raise ConfigError("Rule " + rule_name + " contains both 'pattern' "
                          "and 'default' keys. You must use one or the other.")

      regex = re.compile(definition['pattern'], re.I)
      rule = RelayRule(condition=regex.search, destinations=destinations)
      rules.append(rule)
      continue

    elif 'default' in definition:
      if not definition['default']:
        continue # just ignore default = false
      if default_rule:
        raise ConfigError("Default rule already defined.")
      default_rule = RelayRule(condition=lambda metric: True,
                               destinations=destinations)

  if not default_rule:
    raise Exception("No default rule defined. You must specify exactly one "
                    "rule with 'default = true' instead of a pattern.")

  rules.append(default_rule)
  return rules
