"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import os, re

from os.path import join, exists
from carbon.util import pickle, parseRetentionDefs
from carbon.conf import ConfigError


class StorageRule(object):
  def __init__(self, definition):
    self.definition = dict(definition) # preserve original definition
    # separate out any condition keys
    match_all = definition.pop('match-all', None)
    pattern = definition.pop('pattern', None)
    metric_list = definition.pop('list', None)
    if [match_all, pattern, metric_list].count(None) != 2:
      raise ConfigError("Exactly one condition key must be provided: match-all"
                        " | pattern | list")

    if match_all:
      self.test = lambda metric: True
    elif pattern:
      regex = re.compile(pattern)
      self.test = lambda metric: regex.search(metric)
    elif metric_list:
      list_checker = ListChecker(metric_list)
      self.test = lambda metric: list_checker.check(metric)

    if 'retentions' in definition:
      definition['retentions'] = parseRetentionDefs(definition['retentions'])

    self.context = definition # only context remains

  def set_defaults(self, context):
    for key, value in self.context.items():
      context.setdefault(key, value)

  def matches(self, metric):
    return bool(self.test(metric))


class ListChecker(object):
  def __init__(self, list_name):
    self.list_name = list_name
    self.path = join(settings.WHITELISTS_DIR, list_name)

    if exists(self.path):
      self.mtime = os.stat(self.path).st_mtime
      fh = open(self.path, 'rb')
      self.members = pickle.load(fh)
      fh.close()

    else:
      self.mtime = 0
      self.members = frozenset()

  def check(self, metric):
    if exists(self.path):
      current_mtime = os.stat(self.path).st_mtime

      if current_mtime > self.mtime:
        self.mtime = current_mtime
        fh = open(self.path, 'rb')
        self.members = pickle.load(fh)
        fh.close()

    return metric in self.members

