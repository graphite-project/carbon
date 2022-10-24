#!/usr/bin/env python
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

import sys
import os.path

# Figure out where we're installed
BIN_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BIN_DIR)

# Make sure that carbon's 'lib' dir is in the $PYTHONPATH if we're running from
# source.
LIB_DIR = os.path.join(ROOT_DIR, "lib")
sys.path.insert(0, LIB_DIR)

from carbon.util import run_twistd_plugin  # noqa
from carbon.exceptions import CarbonConfigException  # noqa

try:
    run_twistd_plugin(__file__)
except CarbonConfigException as exc:
    raise SystemExit(str(exc))
