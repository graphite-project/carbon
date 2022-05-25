# -*- coding: utf-8 -*-
import os
import sys
from unittest import TestCase

from mock import mock_open, patch, call

from carbon.instrumentation import getMemUsage


class TestInstrumentation(TestCase):

    def test_getMemUsage(self):
        if sys.version_info[0] >= 3:
            open_call = 'builtins.open'
        else:
            open_call = '__builtin__.open'

        with patch(open_call, mock_open(read_data='1 2 1 1 0 1 0')) as m:
            page_size = os.sysconf('SC_PAGESIZE')
            usage = getMemUsage()
            m.assert_has_calls([call().__exit__(None, None, None)],
                               any_order=True)
            self.assertEqual(usage, 2 * page_size)
