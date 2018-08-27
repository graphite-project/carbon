# -*- coding: utf-8 -*-

from unittest import TestCase
from os import path
from twisted.python.log import addObserver, removeObserver
from carbon import log

try:
    from tempfile import TemporaryDirectory
except ImportError:
    from backports.tempfile import TemporaryDirectory


class CarbonLogFileTest(TestCase):

    def test_write_to_logfile(self):
        with TemporaryDirectory() as tmpdir:
            o = log.CarbonLogObserver()
            o.log_to_dir(tmpdir)
            addObserver(o)
            log.creates('😀😀😀😀 test !!!!')
            removeObserver(o)

            with open(path.join(tmpdir, 'creates.log')) as logfile:
                read_line = logfile.readline()
                self.assertRegexpMatches(read_line, '.*😀😀😀😀 test !!!!')
