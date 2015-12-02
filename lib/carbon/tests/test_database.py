import os
from unittest import TestCase
from mock import patch

from carbon.tests.util import TestSettings
from carbon.database import WhisperDatabase


class WhisperDatabaseTest(TestCase):

    def setUp(self):
        self._sep_patch = patch.object(os.path, 'sep', "/")
        self._sep_patch.start()
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        self.database = WhisperDatabase(settings)

    def tearDown(self):
        self._sep_patch.stop()

    def test_getFilesystemPath(self):
        result = self.database.getFilesystemPath('stats.example.counts')
        self.assertEquals(result, '/tmp/stats/example/counts.wsp')
