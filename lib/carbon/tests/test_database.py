import os
from unittest import TestCase
from mock import patch

from carbon.tests.util import TestSettings
from carbon.database import WhisperDatabase, CeresDatabase


class WhisperDatabaseTest(TestCase):

    def setUp(self):
        self._sep_patch = patch.object(os.path, 'sep', "/")
        self._sep_patch.start()

    def tearDown(self):
        self._sep_patch.stop()

    def test_getFilesystemPath(self):
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        database = WhisperDatabase(settings)
        result = database.getFilesystemPath('stats.example.counts')
        self.assertEqual(result, '/tmp/stats/example/counts.wsp')  # nosec

    def test_getTaggedFilesystemPath(self):
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = False
        database = WhisperDatabase(settings)
        result = database.getFilesystemPath('stats.example.counts;tag1=value1')
        self.assertEqual(
            result, '/tmp/_tagged/872/252/stats_DOT_example_DOT_counts;tag1=value1.wsp')  # nosec

    def test_getTaggedFilesystemPathHashed(self):
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = True
        database = WhisperDatabase(settings)
        result = database.getFilesystemPath('stats.example.counts;tag1=value1')
        self.assertEqual(
            result,
            '/tmp/_tagged/872/252/' +  # nosec
            '872252dcead671982862f82a3b440f02aa8f525dd6d0f2921de0dc2b3e874ad0.wsp')


class CeresDatabaseTest(TestCase):

    def setUp(self):
        self._sep_patch = patch.object(os.path, 'sep', "/")
        self._sep_patch.start()

    def tearDown(self):
        self._sep_patch.stop()

    def test_getFilesystemPath(self):
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        database = CeresDatabase(settings)
        result = database.getFilesystemPath('stats.example.counts')
        self.assertEqual(result, '/tmp/stats/example/counts')  # nosec

    def test_getTaggedFilesystemPath(self):
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = False
        database = CeresDatabase(settings)
        result = database.getFilesystemPath('stats.example.counts;tag1=value1')
        self.assertEqual(
            result, '/tmp/_tagged/872/252/stats_DOT_example_DOT_counts;tag1=value1')  # nosec

    def test_getTaggedFilesystemPathHashed(self):
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = True
        database = CeresDatabase(settings)
        result = database.getFilesystemPath('stats.example.counts;tag1=value1')
        self.assertEqual(
            result,
            '/tmp/_tagged/872/252/' +  # nosec
            '872252dcead671982862f82a3b440f02aa8f525dd6d0f2921de0dc2b3e874ad0')
