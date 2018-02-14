import os
from unittest import TestCase
from mock import patch
from os.path import exists
import shutil

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
        metric = 'stats.example.counts;tag1=value1'

        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = False
        database = WhisperDatabase(settings)

        result = database.getFilesystemPath(metric)
        self.assertEqual(
            result, '/tmp/_tagged/872/252/stats_DOT_example_DOT_counts;tag1=value1.wsp')  # nosec

        result = database.exists(metric)
        self.assertEqual(result, False)

    def test_getTaggedFilesystemPathHashed(self):
        metric = 'stats.example.counts;tag1=value1'

        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = True
        database = WhisperDatabase(settings)

        result = database.getFilesystemPath(metric)
        self.assertEqual(
            result,
            '/tmp/_tagged/872/252/' +  # nosec
            '872252dcead671982862f82a3b440f02aa8f525dd6d0f2921de0dc2b3e874ad0.wsp')

        result = database.exists(metric)
        self.assertEqual(result, False)

    def test_migrateTaggedFilesystemPathHashed(self):
        metric = 'stats.example.counts;tag1=value1'

        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = False
        database = WhisperDatabase(settings)

        result = database.exists(metric)
        self.assertEqual(result, False)

        old_path = database.getFilesystemPath(metric)
        self.assertEqual(
            old_path, '/tmp/_tagged/872/252/stats_DOT_example_DOT_counts;tag1=value1.wsp')  # nosec

        self.assertEqual(exists(old_path), False)

        result = database.create(metric, [(60, 60)], 0.5, 'average')

        self.assertEqual(exists(old_path), True)

        result = database.exists(metric)
        self.assertEqual(result, True)

        settings['TAG_HASH_FILENAMES'] = True
        database = WhisperDatabase(settings)

        hashed_path = database.getFilesystemPath(metric)
        self.assertEqual(
            hashed_path,
            '/tmp/_tagged/872/252/' +  # nosec
            '872252dcead671982862f82a3b440f02aa8f525dd6d0f2921de0dc2b3e874ad0.wsp')

        self.assertEqual(exists(hashed_path), False)

        result = database.exists(metric)
        self.assertEqual(result, True)

        self.assertEqual(exists(old_path), False)
        self.assertEqual(exists(hashed_path), True)

        os.remove(hashed_path)


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
        metric = 'stats.example.counts;tag1=value1'

        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = False
        database = CeresDatabase(settings)

        result = database.getFilesystemPath(metric)
        self.assertEqual(
            result, '/tmp/_tagged/872/252/stats_DOT_example_DOT_counts;tag1=value1')  # nosec

        result = database.exists(metric)
        self.assertEqual(result, False)

    def test_getTaggedFilesystemPathHashed(self):
        metric = 'stats.example.counts;tag1=value1'

        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = True
        database = CeresDatabase(settings)

        result = database.getFilesystemPath(metric)
        self.assertEqual(
            result,
            '/tmp/_tagged/872/252/' +  # nosec
            '872252dcead671982862f82a3b440f02aa8f525dd6d0f2921de0dc2b3e874ad0')

        result = database.exists(metric)
        self.assertEqual(result, False)

    def test_migrateTaggedFilesystemPathHashed(self):
        metric = 'stats.example.counts;tag1=value1'

        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        settings['TAG_HASH_FILENAMES'] = False
        database = CeresDatabase(settings)

        result = database.exists(metric)
        self.assertEqual(result, False)

        old_path = database.getFilesystemPath(metric)
        self.assertEqual(
            old_path, '/tmp/_tagged/872/252/stats_DOT_example_DOT_counts;tag1=value1')  # nosec

        self.assertEqual(exists(old_path), False)

        result = database.create(metric, [(60, 60)], 0.5, 'average')

        self.assertEqual(exists(old_path), True)

        result = database.exists(metric)
        self.assertEqual(result, True)

        settings['TAG_HASH_FILENAMES'] = True
        database = CeresDatabase(settings)

        hashed_path = database.getFilesystemPath(metric)
        self.assertEqual(
            hashed_path,
            '/tmp/_tagged/872/252/' +  # nosec
            '872252dcead671982862f82a3b440f02aa8f525dd6d0f2921de0dc2b3e874ad0')

        self.assertEqual(exists(hashed_path), False)

        result = database.exists(metric)
        self.assertEqual(result, True)

        self.assertEqual(exists(old_path), False)
        self.assertEqual(exists(hashed_path), True)

        shutil.rmtree(hashed_path)
