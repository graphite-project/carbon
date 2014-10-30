import os
import unittest
from copy import copy
from mocker import MockerTestCase
from carbon import writer, log
from carbon.storage import defaultSchema, defaultAggregation

def fakeLog(exc=None):
    pass

def fakeGetFilesystemPath(path):
    def getFilesystemPath(metric):
        return os.path.join(path, '%s.wsp' % metric)
    return getFilesystemPath

class FakeWriteException(Exception): pass

def fakeWriteWhisperFile(dbFilePath, datapoints):
    raise FakeWriteException()

class FakeCache(dict):
    def counts(self):
        return [ (metric, len(datapoints)) for (metric, datapoints) in self.items() ]

class FakeSchema(MockerTestCase):
    def setUp(self):
        self._old_schemas = copy(writer.schemas)
        self._old_agg_schemas = copy(writer.agg_schemas)
        writer.schemas = [defaultSchema]
        writer.agg_schemas = [defaultAggregation]
        self._old_log_err = copy(log.err)
        # Log error message catched by 'trial' tests and makes problems
        log.err = fakeLog
        self._old_metric_cache = copy(writer.MetricCache)
        writer.MetricCache = FakeCache({'some_metric': [[1, 1]]})
        self._old_getFilesystemPath = copy(writer.getFilesystemPath)
        writer.getFilesystemPath = fakeGetFilesystemPath(self.makeDir())

    def tearDown(self):
        writer.schemas = self._old_schemas
        writer.agg_schemas = self._old_agg_schemas
        log.err = self._old_log_err
        writer.MetricCache = self._old_metric_cache
        writer.getFilesystemPath = self._old_getFilesystemPath

class CreateWhisperFileTest(FakeSchema):
    def test_create_exist_file(self):
        """ File exists, do nothing """
        self.assertTrue(writer.createWhisperFile('', '', True))

    def test_create_non_exist_file_basic(self):
        """ File not exists """
        metric_path = writer.getFilesystemPath('create_basic')
        self.assertTrue(writer.createWhisperFile('test_metric', metric_path, False))

    def test_create_file_race_condition(self):
        """ File created after check on existence """
        metric_path = writer.getFilesystemPath('create_race')
        open(metric_path, 'a').close()
        self.assertFalse(writer.createWhisperFile('test_metric', metric_path, False))

class WriteWhisperFileTest(FakeSchema):
    def test_write_non_exists_file(self):
        """ Write datapoints to non-exists file """
        metric_path = writer.getFilesystemPath('write_non_exists')
        self.assertFalse(os.path.exists(metric_path))
        self.assertFalse(writer.writeWhisperFile(metric_path, [[1,1]]))
        # file not created after write points
        self.assertFalse(os.path.exists(metric_path))

    def test_write_file(self):
        """ Write datapoints to exists file """
        metric_path = writer.getFilesystemPath('write_exists')
        self.assertTrue(writer.createWhisperFile('test_metric', metric_path, False))
        self.assertTrue(writer.writeWhisperFile(metric_path, [[1,1]]))
        self.assertTrue(os.path.exists(metric_path))

class FlushTest(FakeSchema):
    def test_flush_basic(self):
        """ Check lock releasing """
        self.assertEqual(writer._flush(), 1)
        self.assertFalse(writer.write_lock.locked())

    def test_flush_exception(self):
        """ Check lock releasing on exception """
        old_writeWhisperFile = copy(writer.writeWhisperFile)
        writer.writeWhisperFile = fakeWriteWhisperFile
        self.assertRaises(FakeWriteException, writer._flush)
        self.assertFalse(writer.write_lock.locked())
        writer.writeWhisperFile = old_writeWhisperFile

    def test_flush_wrong_prefix(self):
        """ Check exc on wrong prefix """
        self.assertRaises(AssertionError, writer._flush, ([1,2,3]))
        self.assertFalse(writer.write_lock.locked())

    def test_flush_none_prefix(self):
        """ Check none prefix """
        self.assertEqual(writer._flush(None), 1)
        self.assertFalse(writer.write_lock.locked())

    def test_flush_prefix(self):
        """ Check partially flush on test prefix """
        writer.MetricCache['test.metric'] = [[2, 2]]
        self.assertEqual(writer._flush('test.'), 1)
        self.assertFalse(writer.write_lock.locked())
        # Default metric not writter
        self.assertEqual(writer.MetricCache['some_metric'], [[1, 1]])

class writeCachedDataPointsTest(FakeSchema):
    def test_write_basic(self):
        """ Check lock releasing on write """
        writer.writeCachedDataPoints()
        self.assertFalse(writer.write_lock.locked())

    def test_write_exception(self):
        """ Check lock releasing on write """
        old_writeWhisperFile = copy(writer.writeWhisperFile)
        writer.writeWhisperFile = fakeWriteWhisperFile
        self.assertRaises(FakeWriteException, writer.writeCachedDataPoints)
        self.assertFalse(writer.write_lock.locked())
        writer.writeWhisperFile = old_writeWhisperFile
