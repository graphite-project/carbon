import os
from unittest import TestCase
from mock import patch

from carbon.tests.util import TestSettings
from carbon.database import WhisperDatabase

# class NoConfigSchemaLoadingTest(TestCase):

#     def setUp(self):
#         settings = {
#             'CONF_DIR': '',
#         }
#         self._settings_patch = patch.dict('carbon.conf.settings', settings)
#         self._settings_patch.start()

#     def tearDown(self):
#         self._settings_patch.stop()

#     def test_loadAggregationSchemas_load_default_schema(self):
#         from carbon.storage import loadAggregationSchemas, defaultAggregation
#         schema_list = loadAggregationSchemas()
#         self.assertEquals(len(schema_list), 1)
#         schema = schema_list[0]
#         self.assertEquals(schema, defaultAggregation)

#     def test_loadStorageSchemas_raise_CarbonConfigException(self):
#         from carbon.storage import loadStorageSchemas
#         from carbon.exceptions import CarbonConfigException
#         with self.assertRaises(CarbonConfigException):
#             loadStorageSchemas()


class ExistingConfigSchemaLoadingTest(TestCase):

    def setUp(self):
        test_directory = os.path.dirname(os.path.realpath(__file__))
        settings = {
            'CONF_DIR': os.path.join(test_directory, 'data', 'conf-directory'),
        }
        self._settings_patch = patch.dict('carbon.conf.settings', settings)
        self._settings_patch.start()

    def tearDown(self):
        self._settings_patch.stop()

    def test_loadStorageSchemas_return_schemas(self):
        from carbon.storage import loadStorageSchemas, PatternSchema, Archive
        schema_list = loadStorageSchemas()
        self.assertEquals(len(schema_list), 3)
        expected = [
            PatternSchema('carbon', '^carbon\.', [Archive.fromString('60:90d')]),
            PatternSchema('default_1min_for_1day', '.*', [Archive.fromString('60s:1d')])
        ]
        for schema, expected_schema in zip(schema_list[:-1], expected):
            self.assertEquals(schema.name, expected_schema.name)
            self.assertEquals(schema.pattern, expected_schema.pattern)
            for (archive, expected_archive) in zip(schema.archives, expected_schema.archives):
                self.assertEquals(archive.getTuple(), expected_archive.getTuple())

    def test_loadStorageSchemas_return_the_default_schema_last(self):
        from carbon.storage import loadStorageSchemas, defaultSchema
        schema_list = loadStorageSchemas()
        last_schema = schema_list[-1]
        self.assertEquals(last_schema.name, defaultSchema.name)
        self.assertEquals(last_schema.archives, defaultSchema.archives)

    def test_loadAggregationSchemas_return_schemas(self):
        from carbon.storage import loadAggregationSchemas, PatternSchema
        schema_list = loadAggregationSchemas()
        self.assertEquals(len(schema_list), 5)
        expected = [
            PatternSchema('min', '\.min$', (0.1, 'min')),
            PatternSchema('max', '\.max$', (0.1, 'max')),
            PatternSchema('sum', '\.count$', (0, 'sum')),
            PatternSchema('default_average', '.*', (0.5, 'average'))
        ]
        for schema, expected_schema in zip(schema_list[:-1], expected):
            self.assertEquals(schema.name, expected_schema.name)
            self.assertEquals(schema.pattern, expected_schema.pattern)
            self.assertEquals(schema.archives, expected_schema.archives)

    def test_loadAggregationSchema_return_the_default_schema_last(self):
        from carbon.storage import loadAggregationSchemas, defaultAggregation
        schema_list = loadAggregationSchemas()
        last_schema = schema_list[-1]
        self.assertEquals(last_schema, defaultAggregation)


class getFilesystemPathTest(TestCase):

    def setUp(self):
        self._sep_patch = patch.object(os.path, 'sep', "/")
        self._sep_patch.start()
        settings = TestSettings()
        settings['LOCAL_DATA_DIR'] = '/tmp/'
        self._database_patch = patch('carbon.state.database', new=WhisperDatabase(settings))
        self._database_patch.start()

    def tearDown(self):
        self._database_patch.stop()
        self._sep_patch.stop()

    def test_getFilesystemPath(self):
        from carbon.storage import getFilesystemPath
        result = getFilesystemPath('stats.example.counts')
        self.assertEquals(result, '/tmp/stats/example/counts.wsp')
