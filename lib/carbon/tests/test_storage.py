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
#         self.assertEqual(len(schema_list), 1)
#         schema = schema_list[0]
#         self.assertEqual(schema, defaultAggregation)

#     def test_loadStorageSchemas_raise_CarbonConfigException(self):
#         from carbon.storage import loadStorageSchemas
#         from carbon.exceptions import CarbonConfigException
#         with self.assertRaises(CarbonConfigException):
#             loadStorageSchemas()


class ExistingConfigSchemaLoadingTest(TestCase):

    def setUp(self):
        test_directory = os.path.dirname(os.path.realpath(__file__))
        settings = TestSettings()
        settings['CONF_DIR'] = os.path.join(test_directory, 'data', 'conf-directory')
        settings['LOCAL_DATA_DIR'] = ''
        self._settings_patch = patch('carbon.conf.settings', settings)
        self._settings_patch.start()
        self._database_patch = patch('carbon.state.database', new=WhisperDatabase(settings))
        self._database_patch.start()

    def tearDown(self):
        self._database_patch.stop()
        self._settings_patch.stop()

    def test_loadStorageSchemas_return_schemas(self):
        from carbon.storage import loadStorageSchemas, PatternSchema, Archive
        schema_list = loadStorageSchemas()
        self.assertEqual(len(schema_list), 3)
        expected = [
            PatternSchema('carbon', r'^carbon\.', [Archive.fromString('60:90d')]),
            PatternSchema('default_1min_for_1day', '.*', [Archive.fromString('60s:1d')])
        ]
        for schema, expected_schema in zip(schema_list[:-1], expected):
            self.assertEqual(schema.name, expected_schema.name)
            self.assertEqual(schema.pattern, expected_schema.pattern)
            for (archive, expected_archive) in zip(schema.archives, expected_schema.archives):
                self.assertEqual(archive.getTuple(), expected_archive.getTuple())

    def test_loadStorageSchemas_return_the_default_schema_last(self):
        from carbon.storage import loadStorageSchemas, defaultSchema
        schema_list = loadStorageSchemas()
        last_schema = schema_list[-1]
        self.assertEqual(last_schema.name, defaultSchema.name)
        self.assertEqual(last_schema.archives, defaultSchema.archives)

    def test_loadAggregationSchemas_return_schemas(self):
        from carbon.storage import loadAggregationSchemas, PatternSchema
        schema_list = loadAggregationSchemas()
        self.assertEqual(len(schema_list), 5)
        expected = [
            PatternSchema('min', r'\.min$', (0.1, 'min')),
            PatternSchema('max', r'\.max$', (0.1, 'max')),
            PatternSchema('sum', r'\.count$', (0, 'sum')),
            PatternSchema('default_average', '.*', (0.5, 'average'))
        ]
        for schema, expected_schema in zip(schema_list[:-1], expected):
            self.assertEqual(schema.name, expected_schema.name)
            self.assertEqual(schema.pattern, expected_schema.pattern)
            self.assertEqual(schema.archives, expected_schema.archives)

    def test_loadAggregationSchema_return_the_default_schema_last(self):
        from carbon.storage import loadAggregationSchemas, defaultAggregation
        schema_list = loadAggregationSchemas()
        last_schema = schema_list[-1]
        self.assertEqual(last_schema, defaultAggregation)
