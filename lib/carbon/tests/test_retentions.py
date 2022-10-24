from unittest import TestCase
from carbon.util import parseRetentionDef


class TestParseRetentionDef(TestCase):
    def test_valid_retentions(self):
        retention_map = (
            ('60:10', (60, 10)),
            ('10:60', (10, 60)),
            ('10s:10h', (10, 3600)),
        )
        for retention, expected in retention_map:
            results = parseRetentionDef(retention)
            self.assertEqual(results, expected)

    def test_invalid_retentions(self):
        retention_map = (
            # From getUnitString
            ('10x:10', ValueError("Invalid unit 'x'")),
            ('60:10x', ValueError("Invalid unit 'x'")),

            # From parseRetentionDef
            ('10X:10', ValueError("Invalid precision specification '10X'")),
            ('10:10$', ValueError("Invalid retention specification '10$'")),
            ('60:10', (60, 10)),
        )
        for retention, expected_exc in retention_map:
            try:
                results = parseRetentionDef(retention)
            except expected_exc.__class__ as exc:
                self.assertEqual(
                    str(expected_exc),
                    str(exc),
                )
                self.assertEqual(
                    expected_exc.__class__,
                    exc.__class__,
                )
            else:
                # When there isn't an exception raised
                self.assertEqual(results, expected_exc)
