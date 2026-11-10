import os
import sys
from io import StringIO
from os.path import abspath, dirname, join
from runpy import run_path
from tempfile import NamedTemporaryFile
from unittest import TestCase

try:
    from contextlib import redirect_stdout
except ImportError:  # Python 2
    from contextlib import contextmanager

    @contextmanager
    def redirect_stdout(target):
        old_target = sys.stdout
        sys.stdout = target
        try:
            yield target
        finally:
            sys.stdout = old_target

VALIDATOR_PATH = abspath(join(dirname(__file__), '..', '..', '..', 'bin', 'validate-storage-schemas.py'))


class ValidateStorageSchemasTest(TestCase):
    def run_validator(self, config_text):
        tmp_file = NamedTemporaryFile('w', suffix='.conf', delete=False)
        try:
            tmp_file.write(config_text)
            tmp_file.close()
            old_argv = sys.argv
            sys.argv = ['validate-storage-schemas.py', tmp_file.name]
            try:
                out = StringIO()
                with redirect_stdout(out):
                    try:
                        run_path(VALIDATOR_PATH)
                    except SystemExit as exc:
                        return exc.code, out.getvalue()
                return 0, out.getvalue()
            finally:
                sys.argv = old_argv
        finally:
            os.unlink(tmp_file.name)

    def test_valid_retentions_pass_validation(self):
        code, output = self.run_validator('[valid]\npattern = .*\nretentions = 60:10,300:20\n')
        self.assertEqual(code, 0)
        self.assertIn('is valid', output)

    def test_invalid_retention_definition_fails_validation(self):
        # Two archives with the same precision parse fine but fail
        # whisper.validateArchiveList; the validator must report failure
        # instead of printing OK and exiting 0.
        code, output = self.run_validator('[bad]\npattern = .*\nretentions = 60:10,60:20\n')
        self.assertNotEqual(code, 0)
        self.assertIn('failed validation', str(code))
        self.assertNotIn('is valid', output)
