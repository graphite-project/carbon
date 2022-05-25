from unittest import TestCase
from mock import MagicMock, patch

from carbon.pipeline import Processor, run_pipeline


class ProcessorTest(TestCase):
  def test_processor_registers(self):
    class DummyProcessor(Processor):
      plugin_name = "dummy_processor"

    self.assertTrue("dummy_processor" in Processor.plugins)
    del Processor.plugins["dummy_processor"]

  def test_run_pipeline_empty_processors(self):
    self.assertEqual(None, run_pipeline("carbon.metric", (0, 0), []))

  def test_run_pipeline(self):
    processor_mock = MagicMock(Processor)

    run_pipeline("carbon.metric", (0, 0), [processor_mock])
    processor_mock.process.assert_called_once_with("carbon.metric", (0, 0))

  def test_run_pipeline_no_processors_uses_state(self):
    processor_mock = MagicMock(Processor)

    import carbon.pipeline
    with patch.object(carbon.pipeline.state, 'pipeline_processors', [processor_mock]):
      run_pipeline("carbon.metric", (0, 0))
    processor_mock.process.assert_called_once_with("carbon.metric", (0, 0))
