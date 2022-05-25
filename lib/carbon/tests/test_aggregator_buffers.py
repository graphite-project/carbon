from mock import call, Mock, patch
from unittest import TestCase

from twisted.internet.task import LoopingCall

from carbon import instrumentation
from carbon.aggregator.buffers import BufferManager, IntervalBuffer, MetricBuffer
from carbon.tests.util import TestSettings


class AggregationBufferManagerTest(TestCase):
  def tearDown(self):
    BufferManager.clear()

  @patch("carbon.aggregator.buffers.MetricBuffer")
  def test_get_nonexistent_buffer_creates_new(self, metric_buffer_mock):
    BufferManager.get_buffer("carbon.foo")
    metric_buffer_mock.assert_called_once_with("carbon.foo")

  @patch("carbon.aggregator.buffers.MetricBuffer", new=Mock())
  def test_get_nonexistent_buffer_creates_and_saves_it(self):
    new_buffer = BufferManager.get_buffer("carbon.foo")
    existing_buffer = BufferManager.get_buffer("carbon.foo")
    self.assertTrue(new_buffer is existing_buffer)

  @patch("carbon.aggregator.buffers.MetricBuffer", new=Mock(spec=MetricBuffer))
  def test_clear_closes_buffers(self):
    metric_buffer_mock = BufferManager.get_buffer("carbon.foo")
    BufferManager.clear()
    metric_buffer_mock.close.assert_called_once_with()


class AggregationMetricBufferTest(TestCase):
  def setUp(self):
    self.new_metric_buffer = MetricBuffer("carbon.foo")

    with patch("carbon.aggregator.buffers.LoopingCall", new=Mock()):
      self.metric_buffer = MetricBuffer("carbon.foo.bar")
      self.metric_buffer.configure_aggregation(60, sum)

  def tearDown(self):
    instrumentation.stats.clear()

  def test_new_buffer_is_unconfigured(self):
    self.assertFalse(self.new_metric_buffer.configured)

  @patch("carbon.aggregator.buffers.LoopingCall", new=Mock())
  def test_configure_buffer_marks_configured(self):
    self.new_metric_buffer.configure_aggregation(60, sum)
    self.assertTrue(self.new_metric_buffer.configured)

  @patch("carbon.aggregator.buffers.LoopingCall", spec=LoopingCall)
  def test_configure_buffer_creates_looping_call(self, looping_call_mock):
    self.new_metric_buffer.configure_aggregation(60, sum)
    looping_call_mock.assert_called_once_with(self.new_metric_buffer.compute_value)

  @patch("carbon.aggregator.buffers.LoopingCall", spec=LoopingCall)
  def test_configure_buffer_starts_looping_call(self, looping_call_mock):
    self.new_metric_buffer.configure_aggregation(60, sum)
    looping_call_mock.return_value.start.assert_called_once_with(60, now=False)

  @patch("carbon.aggregator.buffers.LoopingCall", spec=LoopingCall)
  def test_configure_buffer_uses_freq_if_less_than_writeback_freq(self, looping_call_mock):
    settings = TestSettings()
    settings['WRITE_BACK_FREQUENCY'] = 300
    with patch('carbon.aggregator.buffers.settings', new=settings):
      self.new_metric_buffer.configure_aggregation(60, sum)
      looping_call_mock.return_value.start.assert_called_once_with(60, now=False)

  @patch("carbon.aggregator.buffers.LoopingCall", spec=LoopingCall)
  def test_configure_buffer_uses_writeback_freq_if_less_than_freq(self, looping_call_mock):
    settings = TestSettings()
    settings['WRITE_BACK_FREQUENCY'] = 30
    with patch('carbon.aggregator.buffers.settings', new=settings):
      self.new_metric_buffer.configure_aggregation(60, sum)
      looping_call_mock.return_value.start.assert_called_once_with(30, now=False)

  @patch("carbon.aggregator.buffers.IntervalBuffer", new=Mock())
  def test_input_rounds_down_to_interval(self):
    # Interval of 60
    self.metric_buffer.input((125, 1.0))
    self.assertTrue(120 in self.metric_buffer.interval_buffers)

  @patch("carbon.aggregator.buffers.IntervalBuffer", spec=IntervalBuffer)
  def test_input_passes_datapoint_to_interval_buffer(self, interval_buffer_mock):
    self.metric_buffer.input((120, 1.0))
    interval_buffer_mock.return_value.input.assert_called_once_with((120, 1.0))

  @patch("time.time", new=Mock(return_value=600))
  @patch("carbon.state.events.metricGenerated")
  def test_compute_value_flushes_active_buffer(self, metric_generated_mock):
    self.metric_buffer.input((600, 1.0))
    self.metric_buffer.compute_value()
    metric_generated_mock.assert_called_once_with("carbon.foo.bar", (600, 1.0))

  @patch("time.time", new=Mock(return_value=600))
  @patch("carbon.state.events.metricGenerated")
  def test_compute_value_uses_interval_for_flushed_datapoint(self, metric_generated_mock):
    self.metric_buffer.input((630, 1.0))
    self.metric_buffer.compute_value()
    metric_generated_mock.assert_called_once_with("carbon.foo.bar", (600, 1.0))

  @patch("time.time", new=Mock(return_value=600))
  @patch("carbon.state.events.metricGenerated", new=Mock())
  def test_compute_value_marks_buffer_inactive(self):
    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    self.metric_buffer.interval_buffers[600] = interval_buffer

    with patch.object(IntervalBuffer, 'mark_inactive') as mark_inactive_mock:
      self.metric_buffer.compute_value()
      mark_inactive_mock.assert_called_once_with()

  @patch("time.time", new=Mock(return_value=600))
  @patch("carbon.state.events.metricGenerated", new=Mock())
  def test_compute_value_computes_aggregate(self):
    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    interval_buffer.input((601, 2.0))
    interval_buffer.input((602, 3.0))
    self.metric_buffer.interval_buffers[600] = interval_buffer

    with patch.object(self.metric_buffer, 'aggregation_func') as aggregation_func_mock:
      self.metric_buffer.compute_value()
      aggregation_func_mock.assert_called_once_with([1.0, 2.0, 3.0])

  @patch("time.time", new=Mock(return_value=600))
  @patch("carbon.state.events.metricGenerated")
  def test_compute_value_skips_inactive_buffers(self, metric_generated_mock):
    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    interval_buffer.mark_inactive()
    self.metric_buffer.interval_buffers[600] = interval_buffer

    self.metric_buffer.compute_value()
    self.assertFalse(metric_generated_mock.called)

  @patch("carbon.state.events.metricGenerated")
  def test_compute_value_can_flush_interval_multiple_times(self, metric_generated_mock):
    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    interval_buffer.input((601, 2.0))
    interval_buffer.input((602, 3.0))
    self.metric_buffer.interval_buffers[600] = interval_buffer

    with patch("time.time") as time_mock:
      time_mock.return_value = 600
      self.metric_buffer.compute_value()
      calls = [call("carbon.foo.bar", (600, 6.0))]
      # say WRITE_BACK_FREQUENCY is 30, we flush again if another point came in
      time_mock.return_value = 630
      interval_buffer.input((604, 4.0))
      self.metric_buffer.compute_value()
      calls.append(call("carbon.foo.bar", (600, 10.0)))

      metric_generated_mock.assert_has_calls(calls)

  @patch("carbon.state.events.metricGenerated")
  def test_compute_value_doesnt_flush_unchanged_interval_many_times(self, metric_generated_mock):
    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    self.metric_buffer.interval_buffers[600] = interval_buffer

    with patch("time.time") as time_mock:
      time_mock.return_value = 600
      self.metric_buffer.compute_value()
      calls = [call("carbon.foo.bar", (600, 1.0))]
      # say WRITE_BACK_FREQUENCY is 30, we flush again but no point came in
      time_mock.return_value = 630
      self.metric_buffer.compute_value()

      metric_generated_mock.assert_has_calls(calls)

  def test_compute_value_deletes_expired_buffers(self):
    from carbon.conf import settings
    current_interval = 600 + 60 * settings['MAX_AGGREGATION_INTERVALS']

    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    interval_buffer.mark_inactive()
    self.metric_buffer.interval_buffers[600] = interval_buffer

    # 2nd interval for current time
    interval_buffer = IntervalBuffer(current_interval)
    interval_buffer.input((current_interval, 1.0))
    interval_buffer.mark_inactive()
    self.metric_buffer.interval_buffers[current_interval] = interval_buffer

    with patch("time.time", new=Mock(return_value=current_interval + 60)):
      self.metric_buffer.compute_value()
      self.assertFalse(600 in self.metric_buffer.interval_buffers)

  def test_compute_value_closes_metric_if_last_buffer_deleted(self):
    from carbon.conf import settings
    current_interval = 600 + 60 * settings['MAX_AGGREGATION_INTERVALS']

    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    interval_buffer.mark_inactive()
    self.metric_buffer.interval_buffers[600] = interval_buffer
    BufferManager.buffers['carbon.foo.bar'] = self.metric_buffer

    with patch("time.time", new=Mock(return_value=current_interval + 60)):
      with patch.object(MetricBuffer, 'close') as close_mock:
        self.metric_buffer.compute_value()
        close_mock.assert_called_once_with()

  def test_compute_value_unregisters_metric_if_last_buffer_deleted(self):
    from carbon.conf import settings
    current_interval = 600 + 60 * settings['MAX_AGGREGATION_INTERVALS']

    interval_buffer = IntervalBuffer(600)
    interval_buffer.input((600, 1.0))
    interval_buffer.mark_inactive()
    self.metric_buffer.interval_buffers[600] = interval_buffer
    BufferManager.buffers['carbon.foo.bar'] = self.metric_buffer

    with patch("time.time", new=Mock(return_value=current_interval + 60)):
      self.metric_buffer.compute_value()
      self.assertFalse('carbon.foo.bar' in BufferManager.buffers)

  def test_close_stops_looping_call(self):
    with patch.object(MetricBuffer, 'close') as close_mock:
      self.metric_buffer.close()
      close_mock.assert_called_once_with()
