import importlib.util
from pathlib import Path
import socket
import unittest
from unittest import mock


class ExampleClientTest(unittest.TestCase):
    """Exercise byte transmission and locale-aware load-average parsing."""

    def setUp(self):
        """Load the standalone client without starting its main loop."""
        path = Path(__file__).resolve().parents[3] / 'examples' / 'example-client.py'
        spec = importlib.util.spec_from_file_location('example_client', path)
        self.client = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.client)

    def test_send_load_average(self):
        """Send one metric batch over a real socket as UTF-8 bytes."""
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        receiver.settimeout(1)
        loadavg = ['1.2', '2.3', '3.4']
        stop = mock.patch.object(self.client.time, 'sleep', side_effect=KeyboardInterrupt)
        with mock.patch.object(self.client, 'get_loadavg', return_value=loadavg), \
                mock.patch.object(self.client.time, 'time', return_value=1234567890), \
                stop, mock.patch('builtins.print'), self.assertRaises(KeyboardInterrupt):
            self.client.run(sender, 60)
        sender.shutdown(socket.SHUT_WR)
        with receiver.makefile('rb') as stream:
            self.assertEqual(stream.read(),
                             b'system.loadavg_1min 1.2 1234567890\n'
                             b'system.loadavg_5min 2.3 1234567890\n'
                             b'system.loadavg_15min 3.4 1234567890\n')

    def test_uptime_output(self):
        """Parse localized uptime text without forcing UTF-8 or using a shell."""
        with mock.patch.object(self.client.platform, 'system', return_value='Darwin'), \
                mock.patch.object(self.client.subprocess, 'Popen') as popen:
            popen.return_value.communicate.return_value = (
                '12:00 d\u00e9marr\u00e9, 2 users, load averages: 1.2 2.3 3.4\n', None)
            self.assertEqual(self.client.get_loadavg(), ['1.2', '2.3', '3.4'])
            popen.assert_called_once_with(['uptime'], stdout=self.client.subprocess.PIPE,
                                          universal_newlines=True)
