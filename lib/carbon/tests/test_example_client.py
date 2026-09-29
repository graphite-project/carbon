import importlib.util
from pathlib import Path
import socket
import unittest
from unittest import mock


class ExampleClientTest(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).resolve().parents[3] / 'examples' / 'example-client.py'
        spec = importlib.util.spec_from_file_location('example_client', path)
        self.client = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.client)

    def test_send_load_average(self):
        sender, receiver = socket.socketpair()
        self.addCleanup(sender.close)
        self.addCleanup(receiver.close)
        receiver.settimeout(1)
        with mock.patch.object(self.client, 'get_loadavg', return_value=['1.2', '2.3', '3.4']), \
                mock.patch.object(self.client.time, 'time', return_value=1234567890), \
                mock.patch.object(self.client.time, 'sleep', side_effect=KeyboardInterrupt), \
                mock.patch('builtins.print'):
            with self.assertRaises(KeyboardInterrupt):
                self.client.run(sender, 60)
        sender.shutdown(socket.SHUT_WR)
        with receiver.makefile('rb') as stream:
            self.assertEqual(stream.read(),
                             b'system.loadavg_1min 1.2 1234567890\n'
                             b'system.loadavg_5min 2.3 1234567890\n'
                             b'system.loadavg_15min 3.4 1234567890\n')

    def test_uptime_output(self):
        with mock.patch.object(self.client.platform, 'system', return_value='Darwin'), \
                mock.patch.object(self.client.subprocess, 'Popen') as popen:
            popen.return_value.communicate.return_value = (
                '12:00 d\u00e9marr\u00e9, 2 users, load averages: 1.2 2.3 3.4\n', None)
            self.assertEqual(self.client.get_loadavg(), ['1.2', '2.3', '3.4'])
            popen.assert_called_once_with('uptime', stdout=self.client.subprocess.PIPE,
                                          shell=True, universal_newlines=True)
