import socket

from unittest import TestCase

from carbon.util import parseDestinations
from carbon.util import enableTcpKeepAlive
from carbon.util import TaggedSeries


class UtilTest(TestCase):

    def test_enable_tcp_keep_alive(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        class _Transport():
            def getHandle(self):
                return s

            def setTcpKeepAlive(self, value):
                s.setsockopt(socket.SOL_TCP, socket.SO_KEEPALIVE, value)

        enableTcpKeepAlive(_Transport(), True, None)
        self.assertEquals(s.getsockopt(socket.SOL_TCP, socket.SO_KEEPALIVE), 1)

    def test_sanitizing_name_as_tag_value(self):
        test_cases = [
            {
                'original': "my~.test.abc",
                'expected': "my~.test.abc",
            }, {
                'original': "a.b.c",
                'expected': "a.b.c",
            }, {
                'original': "~~a~~.~~~b~~~.~~~c~~~",
                'expected': "a~~.~~~b~~~.~~~c~~~",
            }, {
                'original': "a.b.c~",
                'expected': "a.b.c~",
            }, {
                'original': "~a.b.c",
                'expected': "a.b.c",
            }, {
                'original': "~a~",
                'expected': "a~",
            }, {
                'original': "~~~",
                'raises': True,
            }, {
                'original': "~",
                'raises': True,
            },
        ]

        for test_case in test_cases:
            if test_case.get('raises', False):
                self.assertRaises(
                    Exception,
                    TaggedSeries.sanitize_name_as_tag_value,
                    test_case['original'],
                )
            else:
                result = TaggedSeries.sanitize_name_as_tag_value(test_case['original'])
                self.assertEquals(result, test_case['expected'])

    def test_validate_tag_key_and_value(self):
        # assert that it raises exception when sanitized name is still not valid
        with self.assertRaises(Exception):
            # sanitized name is going to be '', which is not a valid tag value
            TaggedSeries.sanitize_name_as_tag_value('~~~~')

        with self.assertRaises(Exception):
            # given tag value is invalid because it has length 0
            TaggedSeries.validateTagAndValue('metric.name;tag=')

        with self.assertRaises(Exception):
            # given tag key is invalid because it has length 0
            TaggedSeries.validateTagAndValue('metric.name;=value')

        with self.assertRaises(Exception):
            # given tag is missing =
            TaggedSeries.validateTagAndValue('metric.name;tagvalue')

        with self.assertRaises(Exception):
            # given tag value is invalid because it starts with ~
            TaggedSeries.validateTagAndValue('metric.name;tag=~value')

        with self.assertRaises(Exception):
            # given tag key is invalid because it contains !
            TaggedSeries.validateTagAndValue('metric.name;ta!g=value')


# Destinations have the form:
# <host> ::= <string without colons> | "[" <string> "]"
# <port> ::= <number>
# <instance> ::= <string>
# <destination> ::= <host> ":" <port> | <host> ":" <port> ":" <instance>

class ParseDestinationsTest(TestCase):
    def test_valid_dest_unbracketed(self):
        # Tests valid destinations in the unbracketed form of <host>.
        dests = [
            "127.0.0.1:1234:alpha",         # Full IPv4 address
            "127.1:1234:beta",              # 'Short' IPv4 address
            "localhost:987:epsilon",        # Relative domain name
            "foo.bar.baz.uk.:890:sigma"     # Absolute domain name
        ]

        expected = [
            ("127.0.0.1", 1234, "alpha"),
            ("127.1", 1234, "beta"),
            ("localhost", 987, "epsilon"),
            ("foo.bar.baz.uk.", 890, "sigma")
        ]

        actual = parseDestinations(dests)
        self.assertEquals(len(expected), len(actual))

        for exp, act in zip(expected, actual):
            self.assertEquals(exp, act)

    def test_valid_dest_bracketed(self):
        # Tests valid destinations in the bracketed form of <host>.
        dests = [
            "[fe80:dead:beef:cafe:0007:0007:0007:0001]:123:gamma",  # Full IPv6 address
            "[fe80:1234::7]:456:theta",                             # Compact IPv6 address
            "[::]:1:o",                                             # Very compact IPv6 address
            "[ffff::127.0.0.1]:789:omicron"                         # IPv6 mapped IPv4 address
        ]

        expected = [
            ("fe80:dead:beef:cafe:0007:0007:0007:0001", 123, "gamma"),
            ("fe80:1234::7", 456, "theta"),
            ("::", 1, "o"),
            ("ffff::127.0.0.1", 789, "omicron"),
        ]

        actual = parseDestinations(dests)
        self.assertEquals(len(expected), len(actual))

        for exp, act in zip(expected, actual):
            self.assertEquals(exp, act)

    def test_valid_dest_without_instance(self):
        # Tests destinations without instance specified.
        dests = [
            "1.2.3.4:5678",
            "[::1]:2",
            "stats.example.co.uk:8125",
            "[127.0.0.1]:78",               # Odd use of the bracket feature, but why not?
            "[why.not.this.com]:89",
        ]

        expected = [
            ("1.2.3.4", 5678, None),
            ("::1", 2, None),
            ("stats.example.co.uk", 8125, None),
            ("127.0.0.1", 78, None),
            ("why.not.this.com", 89, None)
        ]

        actual = parseDestinations(dests)
        self.assertEquals(len(expected), len(actual))

        for exp, act in zip(expected, actual):
            self.assertEquals(exp, act)

    def test_wrong_dest(self):
        # Some cases of invalid input, e.g. invalid/missing port.
        dests = [
            "1.2.3.4",                      # No port
            "1.2.3.4:huh",                  # Invalid port (must be int)
            "[fe80::3285:a9ff:fe91:e287]",  # No port
            "[ffff::1.2.3.4]:notaport"      # Invalid port
        ]

        for dest in dests:
            try:
                parseDestinations([dest])
            except ValueError:
                continue
            raise AssertionError("Invalid input was accepted.")
