from unittest import TestCase

from carbon.util import parseDestinations


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
