import random

from zope.interface import implementer

from twisted.internet._resolver import GAIResolver
from twisted.internet.defer import Deferred
from twisted.internet.address import IPv4Address
from twisted.internet.interfaces import IResolverSimple, IResolutionReceiver
from twisted.internet.error import DNSLookupError


# Inspired from /twisted/internet/_resolver.py
@implementer(IResolutionReceiver)
class RandomWins(object):
    """
    An L{IResolutionReceiver} which fires a L{Deferred} with a random result.
    """

    def __init__(self, deferred):
        """
        @param deferred: The L{Deferred} to fire with one resolution
            result arrives.
        """
        self._deferred = deferred
        self._results = []

    def resolutionBegan(self, resolution):
        """
        See L{IResolutionReceiver.resolutionBegan}
        @param resolution: See L{IResolutionReceiver.resolutionBegan}
        """
        self._resolution = resolution

    def addressResolved(self, address):
        """
        See L{IResolutionReceiver.addressResolved}
        @param address: See L{IResolutionReceiver.addressResolved}
        """
        self._results.append(address.host)

    def resolutionComplete(self):
        """
        See L{IResolutionReceiver.resolutionComplete}
        """
        if self._results:
            random.shuffle(self._results)
            self._deferred.callback(self._results[0])
        else:
            self._deferred.errback(DNSLookupError(self._resolution.name))


@implementer(IResolverSimple)
class ComplexResolverSimplifier(object):
    """
    A converter from L{IHostnameResolver} to L{IResolverSimple}
    """
    def __init__(self, nameResolver):
        """
        Create a L{ComplexResolverSimplifier} with an L{IHostnameResolver}.
        @param nameResolver: The L{IHostnameResolver} to use.
        """
        self._nameResolver = nameResolver

    def getHostByName(self, name, timeouts=()):
        """
        See L{IResolverSimple.getHostByName}
        @param name: see L{IResolverSimple.getHostByName}
        @param timeouts: see L{IResolverSimple.getHostByName}
        @return: see L{IResolverSimple.getHostByName}
        """
        result = Deferred()
        self._nameResolver.resolveHostName(RandomWins(result), name, 0,
                                           [IPv4Address])
        return result


def setUpRandomResolver(reactor):
    resolver = GAIResolver(reactor, reactor.getThreadPool)
    reactor.installResolver(ComplexResolverSimplifier(resolver))
