from twisted.internet import defer, protocol, reactor
from twisted.web.client import Agent, HTTPConnectionPool
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface import implements

try:
  from urllib import urlencode
except ImportError:
  from urllib.parse import urlencode


# async http client connection pool
pool = HTTPConnectionPool(reactor)


class StringProducer(object):
  implements(IBodyProducer)

  def __init__(self, body):
    self.body = body
    self.length = len(body)

  def startProducing(self, consumer):
    consumer.write(self.body)
    return defer.succeed(None)

  def pauseProducing(self):
    pass

  def stopProducing(self):
    pass


class SimpleReceiver(protocol.Protocol):
  def __init__(self, response, d):
    self.response = response
    self.buf = ''
    self.d = d

  def dataReceived(self, data):
    self.buf += data

  def connectionLost(self, reason):
    # TODO: test if reason is twisted.web.client.ResponseDone, if not, do an errback
    self.d.callback({
      'code': self.response.code,
      'body': self.buf,
    })


def httpRequest(url, values=None, headers=None, method='POST'):
  fullHeaders = {
    'Content-Type': ['application/x-www-form-urlencoded']
  }
  if headers:
    fullHeaders.update(headers)

  def handle_response(response):
    d = defer.Deferred()
    response.deliverBody(SimpleReceiver(response, d))
    return d

  return Agent(reactor, pool=pool).request(
    method,
    url,
    Headers(fullHeaders),
    StringProducer(urlencode(values)) if values else None
  ).addCallback(handle_response)
