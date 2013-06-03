from twisted.internet import defer
from stompest.error import StompConnectionError, StompCancelledError, StompProtocolError
from stompest.protocol import StompSpec
from stompest.async.util import WaitingDeferred, InFlightOperations
from stompest.async import listener
from carbon import log


class ConnectListener(listener.Listener):
  """Waits for the **CONNECTED** frame to arrive. """

  def __init__(self):
    self._waiting = WaitingDeferred()

  @defer.inlineCallbacks
  def onConnect(self, connection, frame, connectedTimeout): # @UnusedVariable
    yield self._waiting.wait(connectedTimeout, StompCancelledError('STOMP broker did not answer on time [timeout=%s]' % connectedTimeout))

  def onConnected(self, connection, frame): # @UnusedVariable
    connection.remove(self)
    self._waiting.callback(None)

  def onConnectionLost(self, connection, reason):
    connection.remove(self)
    if not self._waiting.called:
      self._waiting.errback(reason)

  def onError(self, connection, frame):
    if not 'password is invalid' in frame.rawHeaders[1][1]:
      log.clients("%s::queueConnectionFailed %s" % (connection, frame.rawHeaders))
      self.onConnectionLost(connection, StompProtocolError('While trying to connect, received %s' % frame.info()))

class ErrorListener(listener.Listener):
  """Handles **ERROR** frames."""

  def onError(self, connection, frame):
    if 'password is invalid' in frame.rawHeaders[1][1]:
      log.clients("%s::queueLoginFailed %s" % (connection, frame.rawHeaders[1][1]))
      self.onConnectionLost(connection)
      connection.factory.stopConnecting()
    else:
      connection.disconnect(failure=StompProtocolError('Received %s' % frame.info()))

  def onConnectionLost(self, connection, *reason): # @UnusedVariable
    connection.remove(self)

class DisconnectListener(listener.Listener):
    """Handles graceful disconnect."""
    def onAdd(self, connection):
        self._disconnecting = False
        self._disconnectReason = None
        connection.disconnected = defer.Deferred()

    def onConnectionLost(self, connection, reason): # @UnusedVariable
        log.clients("%s::disconnected %s" % (connection, reason.getErrorMessage()))
        if not self._disconnecting:
            self._disconnectReason = StompConnectionError('Unexpected connection loss [%s]' % reason.getErrorMessage())

    def onCleanup(self, connection):
        connection.session.close(flush=not self._disconnectReason)
        connection.remove(self)

        if self._disconnectReason:
            # self.log.debug('Calling disconnected errback: %s' % self._disconnectReason)
            connection.disconnected.errback(self._disconnectReason)
        else:
            # self.log.debug('Calling disconnected callback')
            connection.disconnected.callback(None)
        connection.disconnected = None

    def onDisconnect(self, connection, failure, timeout): # @UnusedVariable
        if failure:
            self._disconnectReason = failure
        if self._disconnecting:
            return
        self._disconnecting = True
        log.clients("%s::disconnecting %s" % (connection, ('' if (not failure) else ('[reason=%s]' % failure.getErrorMessage()))))

    def onMessage(self, connection, frame, context): # @UnusedVariable
        if not self._disconnecting:
            return
        connection.nack(frame).addBoth(lambda _: None)

    @property
    def _disconnectReason(self):
        return self.__disconnectReason

    @_disconnectReason.setter
    def _disconnectReason(self, reason):
        if reason:
            reason = self._disconnectReason or reason # existing reason wins
        self.__disconnectReason = reason

class ReceiptListener(listener.Listener):
    def __init__(self, timeout=None):
        self._timeout = timeout
        self._receipts = InFlightOperations('Waiting for receipt')

    def onConnectionLost(self, connection, reason): # @UnusedVariable
        for waiting in self._receipts.values():
            if waiting.called:
                continue
            waiting.errback(StompCancelledError('Receipt did not arrive (connection lost)'))

    @defer.inlineCallbacks
    def onSend(self, connection, frame): # @UnusedVariable
        if not frame:
            defer.returnValue(None)
        receipt = frame.headers.get(StompSpec.RECEIPT_HEADER)
        if receipt is None:
            defer.returnValue(None)
        with self._receipts(receipt, self.log) as receiptArrived:
            yield receiptArrived.wait(self._timeout, StompCancelledError('Receipt did not arrive on time: %s [timeout=%s]' % (receipt, self._timeout)))

    def onReceipt(self, connection, frame, receipt): # @UnusedVariable
        self._receipts[receipt].callback(None)

def defaultListeners():
    return [ConnectListener(), DisconnectListener(), ErrorListener(), listener.HeartBeatListener()]
