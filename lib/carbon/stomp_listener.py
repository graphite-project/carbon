import time
from twisted.internet import defer, reactor, task
from stompest.error import StompConnectionError, StompCancelledError, StompProtocolError
from stompest.protocol import StompSpec
from stompest.async.util import WaitingDeferred, InFlightOperations, sendToErrorDestination
from carbon import log


class Listener(object):
    """This base class defines the interface for the handlers of possible asynchronous STOMP connection events. You may implement any subset of these event handlers and add the resulting listener to the :class:`~.async.client.Stomp` connection.
    """
    def __str__(self):
        return self.__class__.__name__

    # TODO: doc strings for all event handlers.
    def onAdd(self, connection):
        pass

    def onConnect(self, connection, frame, connectedTimeout):
        pass

    def onConnected(self, connection, frame):
        pass

    def onConnectionLost(self, connection, reason):
        pass

    def onCleanup(self, connection):
        pass

    def onDisconnect(self, connection, failure, timeout):
        pass

    def onError(self, connection, frame):
        pass

    def onFrame(self, connection, frame):
        pass

    def onMessage(self, connection, frame, context):
        pass

    def onReceipt(self, connection, frame, receipt):
        pass

    def onSend(self, connection, frame):
        pass

    def onSubscribe(self, connection, frame, context):
        pass

    def onUnsubscribe(self, connection, frame, context):
        pass

class ConnectListener(Listener):
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

class ErrorListener(Listener):
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

class DisconnectListener(Listener):
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

class ReceiptListener(Listener):
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

class SubscriptionListener(Listener):
    DEFAULT_ACK_MODE = 'client-individual'

    def __init__(self, handler, ack=True, errorDestination=None, onMessageFailed=None):
        if not callable(handler):
            raise ValueError('Handler is not callable: %s' % handler)
        self._handler = handler
        self._ack = ack
        self._errorDestination = errorDestination
        self._onMessageFailed = onMessageFailed or sendToErrorDestination
        self._headers = None
        self._messages = InFlightOperations('Handler for message')

    @defer.inlineCallbacks
    def onDisconnect(self, connection, failure, timeout): # @UnusedVariable
        if not self._messages:
            defer.returnValue(None)
        log.clients("%s::disconnecting %s" % (connection, 'Waiting for outstanding message handlers to finish ... [timeout=%s]' % timeout))
        yield self._waitForMessages(timeout)
        log.clients("%s::disconnecting %s" % (connection, 'All handlers complete. Resuming disconnect ...'))

    @defer.inlineCallbacks
    def onMessage(self, connection, frame, context):
        """onMessage(connection, frame, context)
        
        Handle a message originating from this listener's subscription."""
        if context is not self:
            return
        with self._messages(frame.headers[StompSpec.MESSAGE_ID_HEADER], self.log) as waiting:
            try:
                yield self._handler(connection, frame)
            except Exception as e:
                yield self._onMessageFailed(connection, e, frame, self._errorDestination)
            finally:
                if self._ack and (self._headers[StompSpec.ACK_HEADER] in StompSpec.CLIENT_ACK_MODES):
                    connection.ack(frame)
                if not waiting.called:
                    waiting.callback(None)

    def onSubscribe(self, connection, frame, context): # @UnusedVariable
        """Set the **ack** header of the **SUBSCRIBE** frame initiating this listener's subscription to the value of the class atrribute :attr:`DEFAULT_ACK_MODE` (if it isn't set already). Keep a copy of the headers for handling messages originating from this subscription."""
        if context is not self:
            return
        frame.headers.setdefault(StompSpec.ACK_HEADER, self.DEFAULT_ACK_MODE)
        self._headers = frame.headers

    @defer.inlineCallbacks
    def onUnsubscribe(self, connection, frame, context): # @UnusedVariable
        """onUnsubscribe(connection, frame, context)
        
        Forget everything about this listener's subscription and unregister from the **connection**."""
        if context is not self:
            return
        yield self._waitForMessages(None)
        connection.remove(self)

    def onConnectionLost(self, connection, reason): # @UnusedVariable
        """onConnectionLost(connection, reason)
        
        Forget everything about this listener's subscription and unregister from the **connection**."""
        connection.remove(self)

    def _waitForMessages(self, timeout):
        return task.cooperate(handler.wait(timeout, StompCancelledError('Handlers did not finish in time.')) for handler in self._messages.values()).whenDone()

class HeartBeatListener(Listener):
    DEFAULT_THRESHOLDS = {'client': 0.8, 'server': 2.0}

    def __init__(self, thresholds=None):
        self._thresholds = thresholds or self.DEFAULT_THRESHOLDS
        self._heartBeats = {}

    def onConnected(self, connection, frame): # @UnusedVariable
        self._beats(connection)

    def onConnectionLost(self, connection, reason): # @UnusedVariable
        self._beats(None)
        connection.remove(self)

    def onFrame(self, connection, frame): # @UnusedVariable
        connection.session.received()

    def onSend(self, connection, frame): # @UnusedVariable
        connection.session.sent()

    def _beats(self, connection):
        for which in ('client', 'server'):
            self._beat(connection, which)

    def _beat(self, connection, which):
        try:
            self._heartBeats.pop(which).cancel()
        except:
            pass
        if not connection:
            return
        remaining = self._beatRemaining(connection.session, which)
        if remaining < 0:
            return
        if not remaining:
            if which == 'client':
                connection.sendFrame(connection.session.beat())
                remaining = self._beatRemaining(connection.session, which)
            else:
                connection.disconnect(failure=StompConnectionError('Server heart-beat timeout'))
                return
        self._heartBeats[which] = reactor.callLater(remaining, self._beat, connection, which) # @UndefinedVariable

    def _beatRemaining(self, session, which):
        heartBeat = {'client': session.clientHeartBeat, 'server': session.serverHeartBeat}[which]
        if not heartBeat:
            return -1
        last = {'client': session.lastSent, 'server': session.lastReceived}[which]
        elapsed = time.time() - last
        return max((self._thresholds[which] * heartBeat / 1000.0) - elapsed, 0)

def defaultListeners():
    return [ConnectListener(), DisconnectListener(), ErrorListener(), HeartBeatListener()]
