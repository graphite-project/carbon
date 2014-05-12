from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

#Thrift connection pool
class ConnectionPool(object):

    DEFAULT_NETWORK_TIMEOUT = 0
    DEFAULT_POOL_SIZE = 100

    def __init__(self, host, port, iface_cls, size=DEFAULT_POOL_SIZE, async=False, network_timeout=DEFAULT_NETWORK_TIMEOUT):
        self.host = host
        self.port = port
        self.iface_cls = iface_cls

        self.network_timeout = network_timeout
        self.size = size

        self._closed = False
        self._async = async
        if self._async:
            import gevent.queue
            try:
                from gevent import lock as glock
            except ImportError:
                # gevent < 1.0
                from gevent import coros as glock
            self._semaphore = glock.BoundedSemaphore(size)
            self._connection_queue = gevent.queue.LifoQueue(size)
            self._QueueEmpty = gevent.queue.Empty

        else:
            import threading
            import Queue
            self._semaphore = threading.BoundedSemaphore(size)
            self._connection_queue = Queue.LifoQueue(size)
            self._QueueEmpty = Queue.Empty

    def close(self):
        self._closed = True
        while not self._connection_queue.empty():
            try:
                conn = self._connection_queue.get(block=False)
                try:
                    self._close_thrift_connection(conn)
                except:
                    pass
            except self._QueueEmpty:
                pass

    def _create_thrift_connection(self):
        socket = TSocket.TSocket(self.host, self.port)
        if self.network_timeout > 0:
            socket.setTimeout(self.network_timeout)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        connection = self.iface_cls(protocol)
        transport.open()
        return connection

    def _close_thrift_connection(self, conn):
        try:
            conn._iprot.trans.close()
        except:
            print 'warn: failed to close iprot trans on',conn
            pass
        try:
            conn._oprot.trans.close()
        except:
            print 'warn: failed to close oprot trans on',conn
            pass

    def get_connection(self):
        """ get a connection from the pool. This blocks until one is available.
        """
        self._semaphore.acquire()
        if self._closed:
            raise RuntimeError('connection pool closed')
        try:
            return self._connection_queue.get(block=False)
        except self._QueueEmpty:
            try:
                return self._create_thrift_connection()
            except:
                self._semaphore.release()
                raise

    def return_connection(self, conn):
        """ return a thrift connection to the pool.
        """
        if self._closed:
            self._close_thrift_connection(conn)
            return
        self._connection_queue.put(conn)
        self._semaphore.release()

    def release_conn(self, conn):
        """ call when the connect is no usable anymore
        """
        try:
            self._close_thrift_connection(conn)
        except:
            pass
        if not self._closed:
            self._semaphore.release()
