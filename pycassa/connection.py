from exceptions import Exception
import socket
import time

import pool

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from pycassa.cassandra import Cassandra
from pycassa.cassandra.constants import VERSION
from pycassa.cassandra.ttypes import AuthenticationRequest

__all__ = ['connect', 'connect_thread_local']

DEFAULT_SERVER = 'localhost:9160'
DEFAULT_PORT = 9160

LOWEST_COMPATIBLE_VERSION = 17

class Connection(Cassandra.Client):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport=True, timeout=None, credentials=None):
        self.server = server
        server = server.split(':')
        if len(server) <= 1:
            port = 9160
        else:
            port = server[1]
        host = server[0]
        socket = TSocket.TSocket(host, int(port))
        if timeout is not None:
            socket.setTimeout(timeout*1000.0)
        if framed_transport:
            self.transport = TTransport.TFramedTransport(socket)
        else:
            self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        super(Connection, self).__init__(protocol)
        self.transport.open()

        server_api_version = int(self.describe_version().split('.', 1)[0])
        assert (server_api_version >= LOWEST_COMPATIBLE_VERSION), \
                "Thrift API version incompatibility. " \
                 "(Server: %s, Lowest compatible version: %d)" % (server_api_version, LOWEST_COMPATIBLE_VERSION)

        if keyspace is not None:
            self.set_keyspace(keyspace)
        self.keyspace = keyspace

        if credentials is not None:
            request = AuthenticationRequest(credentials=credentials)
            self.login(request)

    def set_keyspace(self, keyspace):
        self.keyspace = keyspace
        super(Connection, self).set_keyspace(keyspace)

    def close(self):
        self.transport.close()


def connect(keyspace, servers=None, framed_transport=True, timeout=None,
            credentials=None, retry_time=60, recycle=None, use_threadlocal=True):
    """
    Constructs a :class:`~pycassa.pool.ConnectionPool`. This is primarily available
    for reasons of backwards-compatibility; creating a ConnectionPool directly
    provides more options.  All of the parameters here correspond directly
    with parameters of the same name in
    :meth:`pycassa.pool.ConnectionPool.__init__()`

    """
    if servers is None:
        servers = [DEFAULT_SERVER]
    return pool.ConnectionPool(keyspace=keyspace, server_list=servers,
                               credentials=credentials, timeout=timeout,
                               use_threadlocal=use_threadlocal, prefill=False,
                               pool_size=len(servers), max_overflow=len(servers),
                               max_retries=len(servers))

def connect_thread_local(*args, **kwargs):
    """ Alias of :meth:`connect` """
    return connect(*args, **kwargs)
