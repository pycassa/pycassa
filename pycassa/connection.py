from exceptions import Exception
import socket
import time
import warnings

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from pycassa.cassandra.c10 import Cassandra
from pycassa.cassandra.constants import *
from pycassa.cassandra.ttypes import AuthenticationRequest
from pycassa.util import compatible
import pool

__all__ = ['connect', 'connect_thread_local']

DEFAULT_SERVER = 'localhost:9160'
DEFAULT_PORT = 9160

LOWEST_COMPATIBLE_VERSION = 17

class ApiMismatch(Exception): pass

class Connection(Cassandra.Client):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport=True, timeout=None,
                 credentials=None, api_version=None):
        self.keyspace = None
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

        if api_version is None:
            server_api_version = self.describe_version()
            if compatible(CASSANDRA_10, server_api_version):
                self.version = CASSANDRA_10
            if compatible(CASSANDRA_08, server_api_version):
                self.version = CASSANDRA_08
            elif compatible(CASSANDRA_07, server_api_version):
                self.version = CASSANDRA_07
            else:
                raise ApiMismatch("Thrift API version incompatibility: " \
                                  "server version %s is not Cassandra 0.7, 0.8, or 1.0" %
                                  (server_api_version))
        else:
            self.version = api_version

        self.set_keyspace(keyspace)

        if credentials is not None:
            request = AuthenticationRequest(credentials=credentials)
            self.login(request)

    def set_keyspace(self, keyspace):
        if keyspace != self.keyspace:
            super(Connection, self).set_keyspace(keyspace)
            self.keyspace = keyspace

    def close(self):
        self.transport.close()


def connect(keyspace, servers=None, framed_transport=True, timeout=0.5,
            credentials=None, retry_time=60, recycle=None, use_threadlocal=True):
    """
    Constructs a :class:`~pycassa.pool.ConnectionPool`. This is primarily available
    for reasons of backwards-compatibility; creating a ConnectionPool directly
    provides more options.  All of the parameters here correspond directly
    with parameters of the same name in
    :meth:`pycassa.pool.ConnectionPool.__init__()`

    .. deprecated:: 1.2.2

    """
    msg = "pycassa.connect() has been deprecated. Create a ConnectionPool " +\
          "instance directly instead."
    warnings.warn(msg, DeprecationWarning)
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
