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

__all__ = ['Connection', 'connect', 'connect_thread_local']

LOWEST_COMPATIBLE_VERSION = 17

class Connection(Cassandra.Client):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport=True, timeout=None, credentials=None):
        self.server = server
        host, port = server.split(":")
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
            credentials=None, retry_time=60, recycle=None, round_robin=None,
            use_threadlocal=True):
    """
    Constructs a single :class:`.Connection`. Connects to a randomly chosen
    server on the list.

    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a :exc:`.NoServerAvailable` exception.

    Failing servers are kept on a separate list and eventually retried, no
    sooner than `retry_time` seconds after failure.

    :param keyspace: The keyspace to associate this connection with.
    :type keyspace: str

    :param servers: List of Cassandra servers with format: "hostname:port".
      Default: ``['localhost:9160']``
    :type servers: str[]

    :param framed_transport: If ``True``, use a :class:`TFramedTransport` instead of a
      :class:`TBufferedTransport`. Cassandra 0.7.x uses framed transport, while
      Cassandra 0.6.x uses buffered.
    :type framed_transport: bool

    :param timeout: Timeout in seconds (e.g. 0.5). Default: ``None`` (it will stall forever)
    :type timeout: float

    :param retry_time: Minimum time in seconds until a failed server is reinstated
      (e.g. 0.5). Default: 60.
    :type retry_time: float

    :param credentials: Dictionary of user credentials. Example:
      ``{'username':'jsmith', 'password':'havebadpass'}``
    :type credentials: dict

    :param recycle: Max time in seconds before an open connection is closed and replaced.
      Default: ``None`` (never recycle)
    :type recycle: float

    :param round_robin: *DEPRECATED*
    :type round_robin: bool

    :rtype: :class:`~pycassa.pool.Pool`

    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return pool.QueuePool(keyspace=keyspace, server_list=servers,
                          credentials=credentials, timeout=timeout,
                          use_threadlocal=use_threadlocal, prefill=False,
                          pool_size=len(servers), max_overflow=len(servers),
                          max_retries=len(servers))

connect_thread_local = connect
