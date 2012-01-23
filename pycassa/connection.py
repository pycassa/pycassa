from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from pycassa.cassandra.c10 import Cassandra
from pycassa.cassandra.constants import (CASSANDRA_07, CASSANDRA_08, CASSANDRA_10)
from pycassa.cassandra.ttypes import AuthenticationRequest
from pycassa.util import compatible

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
