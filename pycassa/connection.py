from exceptions import Exception
import socket
import time

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from pycassa.cassandra import Cassandra
from pycassa.cassandra.constants import VERSION
from pycassa.cassandra.ttypes import AuthenticationRequest

API_VERSION = VERSION.split('.')

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

        server_api_version = self.describe_version().split('.', 1)
        assert server_api_version[0] == API_VERSION[0], \
                "Thrift API version mismatch. " \
                 "(Client: %s, Server: %s)" % (API_VERSION[0], server_api_version[0])

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
