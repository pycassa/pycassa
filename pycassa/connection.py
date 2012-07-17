from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

from pycassa.cassandra import Cassandra
from pycassa.cassandra.ttypes import AuthenticationRequest

DEFAULT_SERVER = 'localhost:9160'
DEFAULT_PORT = 9160

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
            socket.setTimeout(timeout * 1000.0)
        if framed_transport:
            self.transport = TTransport.TFramedTransport(socket)
        else:
            self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        Cassandra.Client.__init__(self, protocol)
        self.transport.open()

        self.set_keyspace(keyspace)

        if credentials is not None:
            request = AuthenticationRequest(credentials=credentials)
            self.login(request)

    def set_keyspace(self, keyspace):
        if keyspace != self.keyspace:
            Cassandra.Client.set_keyspace(self, keyspace)
            self.keyspace = keyspace

    def close(self):
        self.transport.close()
