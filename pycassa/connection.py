from thrift.transport import TTransport
from thrift.transport import TSocket, TSSLSocket
from thrift.protocol import TBinaryProtocol

from pycassa.cassandra import Cassandra
from pycassa.cassandra.ttypes import AuthenticationRequest

DEFAULT_SERVER = 'localhost:9160'
DEFAULT_PORT = 9160


def default_socket_factory(host, port):
    """
    Returns a normal :class:`TSocket` instance.
    """
    return TSocket.TSocket(host, port)


class Connection(Cassandra.Client):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport=True, timeout=None,
                 credentials=None, socket_factory=default_socket_factory):
        self.keyspace = None
        self.server = server
        server = server.split(':')
        if len(server) <= 1:
            port = 9160
        else:
            port = server[1]
        host = server[0]
        socket = socket_factory(host, int(port))
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


def make_ssl_socket_factory(ca_certs, validate=True):
    """
    A convenience function for creating an SSL socket factory.

    `ca_certs` should contain the path to the certificate file,
    `validate` determines whether or not SSL certificate validation will be performed.
    """

    def ssl_socket_factory(host, port):
        """
        Returns a :class:`TSSLSocket` instance.
        """
        return TSSLSocket.TSSLSocket(host, port, ca_certs=ca_certs, validate=validate)

    return ssl_socket_factory
