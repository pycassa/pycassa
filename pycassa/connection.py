import struct
from cStringIO import StringIO

from thrift.transport import TTransport, TSocket, TSSLSocket
from thrift.transport.TTransport import (TTransportBase, CReadableTransport,
        TTransportException)
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


def default_transport_factory(tsocket, host, port):
    """
    Returns a normal :class:`TFramedTransport` instance wrapping `tsocket`.
    """
    return TTransport.TFramedTransport(tsocket)


class Connection(Cassandra.Client):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport=True, timeout=None,
                 credentials=None,
                 socket_factory=default_socket_factory,
                 transport_factory=default_transport_factory):
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
        self.transport = transport_factory(socket, host, port)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        Cassandra.Client.__init__(self, protocol)
        self.transport.open()

        if credentials is not None:
            request = AuthenticationRequest(credentials=credentials)
            self.login(request)

        self.set_keyspace(keyspace)

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


class TSaslClientTransport(TTransportBase, CReadableTransport):

    START = 1
    OK = 2
    BAD = 3
    ERROR = 4
    COMPLETE = 5

    def __init__(self, transport, host, service,
            mechanism='GSSAPI', **sasl_kwargs):

        from puresasl.client import SASLClient

        self.transport = transport
        self.sasl = SASLClient(host, service, mechanism, **sasl_kwargs)

        self.__wbuf = StringIO()
        self.__rbuf = StringIO()

    def open(self):
        if not self.transport.isOpen():
            self.transport.open()

        self.send_sasl_msg(self.START, self.sasl.mechanism)
        self.send_sasl_msg(self.OK, self.sasl.process())

        while True:
            status, challenge = self.recv_sasl_msg()
            if status == self.OK:
                self.send_sasl_msg(self.OK, self.sasl.process(challenge))
            elif status == self.COMPLETE:
                if not self.sasl.complete:
                    raise TTransportException("The server erroneously indicated "
                            "that SASL negotiation was complete")
                else:
                    break
            else:
                raise TTransportException("Bad SASL negotiation status: %d (%s)"
                        % (status, challenge))

    def send_sasl_msg(self, status, body):
        header = struct.pack(">BI", status, len(body))
        self.transport.write(header + body)
        self.transport.flush()

    def recv_sasl_msg(self):
        header = self.transport.readAll(5)
        status, length = struct.unpack(">BI", header)
        if length > 0:
            payload = self.transport.readAll(length)
        else:
            payload = ""
        return status, payload

    def write(self, data):
        self.__wbuf.write(data)

    def flush(self):
        data = self.__wbuf.getvalue()
        encoded = self.sasl.wrap(data)
        # Note stolen from TFramedTransport:
        # N.B.: Doing this string concatenation is WAY cheaper than making
        # two separate calls to the underlying socket object. Socket writes in
        # Python turn out to be REALLY expensive, but it seems to do a pretty
        # good job of managing string buffer operations without excessive copies
        self.transport.write(''.join((struct.pack("!i", len(encoded)), encoded)))
        self.transport.flush()
        self.__wbuf = StringIO()

    def read(self, sz):
        ret = self.__rbuf.read(sz)
        if len(ret) != 0:
            return ret

        self._read_frame()
        return self.__rbuf.read(sz)

    def _read_frame(self):
        header = self.transport.readAll(4)
        length, = struct.unpack('!i', header)
        encoded = self.transport.readAll(length)
        self.__rbuf = StringIO(self.sasl.unwrap(encoded))

    def close(self):
        self.sasl.dispose()
        self.transport.close()

    # Implement the CReadableTransport interface.
    # Stolen shamelessly from TFramedTransport
    @property
    def cstringio_buf(self):
        return self.__rbuf

    def cstringio_refill(self, prefix, reqlen):
        # self.__rbuf will already be empty here because fastbinary doesn't
        # ask for a refill until the previous buffer is empty.  Therefore,
        # we can start reading new frames immediately.
        while len(prefix) < reqlen:
            self._read_frame()
            prefix += self.__rbuf.getvalue()
        self.__rbuf = StringIO(prefix)
        return self.__rbuf


def make_sasl_transport_factory(credential_factory):
    """
    A convenience function for creating a SASL transport factory.

    `credential_factory` should be a function taking two args: `host` and
    `port`.  It should return a ``dict`` of kwargs that will be passed
    to :func:`puresasl.client.SASLClient.__init__()`.

    Example usage::

        >>> def make_credentials(host, port):
        ...    return {'host': host,
        ...            'service': 'cassandra',
        ...            'principal': 'user/role@FOO.EXAMPLE.COM',
        ...            'mechanism': 'GSSAPI'}
        >>>
        >>> factory = make_sasl_transport_factory(make_credentials)
        >>> pool = ConnectionPool(..., transport_factory=factory)

    """

    def sasl_transport_factory(tsocket, host, port):
        sasl_kwargs = credential_factory(host, port)
        sasl_transport = TSaslClientTransport(tsocket, **sasl_kwargs)
        return TTransport.TFramedTransport(sasl_transport)

    return sasl_transport_factory
