import threading
from Queue import Queue

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra

__all__ = ['connect', 'connect_thread_local', 'connect_pooled']

DEFAULT_SERVER = 'localhost:9160'

def create_client_transport(server):
    host, port = server.split(":")
    socket = TSocket.TSocket(host, int(port))
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    transport.open()

    return client, transport

def connect(server=DEFAULT_SERVER):
    """
    Construct a Cassandra connection

    Parameters
    ----------
    server : str
             Cassandra server with format: "hostname:port"
             Default: 'localhost:9160'

    Returns
    -------
    Cassandra client
    """

    return create_client_transport(server)[0]

def connect_pooled(servers=None):
    """
    Construct a pooled queue of Cassandra connections, given by the servers list

    Parameters
    ----------
    servers: [server]
             List of Cassandra servers with format: "hostname:port"
             Create duplicate server entries if you want multiple connections
             to the same server.

             Default: 5 * ['localhost:9160']
             (5 connections to the server at localhost)

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = 5 * [DEFAULT_SERVER]
    return PooledConnection(servers)

class PooledConnection(object):
    def __init__(self, servers):
        self.queue = Queue()
        for server in servers:
            self.queue.put((server, None, None))

    def __getattr__(self, attr):
        def client_call(*args, **kwargs):
            server, client, transport = self.queue.get()
            try:
                if client is None:
                    client, transport = create_client_transport(server)
                return getattr(client, attr)(*args, **kwargs)
            except Thrift.TException as exc:
                # Connection error, try a new server next time
                transport.close()
                client, transport = None, None
                raise exc
            finally:
                self.queue.put((server, client, transport))

        setattr(self, attr, client_call)
        return getattr(self, attr)

def connect_thread_local(servers=None):
    """
    Construct a Cassandra connection for each thread

    Parameters
    ----------
    servers: [server]
             List of Cassandra servers with format: "hostname:port"

             Default: ['localhost:9160']

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return ThreadLocalConnection(servers)

class ThreadLocalConnection(object):
    def __init__(self, servers):
        self.queue = Queue()
        for server in servers:
            self.queue.put(server)
        self.local = threading.local()

    def __getattr__(self, attr):
        def client_call(*args, **kwargs):
            if getattr(self.local, 'client', None) is None:
                server = self.queue.get()
                self.queue.put(server)
                self.local.client, self.local.transport = create_client_transport(server)

            try:
                return getattr(self.local.client, attr)(*args, **kwargs)
            except Thrift.TException as exc:
                # Connection error, try a new server next time
                self.local.transport.close()
                self.local.client = None
                raise exc

        setattr(self, attr, client_call)
        return getattr(self, attr)
