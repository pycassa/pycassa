import threading
from Queue import Queue

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra

__all__ = ['connect', 'connect_thread_local']

DEFAULT_SERVER = 'localhost:9160'

def connect(server=DEFAULT_SERVER):
    """
    Construct a Cassandra connection

    Parameters
    ----------
    server : str
             Cassandra server with format: "hostname:port"

    Returns
    -------
    Cassandra client
    """

    host, port = server.split(":")
    socket = TSocket.TSocket(host, int(port))
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    transport.open()

    return client

def connect_thread_local(servers=None):
    """
    Construct a Cassandra connection, one for each thread

    Parameters
    ----------
    servers: [server]
             List of Cassandra servers with format: "hostname:port"

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
                self.local.client = connect(server)
                self.queue.put(server)

            try:
                return getattr(self.local.client, attr)(*args, **kwargs)
            except Thrift.TException, exc:
                # Connection error, try a new server next time
                self.local.client = None
                raise exc

        setattr(self, attr, client_call)
        return getattr(self, attr)
