from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra

__all__ = ['connect']

def connect(host="localhost", port=9160):
    """
    Construct a Cassandra connection

    Parameters
    ----------
    host : str
           The host of the Cassandra server
    port : int
           The port of the Cassandra server

    Returns
    -------
    Cassandra client
    """

    socket = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    transport.open()

    return client
