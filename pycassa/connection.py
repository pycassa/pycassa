from exceptions import Exception
import logging
import random
import socket
import threading
import time

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra
from cassandra.constants import VERSION
from cassandra.ttypes import AuthenticationRequest

__all__ = ['connect', 'connect_thread_local', 'NoServerAvailable']

DEFAULT_SERVER = 'localhost:9160'
API_VERSION = VERSION.split('.')

log = logging.getLogger('pycassa')

class NoServerAvailable(Exception):
    pass

class ClientTransport(object):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport, timeout, credentials, recycle):
        host, port = server.split(":")
        socket = TSocket.TSocket(host, int(port))
        if timeout is not None:
            socket.setTimeout(timeout*1000.0)
        if framed_transport:
            transport = TTransport.TFramedTransport(socket)
        else:
            transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = Cassandra.Client(protocol)
        transport.open()

        server_api_version = client.describe_version().split('.', 1)
        assert server_api_version[0] == API_VERSION[0], \
                "Thrift API version mismatch. " \
                 "(Client: %s, Server: %s)" % (API_VERSION[0], server_api_version[0])

        client.set_keyspace(keyspace)

        if credentials is not None:
            request = AuthenticationRequest(credentials=credentials)
            client.login(request)

        self.keyspace = keyspace
        self.client = client
        self.transport = transport

        if recycle:
            self.recycle = time.time() + recycle + random.uniform(0, recycle * 0.1)
        else:
            self.recycle = None


def connect(keyspace, servers=None, framed_transport=True, timeout=None,
            credentials=None, retry_time=60, recycle=None, round_robin=None,
            use_threadlocal=True):
    """
    Constructs a single Cassandra connection. Connects to a randomly chosen
    server on the list.

    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Failing servers are kept on a separate list and eventually retried, no
    sooner than `retry_time` seconds after failure.

    Parameters
    ----------
    keyspace: string
              The keyspace to associate this connection with.
    servers : [server]
              List of Cassandra servers with format: "hostname:port"

              Default: ['localhost:9160']
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5)

              Default: None (it will stall forever)
    retry_time: float
              Minimum time in seconds until a failed server is reinstated. (e.g. 0.5)

              Default: 60
    credentials : dict
              Dictionary of Credentials

              Example: {'username':'jsmith', 'password':'havebadpass'}
    recycle: float
              Max time in seconds before an open connection is closed and returned to the pool.

              Default: None (Never recycle)

    round_robin: bool
              *DEPRECATED*

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return Connection(keyspace, servers, framed_transport,
                      timeout, retry_time, recycle,
                      credentials, use_threadlocal)

connect_thread_local = connect

class ServerSet(object):
    """Automatically balanced set of servers.
       Manages a separate stack of failed servers, and automatic
       retrial."""

    def __init__(self, servers, retry_time=10):
        self._lock = threading.RLock()
        self._servers = list(servers)
        self._retry_time = retry_time
        self._dead = []

    def get(self):
        self._lock.acquire()
        try:
            if self._dead:
                ts, revived = self._dead.pop()
                if ts > time.time():  # Not yet, put it back
                    self._dead.append((ts, revived))
                else:
                    self._servers.append(revived)
                    log.info('Server %r reinstated into working pool', revived)
            if not self._servers:
                log.critical('No servers available')
                raise NoServerAvailable()
            return random.choice(self._servers)
        finally:
            self._lock.release()

    def mark_dead(self, server):
        self._lock.acquire()
        try:
            self._servers.remove(server)
            self._dead.insert(0, (time.time() + self._retry_time, server))
        finally:
            self._lock.release()

class Connection(object):

    def __init__(self, keyspace, servers, framed_transport=True, timeout=None,
                 retry_time=10, recycle=None, credentials=None,
                 use_threadlocal=True):
        self._keyspace = keyspace
        self._servers = ServerSet(servers, retry_time)
        self._framed_transport = framed_transport
        self._timeout = timeout
        self._recycle = recycle
        self._credentials = credentials
        self._use_threadlocal = use_threadlocal
        if self._use_threadlocal:
            self._local = threading.local()
        else:
            self._connection = None

    def connect(self):
        """Create new connection unless we already have one."""
        try:
            server = self._servers.get()
            if self._use_threadlocal and not getattr(self._local, 'conn', None):
                self._local.conn = ClientTransport(self._keyspace,
                                                   server, self._framed_transport,
                                                   self._timeout, self._credentials,
                                                   self._recycle)
            elif not self._use_threadlocal and not self._connection:
                self._connection = ClientTransport(self._keyspace,
                                                   server, self._framed_transport,
                                                   self._timeout, self._credentials,
                                                   self._recycle)
        except (Thrift.TException, socket.timeout, socket.error):
            log.warning('Connection to %s failed', server)
            self._servers.mark_dead(server)
            return self.connect()
        if self._use_threadlocal is True:
            return self._local.conn
        else:
            return self._connection

    def close(self):
        """If a connection is open, close its transport."""
        if self._use_threadlocal and hasattr(self._local, 'conn'):
            if self._local.conn:
                self._local.conn.transport.close()
            self._local.conn = None
        elif not self._use_threadlocal:
            if self._connection:
                self._connection.transport.close()
            self._connection = None

    def __getattr__(self, attr):
        def _client_call(*args, **kwargs):
            try:
                conn = self._ensure_connection()
                return getattr(conn.client, attr)(*args, **kwargs)
            except (Thrift.TException, socket.timeout, socket.error), exc:
                log.exception('Client error: %s', exc)
                self.close()
                return _client_call(*args, **kwargs) # Retry
        setattr(self, attr, _client_call)
        return getattr(self, attr)

    def _ensure_connection(self):
        """Make certain we have a valid connection and return it."""
        conn = self.connect()
        if conn.recycle and conn.recycle < time.time():
            log.debug('Client session expired after %is. Recycling.', self._recycle)
            self.close()
            conn = self.connect()
        return conn

    def _replace(self, new_conn):
        self._keyspace = new_conn._keyspace
        self._servers = new_conn._servers
        self._framed_transport = new_conn._framed_transport
        self._timeout = new_conn._timeout
        self._recycle = new_conn._recycle
        self._credentials = new_conn._credentials
        self._use_threadlocal = new_conn._use_threadlocal
        if self._use_threadlocal:
            self._local = new_conn.local
        else:
            self._connection = new_conn._connection

    def get_keyspace_description(self, keyspace=None):
        """
        Describes the given keyspace.
        
        Parameters
        ----------
        keyspace: str
                  Defaults to the current keyspace.

        Returns
        -------
        {column_family_name: CfDef}
        where a CfDef has many attributes describing the column family, including
        the dictionary column_metadata = {column_name: ColumnDef}
        """
        if keyspace is None:
            keyspace = self._keyspace

        ks_def = self.describe_keyspace(keyspace)
        cf_defs = dict()
        for cf_def in ks_def.cf_defs:
            cf_defs[cf_def.name] = cf_def
            old_metadata = cf_def.column_metadata
            new_metadata = dict()
            for datum in old_metadata:
                new_metadata[datum.name] = datum
            cf_def.column_metadata = new_metadata
        return cf_defs
