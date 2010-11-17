"""
Tools for connecting to a Cassandra cluster.

.. seealso:: Module :mod:`~pycassa.pool` to see how connections
    can be pooled.

To get a connection object which you can use directly with a
:class:`~pycassa.columnfamily.ColumnFamily` or with a
:class:`~pycassa.columnfamilymap.ColumnFamilyMap`, you can do the
following::

    >>> import pycassa
    >>> connection = pycassa.connect('Keyspace', ['hostname:9160'])
"""

from exceptions import Exception
from logging.pycassa_logger import *
import random
import socket
import threading
import time

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from pycassa.cassandra import Cassandra
from pycassa.cassandra.constants import VERSION
from pycassa.cassandra.ttypes import AuthenticationRequest, TimedOutException,\
                                     UnavailableException, KsDef

from batch import Mutator

__all__ = ['connect', 'connect_thread_local', 'NoServerAvailable',
           'Connection']

DEFAULT_SERVER = 'localhost:9160'
LOWEST_COMPATIBLE_VERSION = 17

log = PycassaLogger().get_logger()

class NoServerAvailable(Exception):
    """Raised if all servers are currently marked dead."""
    pass

class ClientTransport(object):
    """Encapsulation of a client session."""

    def __init__(self, keyspace, server, framed_transport, timeout, credentials, recycle):
        self.server = server
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

        server_api_version = int(client.describe_version().split('.', 1)[0])
        assert (server_api_version >= LOWEST_COMPATIBLE_VERSION), \
                "Thrift API version incompatibility. " \
                 "(Server: %s, Lowest compatible version: %d)" % (server_api_version, LOWEST_COMPATIBLE_VERSION)

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
      :class:`TBufferedTransport`.  Cassandra 0.7.x uses framed transport, while
      Cassandra 0.6.x uses buffered.
    :type framed_transport: bool

    :param timeout: Timeout in seconds (e.g. 0.5). Default: ``None`` (it will stall forever)
    :type timeout: float

    :param retry_time: Minimum time in seconds until a failed server is reinstated
      (e.g. 0.5).  Default: 60.
    :type retry_time: float

    :param credentials: Dictionary of user credentials. Example:
      ``{'username':'jsmith', 'password':'havebadpass'}``
    :type credentials: dict

    :param recycle: Max time in seconds before an open connection is closed and replaced.
      Default: ``None`` (never recycle)
    :type recycle: float
                  
    :param round_robin: *DEPRECATED*
    :type round_robin: bool

    :rtype: :class:`.Connection`

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
    """A connection that gives access to raw Thrift calls."""

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
            except (UnavailableException, TimedOutException), exc:
                log.exception(exc)
                self.close()
                self._servers.mark_dead(conn.server)
                return _client_call(*args, **kwargs) # Retry
            except (Thrift.TException, socket.timeout, socket.error), exc:
                log.exception('Client error: %s', exc)
                self.close()
                raise
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
            self._local.conn = new_conn._local.conn
        else:
            self._connection = new_conn._connection

    def get_keyspace_description(self, keyspace=None, use_dict_for_col_metadata=False):
        """
        Describes the given keyspace.
        
        :param keyspace: The keyspace to describe. Defaults to the current keyspace.
        :type keyspace: str

        :param use_dict_for_col_metadata: whether or not store the column metadata as a
          dictionary instead of a list
        :type use_dict_for_col_metadata: bool

        :rtype: ``{column_family_name: CfDef}``

        """
        if keyspace is None:
            keyspace = self._keyspace

        ks_def = self.describe_keyspace(keyspace)
        cf_defs = dict()
        for cf_def in ks_def.cf_defs:
            cf_defs[cf_def.name] = cf_def
            if use_dict_for_col_metadata:
                old_metadata = cf_def.column_metadata
                new_metadata = dict()
                for datum in old_metadata:
                    new_metadata[datum.name] = datum
                cf_def.column_metadata = new_metadata
        return cf_defs

    def batch(self, *args, **kwargs):
        """
        Returns a mutator on this connection.

        """
        return Mutator(self, *args, **kwargs)

    def add_keyspace(self, ksdef, block=True, sample_period=0.25):
        """
        Adds a keyspace to the cluster.

        :param ksdef: the keyspace definition
        :type ksdef: :class:`~pycassa.cassandra.ttypes.KsDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.system_add_keyspace(ksdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def update_keyspace(self, ksdef, block=True, sample_period=0.25):
        """
        Updates a keyspace.

        :param ksdef: the new keyspace definition
        :type ksdef: :class:`~pycassa.cassandra.ttypes.KsDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.system_update_keyspace(ksdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def drop_keyspace(self, name, block=True, sample_period=0.25):
        """
        Drop a keyspace from the cluster.

        :param ksdef: the keyspace to drop
        :type ksdef: string

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.system_drop_keyspace(name)
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def add_column_family(self, cfdef, block=True, sample_period=0.25):
        """
        Adds a column family to a keyspace.

        :param cfdef: the column family definition
        :type cfdef: :class:`~pycassa.cassandra.ttypes.CfDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.system_add_column_family(cfdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def update_column_family(self, cfdef, block=True, sample_period=0.25):
        """
        Update a column family definition.

        .. warning:: If you do not set values for *everything* in the
                     :class:`.CfDef`, Cassandra will fill them in with
                     *bad* values.  You should get the existing
                     :class:`.CfDef` using :meth:`get_keyspace_description()`,
                     change what you want, and use that.

        Example usage::

            >>> conn = pycassa.connect('Keyspace1')
            >>> cfdef = conn.get_keyspace_description()['MyColumnFamily']
            >>> cfdef.memtable_throughput_in_mb = 256
            >>> conn.update_column_family(cfdef)

        :param cfdef: the column family definition
        :type cfdef: :class:`~pycassa.cassandra.ttypes.CfDef`

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.system_update_column_family(cfdef) 
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def drop_column_family(self, name, block=True, sample_period=0.25):
        """
        Drops a column family from the cluster.

        :param name: the column family to drop
        :type name: string

        :param block: wait until the cluster has the same schema version
          to return

        :param sample_period: when blocking, how often to check for schema
          agreement among the cluster (in seconds)
        :type sample_period: int

        :rtype: new schema version

        """
        schema_version = self.system_drop_column_family(name)
        if block:
            self._wait_for_agreement(sample_period)
        return schema_version

    def _wait_for_agreement(self, sample_period): 
        while True:
            versions = self.describe_schema_versions()
            if len(versions) == 1:
                break
            time.sleep(sample_period)
