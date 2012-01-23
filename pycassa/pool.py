""" Connection pooling for Cassandra connections. """

import time
import threading
import random
import socket
import Queue

from thrift import Thrift
from connection import Connection
from logging.pool_logger import PoolLogger
from util import as_interface
from cassandra.ttypes import TimedOutException, UnavailableException

_BASE_BACKOFF = 0.01

__all__ = ['QueuePool', 'ConnectionPool', 'PoolListener',
           'ConnectionWrapper', 'AllServersUnavailable',
           'MaximumRetryException', 'NoConnectionAvailable',
           'InvalidRequestError']

class ConnectionWrapper(Connection):
    """
    A wrapper class for :class:`Connection`s that adds pooling functionality.

    """

    # These mark the state of the connection so that we can
    # check to see that they are not returned, checked out,
    # or disposed twice (or from the wrong state).
    _IN_QUEUE = 0
    _CHECKED_OUT = 1
    _DISPOSED = 2

    def __init__(self, pool, max_retries, *args, **kwargs):
        """
        Creates a wrapper for a :class:`~.pycassa.connection.Connection`
        object, adding pooling related functionality while still allowing
        access to the thrift API calls.

        These should not be created directly, only obtained through
        Pool's :meth:`~.ConnectionPool.get()` method.

        """
        self._pool = pool
        self._retry_count = 0
        self.max_retries = max_retries
        self.info = {}
        self.starttime = time.time()
        self.operation_count = 0
        self._state = ConnectionWrapper._CHECKED_OUT
        super(ConnectionWrapper, self).__init__(*args, **kwargs)
        self._pool._notify_on_connect(self)

        # For testing purposes only
        self._should_fail = False
        self._original_meth = self.send_batch_mutate

    def return_to_pool(self):
        """
        Returns this to the pool.

        This has the same effect as calling :meth:`ConnectionPool.put()`
        on the wrapper.

        """
        self._pool.put(self)

    def _checkin(self):
        if self._state == ConnectionWrapper._IN_QUEUE:
            raise InvalidRequestError("A connection has been returned to "
                    "the connection pool twice.")
        elif self._state == ConnectionWrapper._DISPOSED:
            raise InvalidRequestError("A disposed connection has been returned "
                    "to the connection pool.")
        self._state = ConnectionWrapper._IN_QUEUE

    def _checkout(self):
        if self._state != ConnectionWrapper._IN_QUEUE:
            raise InvalidRequestError("A connection has been checked "
                    "out twice.")
        self._state = ConnectionWrapper._CHECKED_OUT

    def _is_in_queue_or_disposed(self):
        ret = self._state == ConnectionWrapper._IN_QUEUE or \
              self._state == ConnectionWrapper._DISPOSED
        return ret

    def _dispose_wrapper(self, reason=None):
        if self._state == ConnectionWrapper._DISPOSED:
            raise InvalidRequestError("A connection has been disposed twice.")
        self._state = ConnectionWrapper._DISPOSED

        self.close()
        self._pool._notify_on_dispose(self, msg=reason)

    def _replace(self, new_conn_wrapper):
        """
        Get another wrapper from the pool and replace our own contents
        with its contents.

        """
        self.transport = new_conn_wrapper.transport
        self._iprot = new_conn_wrapper._iprot
        self._oprot = new_conn_wrapper._oprot
        self.info = new_conn_wrapper.info
        self.starttime = new_conn_wrapper.starttime
        self.operation_count = new_conn_wrapper.operation_count
        self._state = ConnectionWrapper._CHECKED_OUT
        self._should_fail = new_conn_wrapper._should_fail

    @classmethod
    def _retry(cls, f):
        def new_f(self, *args, **kwargs):
            self.operation_count += 1
            try:
                if kwargs.pop('reset', False):
                    self._pool._replace_wrapper() # puts a new wrapper in the queue
                    self._replace(self._pool.get()) # swaps out transport
                result = f(self, *args, **kwargs)
                self._retry_count = 0 # reset the count after a success
                return result
            except Thrift.TApplicationException, app_exc:
                self.close()
                self._pool._decrement_overflow()
                self._pool._clear_current()
                raise app_exc
            except (TimedOutException, UnavailableException, Thrift.TException,
                    socket.error, IOError, EOFError), exc:
                self._pool._notify_on_failure(exc, server=self.server, connection=self)

                self.close()
                self._pool._decrement_overflow()
                self._pool._clear_current()

                self._retry_count += 1
                if self.max_retries != -1 and self._retry_count > self.max_retries:
                    raise MaximumRetryException('Retried %d times. Last failure was %s: %s' %
                                                (self._retry_count, exc.__class__.__name__, exc))
                # Exponential backoff
                time.sleep(_BASE_BACKOFF * (2 ** self._retry_count))

                kwargs['reset'] = True
                return new_f(self, *args, **kwargs)

        new_f.__name__ = f.__name__
        return new_f

    def _fail_once(self, *args, **kwargs):
        if self._should_fail:
            self._should_fail = False
            raise TimedOutException
        else:
            return self._original_meth(*args, **kwargs)

    def get_keyspace_description(self, keyspace=None, use_dict_for_col_metadata=False):
        """
        Describes the given keyspace.

        If `use_dict_for_col_metadata` is ``True``, the column metadata will be stored
        as a dictionary instead of a list

        A dictionary of the form ``{column_family_name: CfDef}`` is returned.

        """
        if keyspace is None:
            keyspace = self.keyspace

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

retryable = ('get', 'get_slice', 'multiget_slice', 'get_count', 'multiget_count',
             'get_range_slices', 'get_indexed_slices', 'batch_mutate', 'add',
             'insert', 'remove', 'remove_counter', 'truncate', 'describe_keyspace')
for fname in retryable:
    new_f = ConnectionWrapper._retry(getattr(Connection, fname))
    setattr(ConnectionWrapper, fname, new_f)

class ConnectionPool(object):
    """A pool that maintains a queue of open connections."""

    _max_overflow = 0

    def _get_max_overflow(self):
        return self._max_overflow

    def _set_max_overflow(self, max_overflow):
        try:
            self._pool_lock.acquire()

            self._max_overflow = max_overflow
            self._overflow_enabled = max_overflow > 0 or max_overflow == -1
            if max_overflow == -1:
                self._max_conns = (2 ** 31) - 1
            else:
                self._max_conns = self._pool_size + max_overflow
        finally:
            self._pool_lock.release()

    max_overflow = property(_get_max_overflow, _set_max_overflow)
    """ Whether or not a new connection may be opened when the
    pool is empty is controlled by `max_overflow`.  This specifies how many
    additional connections may be opened after the pool has reached `pool_size`;
    keep in mind that these extra connections will be discarded upon checkin
    until the pool is below `pool_size`.  This may be set to -1 to indicate no
    overflow limit. The default value is 0, which does not allow for overflow. """

    pool_timeout = 30
    """ If ``pool_size + max_overflow`` connections have already been checked
    out, an attempt to retrieve a new connection from the pool will wait
    up to `pool_timeout` seconds for a connection to be returned to the
    pool before giving up. Note that this setting is only meaningful when you
    are accessing the pool concurrently, such as with multiple threads.
    This may be set to 0 to fail immediately or -1 to wait forever. 
    The default value is 30. """

    recycle = 10000
    """ After performing `recycle` number of operations, connections will
    be replaced when checked back in to the pool.  This may be set to
    -1 to disable connection recycling. The default value is 10,000. """

    max_retries = 5
    """ When an operation on a connection fails due to an :exc:`~.TimedOutException`
    or :exc:`~.UnavailableException`, which tend to indicate single or
    multiple node failure, the operation will be retried on different nodes
    up to `max_retries` times before an :exc:`~.MaximumRetryException` is raised.
    Setting this to 0 disables retries and setting to -1 allows unlimited retries. 
    The default value is 5. """

    logging_name = None
    """ By default, each pool identifies itself in the logs using ``id(self)``.
    If multiple pools are in use for different purposes, setting `logging_name` will
    help individual pools to be identified in the logs. """

    def __init__(self, keyspace,
                 server_list=['localhost:9160'],
                 credentials=None,
                 timeout=0.5,
                 use_threadlocal=True,
                 pool_size=5,
                 prefill=True,
                 **kwargs):
        """
        Constructs a pool that maintains a queue of open connections.

        All connections in the pool will be opened to `keyspace`.

        `server_list` is a sequence of servers in the form ``"host:port"`` that
        the pool will connect to. The port defaults to 9160 if excluded.
        The list will be randomly shuffled before being drawn from sequentially.
        `server_list` may also be a function that returns the sequence of servers.

        If authentication or authorization is required, `credentials` must
        be supplied.  This should be a dictionary containing 'username' and
        'password' keys with appropriate string values.

        `timeout` specifies in seconds how long individual connections will
        block before timing out. If set to ``None``, connections will never
        timeout.

        If `use_threadlocal` is set to ``True``, repeated calls to
        :meth:`get()` within the same application thread will
        return the same :class:`ConnectionWrapper` object if one is
        already checked out from the pool.  Be careful when setting `use_threadlocal`
        to ``False`` in a multithreaded application, especially with retries enabled.
        Synchronization may be required to prevent the connection from changing while
        another thread is using it.

        The pool will keep up `pool_size` open connections in the pool
        at any time.  When a connection is returned to the pool, the
        connection will be discarded is the pool already contains `pool_size`
        connections.  The total number of simultaneous connections the pool will
        allow is ``pool_size + max_overflow``,
        and the number of "sleeping" connections the pool will allow is ``pool_size``.

        A good choice for `pool_size` is a multiple of the number of servers
        passed to the Pool constructor.  If a size less than this is chosen,
        the last ``(len(server_list) - pool_size)`` servers may not be used until
        either overflow occurs, a connection is recycled, or a connection
        fails. Similarly, if a multiple of ``len(server_list)`` is not chosen,
        those same servers would have a decreased load. By default, overflow
        is disabled.

        If `prefill` is set to ``True``, `pool_size` connections will be opened
        when the pool is created.

        Example Usage:

        .. code-block:: python

            >>> pool = pycassa.ConnectionPool(keyspace='Keyspace1', server_list=['10.0.0.4:9160', '10.0.0.5:9160'], prefill=False)
            >>> cf = pycassa.ColumnFamily(pool, 'Standard1')
            >>> cf.insert('key', {'col': 'val'})
            1287785685530679

        """

        self._pool_threadlocal = use_threadlocal
        self.keyspace = keyspace
        self.credentials = credentials
        self.timeout = timeout
        if use_threadlocal:
            self._tlocal = threading.local()

        self._pool_size = pool_size
        self._q = Queue.Queue(pool_size)
        self._pool_lock = threading.Lock()
        self._current_conns = 0

        # Listener groups
        self.listeners = []
        self._on_connect = []
        self._on_checkout = []
        self._on_checkin = []
        self._on_dispose = []
        self._on_recycle = []
        self._on_failure = []
        self._on_server_list = []
        self._on_pool_recreate = []
        self._on_pool_dispose = []
        self._on_pool_max = []

        self.add_listener(PoolLogger())

        if "listeners" in kwargs:
            listeners = kwargs["listeners"]
            for l in listeners:
                self.add_listener(l)

        self.logging_name = kwargs.get("logging_name", None)
        if not self.logging_name:
            self.logging_name = id(self)

        if "max_overflow" not in kwargs:
            self._set_max_overflow(0)

        recognized_kwargs = ["pool_timeout", "recycle", "max_retries", "max_overflow"]
        for kw in recognized_kwargs:
            if kw in kwargs:
                setattr(self, kw, kwargs[kw])

        self.set_server_list(server_list)

        self._prefill = prefill
        if self._prefill:
            self.fill()

    def set_server_list(self, server_list):
        """
        Sets the server list that the pool will make connections to.

        `server_list` should be sequence of servers in the form ``"host:port"`` that
        the pool will connect to.  The list will be randomly permuted before
        being used. `server_list` may also be a function that returns the
        sequence of servers.
        """
        if callable(server_list):
            self.server_list = list(server_list())
        else:
            self.server_list = list(server_list)

        random.shuffle(self.server_list)
        self._list_position = 0
        self._notify_on_server_list(self.server_list)

    def _get_next_server(self):
        """
        Gets the next 'localhost:port' combination from the list of
        servers and increments the position. This is not thread-safe,
        but client-side load-balancing isn't so important that this is
        a problem.
        """
        if self._list_position >= len(self.server_list):
            self._list_position = 0
        server = self.server_list[self._list_position]
        self._list_position += 1
        return server

    def _create_connection(self):
        """Creates a ConnectionWrapper, which opens a
        pycassa.connection.Connection."""
        failure_count = 0
        while failure_count < 2 * len(self.server_list):
            try:
                server = self._get_next_server()
                wrapper = self._get_new_wrapper(server)
                return wrapper
            except (Thrift.TException, socket.error, IOError, EOFError), exc:
                self._notify_on_failure(exc, server)
                failure_count += 1
        raise AllServersUnavailable('An attempt was made to connect to each of the servers ' +
                                    'twice, but none of the attempts succeeded. The last failure was %s: %s' %
                                    (exc.__class__.__name__, exc))

    def fill(self):
        """
        Adds connections to the pool until at least ``pool_size`` connections
        exist, whether they are currently checked out from the pool or not.

        .. versionadded:: 1.2.0
        """
        try:
            self._pool_lock.acquire()
            while self._current_conns < self._pool_size:
                conn = self._create_connection()
                conn._checkin()
                self._q.put(conn, False)
                self._current_conns += 1
        finally:
            self._pool_lock.release()

    def _get_new_wrapper(self, server):
        return ConnectionWrapper(self, self.max_retries,
                                 self.keyspace, server,
                                 framed_transport=True,
                                 timeout=self.timeout,
                                 credentials=self.credentials)

    def _replace_wrapper(self):
        """Try to replace the connection."""
        if not self._q.full():
            try:
                conn = self._create_connection()
                conn._checkin()
                self._q.put(conn, False)
                self._current_conns += 1
            except Queue.Full:
                pass

    def _clear_current(self):
        """ If using threadlocal, clear our threadlocal current conn. """
        if self._pool_threadlocal:
            self._tlocal.current = None

    def put(self, conn):
        """ Returns a connection to the pool. """
        if self._pool_threadlocal:
            if hasattr(self._tlocal, 'current') and self._tlocal.current:
                conn = self._tlocal.current
                self._tlocal.current = None
            else:
                conn = None
        if conn:
            try:
                conn._retry_count = 0
                if conn._is_in_queue_or_disposed():
                    raise InvalidRequestError("Connection was already checked in or disposed")
                conn = self._put_conn(conn)
                self._notify_on_checkin(conn)
            except Queue.Full:
                self._notify_on_checkin(conn)
                conn._dispose_wrapper(reason="pool is already full")
                self._decrement_overflow()
    return_conn = put

    def _decrement_overflow(self):
        self._pool_lock.acquire()
        self._current_conns -= 1
        self._pool_lock.release()

    def _put_conn(self, conn):
        """
        Put a connection in the queue, recycling first if needed.
        This method does not handle the pool queue being full.
        """
        if self.recycle > -1 and conn.operation_count > self.recycle:
            new_conn = self._create_connection()
            self._notify_on_recycle(conn, new_conn)
            conn._dispose_wrapper(reason="recyling connection")
            conn = new_conn
        conn._checkin()
        self._q.put_nowait(conn)
        return conn

    def get(self):
        """ Gets a connection from the pool. """
        if self._pool_threadlocal:
            try:
                conn = None
                if self._tlocal.current:
                    conn = self._tlocal.current
                if conn:
                    return conn
            except AttributeError:
                pass
        try:
            self._pool_lock.acquire()
            if self._current_conns < self._pool_size:
                # The pool was not prefilled, and we need to add connections to reach pool_size
                conn = self._create_connection()
                self._current_conns += 1
            else:
                try:
                    # We don't want to waste time blocking if overflow is not enabled; similarly,
                    # if we're not at the max overflow, we can fail quickly and create a new
                    # connection
                    timeout = self.pool_timeout
                    if timeout == -1:
                        timeout = None
                        block = self._current_conns >= self._max_conns
                    elif timeout == 0:
                        block = False
                    else:
                        block = self._current_conns >= self._max_conns

                    conn = self._q.get(block, timeout)
                    conn._checkout()
                except Queue.Empty:
                    if self._current_conns < self._max_conns:
                        conn = self._create_connection()
                        self._current_conns += 1
                    else:
                        self._notify_on_pool_max(pool_max=self._max_conns)
                        size_msg = "size %d" % (self._pool_size, )
                        if self._overflow_enabled:
                            size_msg += "overflow %d" % (self._max_overflow)
                        message = "ConnectionPool limit of %s reached, unable to obtain connection after %d seconds" \
                                  % (size_msg, self.pool_timeout)
                        raise NoConnectionAvailable(message)
        finally:
            self._pool_lock.release()

        if self._pool_threadlocal:
            self._tlocal.current = conn
        self._notify_on_checkout(conn)
        return conn

    def execute(self, f, *args, **kwargs):
        """
        Get a connection from the pool, execute
        `f` on it with `*args` and `**kwargs`, return the
        connection to the pool, and return the result of `f`.
        """
        conn = None
        try:
            conn = self.get()
            return getattr(conn, f)(*args, **kwargs)
        finally:
            if conn:
                self.put(conn)

    def dispose(self):
        """ Closes all checked in connections in the pool. """
        while True:
            try:
                conn = self._q.get(False)
                conn._dispose_wrapper(
                        reason="Pool %s is being disposed" % id(self))
            except Queue.Empty:
                break

        self._overflow = 0 - self.size()
        self._notify_on_pool_dispose()

    def size(self):
        """ Returns the capacity of the pool. """
        return self._pool_size

    def checkedin(self):
        """ Returns the number of connections currently in the pool. """
        return self._q.qsize()

    def overflow(self):
        """ Returns the number of overflow connections that are currently open. """
        return max(self._current_conns - self._pool_size, 0)

    def checkedout(self):
        """ Returns the number of connections currently checked out from the pool. """
        return self._current_conns - self.checkedin()

    def add_listener(self, listener):
        """
        Add a :class:`PoolListener`-like object to this pool.

        `listener` may be an object that implements some or all of
        :class:`PoolListener`, or a dictionary of callables containing implementations
        of some or all of the named methods in :class:`PoolListener`.

        """

        listener = as_interface(listener,
            methods=('connection_created', 'connection_checked_out',
                     'connection_checked_in', 'connection_disposed',
                     'connection_recycled', 'connection_failed',
                     'obtained_server_list', 'pool_recreated',
                     'pool_disposed', 'pool_at_max'))

        self.listeners.append(listener)
        if hasattr(listener, 'connection_created'):
            self._on_connect.append(listener)
        if hasattr(listener, 'connection_checked_out'):
            self._on_checkout.append(listener)
        if hasattr(listener, 'connection_checked_in'):
            self._on_checkin.append(listener)
        if hasattr(listener, 'connection_disposed'):
            self._on_dispose.append(listener)
        if hasattr(listener, 'connection_recycled'):
            self._on_recycle.append(listener)
        if hasattr(listener, 'connection_failed'):
            self._on_failure.append(listener)
        if hasattr(listener, 'obtained_server_list'):
            self._on_server_list.append(listener)
        if hasattr(listener, 'pool_recreated'):
            self._on_pool_recreate.append(listener)
        if hasattr(listener, 'pool_disposed'):
            self._on_pool_dispose.append(listener)
        if hasattr(listener, 'pool_at_max'):
            self._on_pool_max.append(listener)

    def _notify_on_pool_recreate(self):
        if self._on_pool_recreate:
            dic = {'pool_id': self.logging_name,
                   'level': 'info'}
            for l in self._on_pool_recreate:
                l.pool_recreated(dic)

    def _notify_on_pool_dispose(self):
        if self._on_pool_dispose:
            dic = {'pool_id': self.logging_name,
                   'level': 'info'}
            for l in self._on_pool_dispose:
                l.pool_disposed(dic)

    def _notify_on_pool_max(self, pool_max):
        if self._on_pool_max:
            dic = {'pool_id': self.logging_name,
                   'level': 'info',
                   'pool_max': pool_max}
            for l in self._on_pool_max:
                l.pool_at_max(dic)

    def _notify_on_dispose(self, conn_record, msg=""):
        if self._on_dispose:
            dic = {'pool_id': self.logging_name,
                   'level': 'debug',
                   'connection': conn_record}
            if msg:
                dic['message'] = msg
            for l in self._on_dispose:
                l.connection_disposed(dic)

    def _notify_on_server_list(self, server_list):
        dic = {'pool_id': self.logging_name,
               'level': 'debug',
               'server_list': server_list}
        if self._on_server_list:
            for l in self._on_server_list:
                l.obtained_server_list(dic)

    def _notify_on_recycle(self, old_conn, new_conn):
        if self._on_recycle:
            dic = {'pool_id': self.logging_name,
                   'level': 'debug',
                   'old_conn': old_conn,
                   'new_conn': new_conn}
        for l in self._on_recycle:
            l.connection_recycled(dic)

    def _notify_on_connect(self, conn_record, msg="", error=None):
        if self._on_connect:
            dic = {'pool_id': self.logging_name,
                   'level': 'debug',
                   'connection': conn_record}
            if msg:
                dic['message'] = msg
            if error:
                dic['error'] = error
                dic['level'] = 'warn'
            for l in self._on_connect:
                l.connection_created(dic)

    def _notify_on_checkin(self, conn_record):
        if self._on_checkin:
            dic = {'pool_id': self.logging_name,
                   'level': 'debug',
                   'connection': conn_record}
            for l in self._on_checkin:
                l.connection_checked_in(dic)

    def _notify_on_checkout(self, conn_record):
        if self._on_checkout:
            dic = {'pool_id': self.logging_name,
                   'level': 'debug',
                   'connection': conn_record}
            for l in self._on_checkout:
                l.connection_checked_out(dic)

    def _notify_on_failure(self, error, server, connection=None):
        if self._on_failure:
            dic = {'pool_id': self.logging_name,
                   'level': 'info',
                   'error': error,
                   'server': server,
                   'connection': connection}
            for l in self._on_failure:
                l.connection_failed(dic)

QueuePool = ConnectionPool

class PoolListener(object):
    """Hooks into the lifecycle of connections in a :class:`ConnectionPool`.

    Usage::

        class MyListener(PoolListener):
            def connection_created(self, dic):
                '''perform connect operations'''
            # etc.

        # create a new pool with a listener
        p = ConnectionPool(..., listeners=[MyListener()])

        # or add a listener after the fact
        p.add_listener(MyListener())

    Listeners receive a dictionary that contains event information and
    is indexed by a string describing that piece of info.  For example,
    all event dictionaries include 'level', so dic['level'] will return
    the prescribed logging level.

    There is no need to subclass :class:`PoolListener` to handle events.
    Any class that implements one or more of these methods can be used
    as a pool listener.  The :class:`ConnectionPool` will inspect the methods
    provided by a listener object and add the listener to one or more
    internal event queues based on its capabilities.  In terms of
    efficiency and function call overhead, you're much better off only
    providing implementations for the hooks you'll be using.

    Each of the :class:`PoolListener` methods wil be called with a
    :class:`dict` as the single parameter. This :class:`dict` may
    contain the following fields:

        * `connection`: The :class:`ConnectionWrapper` object that persistently
          manages the connection

        * `message`: The reason this event happened

        * `error`: The :class:`Exception` that caused this event

        * `pool_id`: The id of the :class:`ConnectionPool` that this event came from

        * `level`: The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'

    Entries in the :class:`dict` that are specific to only one event type are
    detailed with each method.


    """

    def connection_created(self, dic):
        """Called once for each new Cassandra connection.

        Fields: `pool_id`, `level`, and `connection`.
        """

    def connection_checked_out(self, dic):
        """Called when a connection is retrieved from the Pool.

        Fields: `pool_id`, `level`, and `connection`.
        """

    def connection_checked_in(self, dic):
        """Called when a connection returns to the pool.

        Fields: `pool_id`, `level`, and `connection`.
        """

    def connection_disposed(self, dic):
        """Called when a connection is closed.

        ``dic['message']``: A reason for closing the connection, if any.

        Fields: `pool_id`, `level`, `connection`, and `message`.
        """

    def connection_recycled(self, dic):
        """Called when a connection is recycled.

        ``dic['old_conn']``: The :class:`ConnectionWrapper` that is being recycled

        ``dic['new_conn']``: The :class:`ConnectionWrapper` that is replacing it

        Fields: `pool_id`, `level`, `old_conn`, and `new_conn`.
        """

    def connection_failed(self, dic):
        """Called when a connection to a single server fails.

        ``dic['server']``: The server the connection was made to.

        Fields: `pool_id`, `level`, `error`, `server`, and `connection`.
        """
    def server_list_obtained(self, dic):
        """Called when the pool finalizes its server list.

        ``dic['server_list']``: The randomly permuted list of servers that the
        pool will choose from.

        Fields: `pool_id`, `level`, and `server_list`.
        """

    def pool_recreated(self, dic):
        """Called when a pool is recreated.

        Fields: `pool_id`, and `level`.
        """

    def pool_disposed(self, dic):
        """Called when a pool is disposed.

        Fields: `pool_id`, and `level`.
        """

    def pool_at_max(self, dic):
        """
        Called when an attempt is made to get a new connection from the
        pool, but the pool is already at its max size.

        ``dic['pool_max']``: The max number of connections the pool will
        keep open at one time.

        Fields: `pool_id`, `pool_max`, and `level`.
        """


class AllServersUnavailable(Exception):
    """Raised when none of the servers given to a pool can be connected to."""

class NoConnectionAvailable(Exception):
    """Raised when there are no connections left in a pool."""

class MaximumRetryException(Exception):
    """
    Raised when a :class:`ConnectionWrapper` has retried the maximum
    allowed times before being returned to the pool; note that all of
    the retries do not have to be on the same operation.
    """

class InvalidRequestError(Exception):
    """
    Pycassa was asked to do something it can't do.

    This error generally corresponds to runtime state errors.
    """
