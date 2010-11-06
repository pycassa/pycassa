# pool.py - Connection pooling for SQLAlchemy
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer
# mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""
Connection pooling for Cassandra connections.

Provides a number of connection pool implementations for a variety of
usage scenarios and thread behavior requirements imposed by the
application.

.. seealso:: :mod:`pycassa.connection`

"""

import weakref, time, threading, random

import connection
import queue as pool_queue
from logging.pool_logger import PoolLogger
from util import as_interface
from cassandra.ttypes import TimedOutException, UnavailableException
from thrift import Thrift

import threading

__all__ = ['Pool', 'QueuePool', 'SingletonThreadPool', 'StaticPool',
           'NullPool', 'AssertionPool', 'PoolListener', 'ConnectionWrapper',
           'ImmutableConnectionWrapper', 'MutableConnectionWrapper',
           'ReplaceableConnectionWrapper', 'AllServersUnavailable',
           'MaximumRetryException', 'NoConnectionAvailable',
           'InvalidRequestError']

class Pool(object):
    """An abstract base class for all other pools."""

    def __init__(self, keyspace, server_list=['localhost:9160'],
                 credentials=None, timeout=0.5, logging_name=None,
                 use_threadlocal=True, listeners=[]):
        """
        Construct an instance of the abstract base class :class:`Pool`.  This
        should not be called directly, only by subclass :meth:`__init__()`
        methods.

        :param keyspace: The keyspace this connection pool will
          make all connections to.

        :param server_list: A sequence of servers in the form 'host:port' that
          the pool will connect to.  The list will be randomly permuted before
          being used. `server_list` may also be a function that returns the
          sequence of servers.

        :param credentials: A dictionary containing 'username' and 'password'
          keys and appropriate string values.

        :param timeout: The amount of time in seconds before a connection
          will time out.  Defaults to 0.5 (half a second).

        :param logging_name:  String identifier which will be used within
          the "name" field of logging records generated within the
          "pycassa.pool" logger. Defaults to ``id(pool)``.

        :param use_threadlocal: If set to ``True``, repeated calls to
          :meth:`get()` within the same application thread will
          return the same :class:`ConnectionWrapper`
          object, if one has already been retrieved from the pool and
          has not been returned yet.

        :param listeners: A list of
          :class:`PoolListener`-like objects or
          dictionaries of callables that receive events when
          pool or connection events occur.

        """
        if logging_name:
            self.logging_name = self._orig_logging_name = logging_name
        else:
            self._orig_logging_name = None
            self.logging_name = id(self)

        self._pool_threadlocal = use_threadlocal
        self.keyspace = keyspace
        self.credentials = credentials
        self.timeout = timeout
        self._tlocal = threading.local()

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

        for l in listeners:
            self.add_listener(l)

        self.set_server_list(server_list)

    def set_server_list(self, server_list):
        """
        Sets the server list that the pool will make connections to.

        :param server_list: A sequence of servers in the form 'host:port' that
          the pool will connect to.  The list will be randomly permuted before
          being used. `server_list` may also be a function that returns the
          sequence of servers.

        """

        if callable(server_list):
            self.server_list = list(server_list())
        else:
            self.server_list = list(server_list)

        assert len(self.server_list) > 0

        # Randomly permute the array (trust me, it's uniformly random)
        n = len(self.server_list)
        for i in range(0, n):
            j = random.randint(i, n-1)
            temp = self.server_list[j]
            self.server_list[j] = self.server_list[i]
            self.server_list[i] = temp

        self._list_position = 0
        self._notify_on_server_list(self.server_list)

    def _get_next_server(self):
        """
        Gets the next 'localhost:port' combination from the list of
        servers and increments the position. This is not thread-safe,
        but client-side load-balancing isn't so important that this is
        a problem.
        """
        server = self.server_list[self._list_position % len(self.server_list)]
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
            except connection.NoServerAvailable, exc:
                self._notify_on_failure(exc, server)
                failure_count += 1
        raise AllServersUnavailable('An attempt was made to connect to each of the servers '
                'twice, but none of the attempts succeeded.')

    def _get_new_wrapper(self, server):
        raise NotImplementedError()

    def recreate(self):
        """Return a new instance with identical creation arguments."""

        raise NotImplementedError()

    def dispose(self):
        """
        Dispose of this pool.

        This method leaves the possibility of checked-out connections
        remaining open, It is advised to not reuse the pool once
        :meth:`dispose()` is called, and to instead use a new pool
        constructed by :meth:`recreate()`.

        """
        raise NotImplementedError()

    def return_conn(self, record):
        """
        Return a ConnectionWrapper to the pool.

        :param record: The :class:`ConnectionWrapper` to retrun to the pool.

        """
        self._do_return_conn(record)

    def get(self):
        """
        Get a :class:`ConnectionWrapper` from the pool.

        """
        return self._do_get()

    def _do_get(self):
        raise NotImplementedError()

    def _do_return_conn(self, conn):
        raise NotImplementedError()

    def status(self):
        raise NotImplementedError()

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
            dic = self._get_dic()
            dic['level'] = 'info'
            for l in self._on_pool_recreate:
                l.pool_recreated(dic)

    def _notify_on_pool_dispose(self):
        if self._on_pool_dispose:
            dic = self._get_dic()
            dic['level'] = 'info'
            for l in self._on_pool_dispose:
                l.pool_disposed(dic)

    def _notify_on_pool_max(self, pool_max):
        if self._on_pool_max:
            dic = self._get_dic()
            dic['level'] = 'info'
            dic['pool_max'] = pool_max
            for l in self._on_pool_max:
                l.pool_at_max(dic)

    def _notify_on_dispose(self, conn_record, msg="", error=None):
        if self._on_dispose:
            dic = self._get_dic()
            dic['level'] = 'debug'
            dic['connection'] = conn_record
            if msg:
                dic['message'] = msg
            if error:
                dic['error'] = error
                dic['level'] = 'warn'
            for l in self._on_dispose:
                l.connection_disposed(dic)

    def _notify_on_server_list(self, server_list):
        dic = self._get_dic()
        dic['level'] = 'debug'
        dic['server_list'] = server_list
        if self._on_server_list:
            for l in self._on_server_list:
                l.obtained_server_list(dic)

    def _notify_on_recycle(self, old_conn, new_conn):
        if self._on_recycle:
            dic = self._get_dic()
            dic['level'] = 'debug'
            dic['old_conn'] = old_conn
            dic['new_conn'] = new_conn
        for l in self._on_recycle:
            l.connection_recycled(dic)

    def _notify_on_connect(self, conn_record, msg="", error=None):
        if self._on_connect:
            dic = self._get_dic()
            dic['level'] = 'debug'
            dic['connection'] = conn_record
            if msg:
                dic['message'] = msg
            if error:
                dic['error'] = error
                dic['level'] = 'warn'
            for l in self._on_connect:
                l.connection_created(dic)

    def _notify_on_checkin(self, conn_record):
        if self._on_checkin:
            dic = self._get_dic()
            dic['level'] = 'debug'
            dic['connection'] = conn_record
            for l in self._on_checkin:
                l.connection_checked_in(dic)

    def _notify_on_checkout(self, conn_record):
        if self._on_checkout:
            dic = self._get_dic()
            dic['level'] = 'debug'
            dic['connection'] = conn_record
            for l in self._on_checkout:
                l.connection_checked_out(dic)

    def _notify_on_failure(self, error, server, connection=None):
        if self._on_failure:
            dic = self._get_dic()
            dic['level'] = 'info'
            dic['error'] = error
            dic['server'] = server
            dic['connection'] = connection
            for l in self._on_failure:
                l.connection_failed(dic)

    def _get_dic(self):
        return {'pool_type': self.__class__.__name__,
                'pool_id': self.logging_name}


class ConnectionWrapper(connection.Connection):
    """
    A wrapper class for :class:`Connection`s that adds pooling functionality.

    """

    # These mark the state of the connection so that we can
    # check to see that they are not returned, checked out,
    # or disposed twice (or from the wrong state).
    _IN_QUEUE = 0
    _CHECKED_OUT = 1
    _DISPOSED = 2

    def __init__(self, pool, *args, **kwargs):
        """
        Creates a wrapper for a :class:`pycassa.connection.Connection`
        object, adding pooling related functionality while still allowing
        access to the thrift API calls.

        These should not be created directly, only obtained through
        Pool's :meth:`~.Pool.get()` method.

        """
        self._pool = pool
        self._lock = threading.Lock()
        self.info = {}
        self.starttime = time.time()
        self.operation_count = 0
        self._state = ConnectionWrapper._CHECKED_OUT
        super(ConnectionWrapper, self).__init__(*args, **kwargs)
        self.connect()
        self._pool._notify_on_connect(self)

    def return_to_pool(self):
        """
        Returns this to the pool.

        This has the same effect as calling :meth:`Pool.return_conn()`
        on the wrapper.

        """
        self._pool.return_conn(self)

    def _checkin(self):
        try:
            self._lock.acquire()
            if self._state != ConnectionWrapper._CHECKED_OUT:
                raise InvalidRequestError("A connection has been returned to "
                        "the connection pool twice.")
            self._state = ConnectionWrapper._IN_QUEUE
        finally:
            self._lock.release()

    def _checkout(self):
        try:
            self._lock.acquire()
            if self._state != ConnectionWrapper._IN_QUEUE:
                raise InvalidRequestError("A connection has been checked "
                        "out twice.")
            self._state = ConnectionWrapper._CHECKED_OUT
        finally:
            self._lock.release()

    def _in_queue(self):
        try:
            self._lock.acquire()
            ret = self._state == ConnectionWrapper._IN_QUEUE
        finally:
            self._lock.release()
        return ret

    def _dispose_wrapper(self, reason=None):
        try:
            self._lock.acquire()
            if self._state == ConnectionWrapper._DISPOSED:
                raise InvalidRequestError("A connection has been disposed "
                        "twice.")
            self._state = ConnectionWrapper._DISPOSED
        finally:
            self._lock.release()

        self.close()
        self._pool._notify_on_dispose(self, msg=reason)

    def __getattr__(self, attr):
        raise NotImplementedError()

class ImmutableConnectionWrapper(ConnectionWrapper):
    """A connection wrapper that may not be altered."""

    def __init__(self, pool, *args, **kwargs):
        """
        Create a ConnectionWrapper that does not support retries through replacing
        one wrapper with another or by swapping out the lower-level
        :class:`pycassa.connection.Connection`.

        This is currently only used by :class:`StaticPool`.  Here, the connection
        is immutable because multiple threads may be using the same connection
        at the same time.

        These should not be created directly.

        """
        super(ImmutableConnectionWrapper, self).__init__(pool, *args, **kwargs)

    def __getattr__(self, attr):
        def _client_call(*args, **kwargs):
            self.operation_count += 1
            try:
                conn = self._ensure_connection()
                return getattr(conn.client, attr)(*args, **kwargs)
            except (TimedoutException, UnavailableException), exc:
                self._pool._notify_on_failure(exc, server=self._servers._servers[0],
                                              connection=self)
                raise
        setattr(self, attr, _client_call)
        return getattr(self, attr)

class ReplaceableConnectionWrapper(ConnectionWrapper):
    """A connection wrapper that may be replaced by another wrapper."""

    def __init__(self, pool, max_retries, *args, **kwargs):
        """
        Create a ConnectionWrapper that supports retries by obtaining another
        wrapper from the pool and swapping all contents with it.

        Caution should be used when this is used with ``use_threadlocal=False``.

        These should not be created directly.

        """
        super(ReplaceableConnectionWrapper, self).__init__(pool, *args, **kwargs)
        self._retry_count = 0
        self._max_retries = max_retries

    def _replace(self, new_conn_wrapper):
        """
        Get another wrapper from the pool and replace our own contents
        with its contents.

        """
        super(ConnectionWrapper, self)._replace(new_conn_wrapper)
        self._lock = new_conn_wrapper._lock
        self._info = new_conn_wrapper.info
        self._starttime = new_conn_wrapper.starttime
        self.operation_count = new_conn_wrapper.operation_count
        self._state = ConnectionWrapper._CHECKED_OUT

    def __getattr__(self, attr):
        def _client_call(*args, **kwargs):
            self.operation_count += 1
            try:
                conn = self._ensure_connection()
                return getattr(conn.client, attr)(*args, **kwargs)
            except (TimedOutException, UnavailableException), exc:
                self._pool._notify_on_failure(exc, server=self._servers._servers[0],
                                              connection=self)

                self._retry_count += 1
                if self._max_retries != -1 and self._retry_count > self._max_retries:
                    raise MaximumRetryException('Retried %d times' % self._retry_count)

                self.close()

                # If using threadlocal, we have to wipe out 'current' so that
                # the pool's get() won't return self
                if self._pool._pool_threadlocal:
                    self._pool._tlocal.current = None

                if hasattr(self._pool, '_replace_wrapper'):
                    self._pool._replace_wrapper()
                self._replace(self._pool.get())
                return self.__getattr__(attr)(*args, **kwargs)
        setattr(self, attr, _client_call)
        return getattr(self, attr)

class MutableConnectionWrapper(ConnectionWrapper):
    """A connection wrapper that may be altered."""

    def __init__(self, pool, max_retries, *args, **kwargs):
        """
        Create a :class:`ConnectionWrapper` that supports retries by
        opening a new connection to the next server in Pool's list.

        Caution should be used when this is used with ``use_threadlocal=False``.

        These should not be created directly.

        """
        super(MutableConnectionWrapper, self).__init__(pool, *args, **kwargs)
        self._retry_count = 0
        self._max_retries = max_retries

    def _replace_conn(self):
        """
        Try getting servers from Pool's list and open connections to them
        until one succeeds or we have failed enough times; if we succeed,
        swap the contents of our pycassa.connection.Connection attributes with
        that connection's.

        """
        self.close()
        failure_count = 0
        while failure_count < 2 * len(self._pool.server_list):
            try:
                new_serv = self._pool._get_next_server()
                new_conn = connection.Connection(self._pool.keyspace, [new_serv],
                                      credentials=self._pool.credentials,
                                      use_threadlocal=self._pool._pool_threadlocal)
                new_conn.connect()
                super(MutableConnectionWrapper, self)._replace(new_conn)
                return
            except (TimedOutException, UnavailableException,
                    Thrift.TException, NoServerAvailable), exc:
                self._pool._notify_on_failure(exc, server=new_serv, 
                                              connection=new_conn)
                failure_count += 1
        raise AllServersUnavailable('An attempt was made to connect to each of the servers '
                'twice, but none of the attempts succeeded.')

    def __getattr__(self, attr):
        def _client_call(*args, **kwargs):
            self.operation_count += 1
            try:
                conn = self._ensure_connection()
                return getattr(conn.client, attr)(*args, **kwargs)
            except TimedOutException, exc:
                self._pool._notify_on_failure(exc, server=self._servers._servers[0],
                                              connection=self)
                self._retry_count += 1
                if self._max_retries != -1 and self._retry_count > self._max_retries:
                    raise MaximumRetryException('Retried %d times' % self._retry_count)
                self._replace_conn()
                return self.__getattr__(attr)(*args, **kwargs)
        setattr(self, attr, _client_call)
        return getattr(self, attr)

class QueuePool(Pool):
    """A pool that maintains a queue of open connections."""

    def __init__(self, pool_size=5, max_overflow=10,
                 pool_timeout=30, recycle=10000, max_retries=5,
                 prefill=True, *args, **kwargs):
        """
        Construct a Pool that maintains a queue of open connections.

        This is typically what you want to use for connection pooling.

        Be careful when using a QueuePool with ``use_threadlocal=False``,
        especially with retries enabled.  Synchronization may be required to
        prevent the connection from changing while another thread is using it.

        All of the parameters for :meth:`Pool.__init__()` are available, as
        well as the following:

        :param pool_size: The size of the pool to be maintained,
          defaults to 5. This is the largest number of connections that
          will be kept in the pool at one time.

          A good choice for this is usually a multiple of the number of servers
          passed to the Pool constructor.  If a size less than this is chosen,
          the last ``(len(server_list) - pool_size)`` servers may not be used until
          either overflow occurs, a connection is recycled, or a connection
          fails. Similarly, if a multiple of ``len(server_list)`` is not chosen,
          those same servers would have a decreased load.

        :param max_overflow: The maximum overflow size of the
          pool. When the number of checked-out connections reaches the
          size set in pool_size, additional connections will be
          returned up to this limit. When those additional connections
          are returned to the pool, they are disconnected and
          discarded. It follows then that the total number of
          simultaneous connections the pool will allow is
          ``pool_size + max_overflow``, and the total number of "sleeping"
          connections the pool will allow is ``pool_size``. `max_overflow`
          can be set to -1 to indicate no overflow limit; no limit
          will be placed on the total number of concurrent
          connections. Defaults to 10.

        :param pool_timeout: The number of seconds to wait before giving up
          on returning a connection. Defaults to 30.

        :param recycle: If set to non -1, number of operations between
          connection recycling, which means upon checkin, if this
          this many thrift operations have been performed,
          the connection will be closed and replaced with a newly opened
          connection if necessary. Defaults to 10000.

        :param max_retries: If set to non -1, the number times a connection
          can failover before an Exception is raised. Setting to 0 disables
          retries and setting to -1 allows unlimited retries. Defaults to 5.

        :param prefill: If ``True``, the pool creates ``pool_size`` connections
          upon creation and adds them to the queue.  Default is ``True``.

        Example::

            >>> pool = pycassa.QueuePool(keyspace='Keyspace1', server_list=['10.0.0.4:9160', '10.0.0.5:9160'], prefill=False)
            >>> conn = pool.get()
            >>> cf = pycassa.ColumnFamily(conn, 'Standard1')
            >>> cf.insert('key', {'col': 'val'})
            1287785685530679
            >>> conn.return_to_pool()

        """
        Pool.__init__(self, *args, **kwargs)
        self._pool_size = pool_size
        self._q = pool_queue.Queue(pool_size)
        self._max_overflow = max_overflow
        self._pool_timeout = pool_timeout
        self._recycle = recycle
        self._max_retries = max_retries
        self._prefill = prefill
        self._overflow_lock = self._max_overflow > -1 and \
                                    threading.Lock() or None
        if prefill:
            self._overflow = 0
            for i in range(pool_size):
                self._q.put(self._create_connection(), False)
        else:
            self._overflow = 0 - pool_size

    def recreate(self):
        self._notify_on_pool_recreate()
        return QueuePool(pool_size=self._q.maxsize,
                         max_overflow=self._max_overflow,
                         pool_timeout=self._pool_timeout,
                         keyspace=self.keyspace,
                         server_list=self.server_list,
                         credentials=self.credentials,
                         timeout=self.timeout,
                         recycle=self._recycle,
                         max_retries=self._max_retries,
                         prefill=self._prefill,
                         logging_name=self._orig_logging_name,
                         use_threadlocal=self._pool_threadlocal,
                         listeners=self.listeners)

    def _get_new_wrapper(self, server):
        return ReplaceableConnectionWrapper(self, self._max_retries,
                                            self.keyspace, [server],
                                            credentials=self.credentials,
                                            timeout=self.timeout,
                                            use_threadlocal=self._pool_threadlocal)

    def _replace_wrapper(self):
        """Try to replace the connection."""
        if not self._q.full():
            try:
                self._q.put(self._create_connection(), False)
            except pool_queue.Full:
                pass

    def _do_return_conn(self, conn):
        try:
            if self._pool_threadlocal:
                if hasattr(self._tlocal, 'current'):
                    if self._tlocal.current:
                        conn = self._tlocal.current()
                        self._tlocal.current = None
                        conn._retry_count = 0
                        conn = self._put_conn(conn)
                        self._notify_on_checkin(conn)
            else:
                conn._retry_count = 0
                conn = self._put_conn(conn)
                self._notify_on_checkin(conn)
        except pool_queue.Full:
            if conn._in_queue():
                raise InvalidRequestError("A connection was returned to "
                        " the connection pool pull twice.")
            conn._dispose_wrapper(reason="pool is already full")
            if self._overflow_lock is None:
                self._overflow -= 1
                self._notify_on_checkin(conn)
            else:
                self._overflow_lock.acquire()
                try:
                    self._overflow -= 1
                    self._notify_on_checkin(conn)
                finally:
                    self._overflow_lock.release()

    def _put_conn(self, conn):
        """Put a connection in the queue, recycling if needed."""
        if self._recycle > -1 and conn.operation_count > self._recycle:
            new_conn = self._create_connection()
            self._notify_on_recycle(conn, new_conn)
            conn._dispose_wrapper(reason="recyling connection")
            self._q.put(new_conn, False)
            return new_conn
        else:
            self._q.put(conn, False)
            return conn

    def _do_get(self):
        if self._pool_threadlocal:
            try:
                conn = None
                if self._tlocal.current:
                    conn = self._tlocal.current()
                if conn:
                    return conn
            except AttributeError:
                pass
        try:
            wait = self._max_overflow > -1 and \
                        self._overflow >= self._max_overflow
            conn = self._q.get(wait, self._pool_timeout)
        except pool_queue.Empty:
            if self._max_overflow > -1 and \
                        self._overflow >= self._max_overflow:
                if not wait:
                    return self._do_get()
                else:
                    self._notify_on_pool_max(
                            pool_max=self.size() + self.overflow())
                    raise NoConnectionAvailable(
                            "QueuePool limit of size %d overflow %d reached, "
                            "connection timed out, pool_timeout %d" %
                            (self.size(), self.overflow(), self._pool_timeout))

            if self._overflow_lock is not None:
                self._overflow_lock.acquire()

            if self._max_overflow > -1 and \
                        self._overflow >= self._max_overflow:
                if self._overflow_lock is not None:
                    self._overflow_lock.release()
                return self._do_get()

            try:
                conn = self._create_connection()
                self._overflow += 1
            finally:
                if self._overflow_lock is not None:
                    self._overflow_lock.release()

        # Check to make sure the connection is good
        try:
            conn._ensure_connection()
        except connection.NoServerAvailable:
            self._q.put(self._create_connection(), False)
            return self._do_get()

        if self._pool_threadlocal:
            self._tlocal.current = weakref.ref(conn)
        self._notify_on_checkout(conn)
        return conn

    def dispose(self):
        while True:
            try:
                conn = self._q.get(False)
                conn._dispose_wrapper(
                        reason="Pool %s is being disposed" % id(self))
            except pool_queue.Empty:
                break

        self._overflow = 0 - self.size()
        self._notify_on_pool_dispose()

    def status(self):
        return "Pool size: %d  Connections in pool: %d "\
                "Current Overflow: %d Current Checked out "\
                "connections: %d" % (self.size(),
                                    self.checkedin(),
                                    self.overflow(),
                                    self.checkedout())

    def size(self):
        return self._pool_size

    def checkedin(self):
        return self._q.qsize()

    def overflow(self):
        return self._overflow

    def checkedout(self):
        return self._pool_size - self._q.qsize() + self._overflow

class SingletonThreadPool(Pool):
    """A Pool that maintains one connection per thread."""

    def __init__(self, pool_size=5, max_retries=5, *args, **kwargs):
        """
        Creates a Pool that maintains one connection per thread.

        Maintains one connection per each thread, never moving a connection to a
        thread other than the one which it was created in.

        All of the parameters for :meth:`Pool.__init__()` are available, as
        well as the following:

        :param pool_size: The number of threads in which to maintain connections
            at once.  Defaults to five.

        :param max_retries: If set to non -1, the number times a connection
          can failover before an Exception is raised. Setting to 0 disables
          retries and setting to -1 allows unlimited retries. Defaults to 5.

        """

        kwargs['use_threadlocal'] = True
        Pool.__init__(self, *args, **kwargs)
        self._all_conns = set()
        self.size = pool_size
        self._max_retries = max_retries

    def recreate(self):
        self._notify_on_pool_recreate()
        return SingletonThreadPool(
            pool_size=self.size,
            max_retries=self._max_retries,
            keyspace=self.keyspace,
            server_list=self.server_list,
            credentials=self.credentials,
            timeout=self.timeout,
            logging_name=self._orig_logging_name,
            use_threadlocal=self._pool_threadlocal,
            listeners=self.listeners)

    def dispose(self):
        for conn in self._all_conns:
            try:
                conn._dispose_wrapper()
            except (SystemExit, KeyboardInterrupt):
                raise

        self._all_conns.clear()
        self._notify_on_pool_dispose()

    def status(self):
        return "SingletonThreadPool id:%d size: %d" % \
                            (id(self), len(self._all_conns))

    def _get_new_wrapper(self, server):
        return MutableConnectionWrapper(self, self._max_retries,
                                        self.keyspace, [server],
                                        credentials=self.credentials,
                                        timeout=self.timeout,
                                        use_threadlocal=self._pool_threadlocal)

    def _do_return_conn(self, conn):
        if hasattr(self._tlocal, 'current'):
            conn = self._tlocal.current()
            self._all_conns.discard(conn)
            del self._tlocal.current
            self._notify_on_checkin(conn)
            conn._retry_count = 0
        pass

    def _do_get(self):
        try:
            c = self._tlocal.current()
            if c:
                return c
        except AttributeError:
            pass
        if len(self._all_conns) >= self.size:
            self._notify_on_pool_max(pool_max=self.size)
            raise NoConnectionAvailable()
        else:
            c = self._create_connection()
            self._tlocal.current = weakref.ref(c)
            self._all_conns.add(c)
        return c

class NullPool(Pool):
    """A Pool which does not pool connections."""

    def __init__(self, max_retries=5, *args, **kwargs):
        """
        Creates a :class:`Pool` which does not pool connections.

        Instead, it opens and closes the underlying Cassandra connection
        per each :meth:`~Pool.get()` and :meth:`~Pool.return_conn()`.

        ``NullPool`` supports retry behavior.

        Instead of using this with threadlocal storage, you should use a
        :class:`SingletonThreadPool`.

        All of the parameters for :meth:`Pool.__init__()` are available,
        as well as:

        :param max_retries: If set to non -1, the number times a connection
          can failover before an Exception is raised. Setting to 0 disables
          retries and setting to -1 allows unlimited retries. Defaults to 5.

        """
        Pool.__init__(self, *args, **kwargs)
        self._max_retries = max_retries

    def status(self):
        return "NullPool"

    def _get_new_wrapper(self, server):
        return ReplaceableConnectionWrapper(self, self._max_retries,
                                            self.keyspace, [server],
                                            credentials=self.credentials,
                                            timeout=self.timeout,
                                            use_threadlocal=self._pool_threadlocal)

    def _do_return_conn(self, conn):
        conn._dispose_wrapper()
        self._notify_on_checkin(conn)

    def _do_get(self):
        conn = self._create_connection()
        self._notify_on_checkout(conn)
        return conn

    def recreate(self):
        self._notify_on_pool_recreate()
        return NullPool(max_retries=self._max_retries,
                        keyspace=self.keyspace,
                        server_list=self.server_list,
                        credentials=self.credentials,
                        timeout=self.timeout,
                        logging_name=self._orig_logging_name,
                        use_threadlocal=self._pool_threadlocal,
                        listeners=self.listeners)

    def dispose(self):
        self._notify_on_pool_dispose()


class StaticPool(Pool):
    """A Pool of exactly one connection, used for all requests."""  

    def __init__(self, *args, **kwargs):
        """
        Creates a pool with exactly one connection that is used
        for all requests.

        Automatic retries are not currently supported.

        All of the parameters for :meth:`Pool.__init__()` are available.

        """
        Pool.__init__(self, *args, **kwargs)
        self._conn = self._create_connection()

    def status(self):
        return "StaticPool"

    def dispose(self):
        if '_conn' in self.__dict__:
            self._conn._dispose_wrapper()
            self._conn = None
        self._notify_on_pool_dispose()

    def recreate(self):
        self._notify_on_pool_recreate()
        return self.__class__(keyspace=self.keyspace,
                              server_list=self.server_list,
                              credentials=self.credentials,
                              timeout=self.timeout,
                              use_threadlocal=self._pool_threadlocal,
                              logging_name=self._orig_logging_name,
                              listeners=self.listeners)

    def _get_new_wrapper(self, server):
        return ImmutableConnectionWrapper(self, self.keyspace, [server],
                                  credentials=self.credentials,
                                  timeout=self.timeout,
                                  use_threadlocal=self._pool_threadlocal)


    def _do_return_conn(self, conn):
        self._notify_on_checkin(conn)
        pass

    def _do_get(self):
        self._notify_on_checkout(self._conn)
        try:
            self._conn._ensure_connection()
        except connection.NoServerAvailable:
            self._conn = self._create_connection()
        return self._conn

class AssertionPool(Pool):
    """A Pool that allows at most one checked out connection at any given
    time."""

    def __init__(self, max_retries=5, *args, **kwargs):
        """
        Creates a Pool that allows at most one checked out connection at any given
        time.

        This will raise an :exc:`AssertionError` if more than one connection is checked
        out at a time.  Useful for debugging code that is using more connections
        than desired.

        AssertionPools support automatic retries.

        All of the parameters for :meth:`Pool.__init__()` are available,
        as well as:

        :param max_retries: If set to non -1, the number times a connection
          can failover before an Exception is raised. Setting to 0 disables
          retries and setting to -1 allows unlimited retries. Defaults to 5.

        """

        Pool.__init__(self, *args, **kwargs)
        self._conn = None
        self._checked_out = False
        self._max_retries = max_retries

    def status(self):
        return "AssertionPool"

    def _get_new_wrapper(self, server):
        return MutableConnectionWrapper(self, self._max_retries,
                                        self.keyspace, [server],
                                        credentials=self.credentials,
                                        timeout=self.timeout,
                                        use_threadlocal=self._pool_threadlocal)

    def _do_return_conn(self, conn):
        if not self._checked_out:
            raise AssertionError("connection is not checked out")
        self._checked_out = False
        self._notify_on_checkin(conn)
        conn._retry_count = 0
        assert conn is self._conn

    def dispose(self):
        self._checked_out = False
        if self._conn:
            self._conn._dispose_wrapper()
        self._notify_on_pool_dispose()

    def recreate(self):
        self._notify_on_pool_recreate()
        return AssertionPool(max_retries=self._max_retries,
                             keyspace=self.keyspace,
                             server_list=self.server_list,
                             credentials=self.credentials,
                             timeout=self.timeout,
                             logging_name=self._orig_logging_name,
                             listeners=self.listeners)

    def _do_get(self):
        if self._checked_out:
            self._notify_on_pool_max(pool_max=1)
            raise AssertionError("connection is already checked out")

        if not self._conn:
            self._conn = self._create_connection()
        else:
            try:
                self._conn._ensure_connection()
            except connection.NoServerAvailable:
                self._conn = self._create_connection()

        self._checked_out = True
        self._notify_on_checkout(self._conn)
        return self._conn


class PoolListener(object):
    """Hooks into the lifecycle of connections in a :class:`Pool`.

    Usage::

        class MyListener(PoolListener):
            def connection_created(self, dic):
                '''perform connect operations'''
            # etc.

        # create a new pool with a listener
        p = QueuePool(..., listeners=[MyListener()])

        # or add a listener after the fact
        p.add_listener(MyListener())

    All of the standard connection :class:`Pool` types can
    accept event listeners for key connection lifecycle events.

    Listeners receive a dictionary that contains event information and
    is indexed by a string describing that piece of info.  For example,
    all event dictionaries include 'level', so dic['level'] will return
    the prescribed logging level.

    There is no need to subclass :class:`PoolListener` to handle events.
    Any class that implements one or more of these methods can be used
    as a pool listener.  The :class:`Pool` will inspect the methods
    provided by a listener object and add the listener to one or more
    internal event queues based on its capabilities.  In terms of
    efficiency and function call overhead, you're much better off only
    providing implementations for the hooks you'll be using.

    """

    def connection_created(self, dic):
        """Called once for each new Cassandra connection.

        dic['connection']
          The :class:`ConnectionWrapper` that persistently manages the connection

        dic['message']
            A reason for closing the connection, if any.

        dic['error']
            An error that occured while closing the connection, if any.

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """

    def connection_checked_out(self, dic):
        """Called when a connection is retrieved from the Pool.

        dic['connection']
          The :class:`ConnectionWrapper` that persistently manages the connection

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """

    def connection_checked_in(self, dic):
        """Called when a connection returns to the pool.

        Note that the connection may be None if the connection has been closed.

        dic['connection']
          The :class:`ConnectionWrapper` that persistently manages the connection

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """

    def connection_disposed(self, dic):
        """Called when a connection is closed.

        dic['connection']
            The :class:`ConnectionWrapper` that persistently manages the connection

        dic['message']
            A reason for closing the connection, if any.

        dic['error']
            An error that occured while closing the connection, if any.

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """

    def connection_recycled(self, dic):
        """Called when a connection is recycled.

        dic['old_conn']
            The :class:`ConnectionWrapper` that is being recycled

        dic['new_conn']
            The :class:`ConnectionWrapper` that is replacing it

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """

    def connection_failed(self, dic):
        """Called when a connection to a single server fails.

        dic['error']
          The connection error (Exception).

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """
    def server_list_obtained(self, dic):
        """Called when the pool finalizes its server list.

        dic['server_list']
            The randomly permuted list of servers.

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

       """

    def pool_recreated(self, dic):
        """Called when a pool is recreated.

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """

    def pool_disposed(self, dic):
        """Called when a pool is recreated.

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

        """

    def pool_at_max(self, dic):
        """
        Called when an attempt is made to get a new connection from the
        pool, but the pool is already at its max size.

        dic['pool_type']
          The type of pool the connection was created in; e.g. :class:`QueuePool`

        dic['pool_id']
          The logging name of the connection's pool (defaults to id(pool))

        dic['level']
          The prescribed logging level for this event.  Can be 'debug', 'info',
          'warn', 'error', or 'critical'.

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
