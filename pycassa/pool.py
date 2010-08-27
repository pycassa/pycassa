# pool.py - Connection pooling for SQLAlchemy
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer
# mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Connection pooling for Cassandra connections.

Provides a number of connection pool implementations for a variety of
usage scenarios and thread behavior requirements imposed by the
application.
"""

import weakref, time, threading, random

import connection
import queue as pool_queue
from util import threading, as_interface, memoized_property
from logger import PycassaLogger

class Pool(object):
    """Abstract base class for connection pools."""

    def __init__(self, keyspace, server_list=['localhost:9160'],
                 credentials=None, recycle=-1, logging_name=None,
                 use_threadlocal=True, listeners=None):
        """
        Construct a Pool.

        :param keyspace: The keyspace this connection pool will
          make all connections to.

        :param server_list: A sequence of servers in the form 'host:port' that
          the pool will connect to.  The list will be randomly permuted before
          being used. server_list may also be a function that returns the
          sequence of servers.

        :param credentials: A dictionary containing 'username' and 'password'
          keys and appropriate string values.

        :param recycle: If set to non -1, number of seconds between
          connection recycling, which means upon checkout, if this
          timeout is surpassed the connection will be closed and
          replaced with a newly opened connection. Defaults to -1.

        :param logging_name:  String identifier which will be used within
          the "name" field of logging records generated within the 
          "pycassa.pool" logger. Defaults to a hexstring of the object's 
          id.

        :param use_threadlocal: If set to True, repeated calls to
          :meth:`connect` within the same application thread will be
          guaranteed to return the same connection object, if one has
          already been retrieved from the pool and has not been
          returned yet.  Offers a slight performance advantage at the
          cost of individual transactions by default.  The
          :meth:`unique_connection` method is provided to bypass the
          threadlocal behavior installed into :meth:`connect`.

        :param listeners: A list of
          :class:`~PoolListener`-like objects or
          dictionaries of callables that receive events when Cassandra
          connections are created, checked out and checked in to the
          pool.

        """
        if logging_name:
            self.logging_name = self._orig_logging_name = logging_name
        else:
            self._orig_logging_name = None
            self.logging_name = id(self)
            
        self._recycle = recycle
        self._use_threadlocal = use_threadlocal
        self.keyspace = keyspace
        self.credentials = credentials
        self._creator = self.create
        self._tlocal = threading.local()

        # Listener groups
        self.listeners = []
        self._on_connect = []
        self._on_checkout = []
        self._on_checkin = []
        self._on_close = []
        self._on_server_list = []
        self._on_pool_recreate = []
        self._on_pool_dispose = []
        self._on_pool_max = []

        self.add_listener(PycassaLogger())

        if listeners:
            for l in listeners:
                self.add_listener(l)

        self.set_server_list(server_list)

    def set_server_list(self, server_list):
        """
        Sets the server list that the pool will make connections to.

        :param server_list: A sequence of servers in the form 'host:port' that
          the pool will connect to.  The list will be randomly permuted before
          being used. server_list may also be a function that returns the
          sequence of servers.

        """

        if callable(server_list):
            self.server_list = list(server_list())
        else:
            self.server_list = list(server_list)

        # Randomly permute the array (trust me, it's uniformly random)
        n = len(self.server_list)
        for i in range(0, n):
            j = random.randint(i, n-1)
            temp = self.server_list[j]
            self.server_list[j] = self.server_list[i]
            self.server_list[i] = temp

        self._list_position = 0

        dic = self._get_dic()
        dic['server_list'] = self.server_list
        if self._on_server_list:
            for l in self._on_server_list:
                l.obtained_server_list(dic)


    def _create_connection(self):
        return _ConnectionRecord(self)

    def create(self):
        """Creates a new pycassa/Thrift connection."""
        server = self.server_list[self._list_position % len(self.server_list)]
        self._list_position += 1
        if self._use_threadlocal:
            return connection.connect(keyspace=self.keyspace, servers=[server],
                                      credentials=self.credentials)
        else:
            return connection.connect_unique(keyspace=self.keyspace,
                                             servers=[server],
                                             credentials=self.credentials)

    def recreate(self):
        """Return a new instance with identical creation arguments."""

        raise NotImplementedError()

    def dispose(self):
        """Dispose of this pool.

        This method leaves the possibility of checked-out connections
        remaining open, It is advised to not reuse the pool once dispose()
        is called, and to instead use a new pool constructed by the
        recreate() method.
        """

        raise NotImplementedError()

    def return_conn(self, record):
        """Returns a ConnectionRecord to the pool."""
        self._do_return_conn(record)

    def get(self):
        return self._do_get()

    def _do_get(self):
        raise NotImplementedError()

    def _do_return_conn(self, conn):
        raise NotImplementedError()

    def status(self):
        raise NotImplementedError()

    def add_listener(self, listener):
        """Add a ``PoolListener``-like object to this pool.

        ``listener`` may be an object that implements some or all of
        PoolListener, or a dictionary of callables containing implementations
        of some or all of the named methods in PoolListener.

        """

        listener = as_interface(listener,
            methods=('connect', 'checkout', 'checkin', 'close',
                     'obtained_server_list', 'pool_recreated',
                     'pool_disposed', 'close', 'pool_max'))

        self.listeners.append(listener)
        if hasattr(listener, 'connect'):
            self._on_connect.append(listener)
        if hasattr(listener, 'checkout'):
            self._on_checkout.append(listener)
        if hasattr(listener, 'checkin'):
            self._on_checkin.append(listener)
        if hasattr(listener, 'close'):
            self._on_close.append(listener)
        if hasattr(listener, 'obtained_server_list'):
            self._on_server_list.append(listener)
        if hasattr(listener, 'pool_recreated'):
            self._on_pool_recreate.append(listener)
        if hasattr(listener, 'pool_disposed'):
            self._on_pool_dispose.append(listener)
        if hasattr(listener, 'pool_max'):
            self._on_pool_max.append(listener)

    def _notify_on_pool_recreate(self):
        if self._on_pool_recreate:
            dic = self._get_dic()
            dic['level'] = 'info'
            for l in self._on_pool_recreate:
                l.pool_recreated(dic)

    def _notify_on_pool_dispose(self, status=""):
        if self._on_pool_dispose:
            dic = self._get_dic()
            dic['level'] = 'info'
            if status:
                dic['status'] = status
            for l in self._on_pool_dispose:
                l.pool_disposed(dic)

    def _notify_on_pool_max(self, pool_max):
        if self._on_pool_max:
            dic = self._get_dic()
            dic['level'] = 'info'
            dic['pool_max'] = pool_max
            for l in self._on_pool_max:
                l.pool_max(dic)

    def _notify_on_close(self, conn_record, msg="", error=None):
        if self._on_close:
            dic = self._get_dic()
            dic['level'] = 'info'
            dic['conn_record'] = conn_record
            if msg:
                dic['message'] = msg
            if error:
                dic['error'] = error
                dic['level'] = 'warn'
            for l in self._on_close:
                l.close(dic)

    def _notify_on_connect(self, conn_record, msg="", error=None):
        if self._on_connect:
            dic = self._get_dic()
            dic['level'] = 'info'
            dic['conn_record'] = conn_record
            if msg:
                dic['message'] = msg
            if error:
                dic['error'] = error
                dic['level'] = 'warn'
            for l in self._on_connect:
                l.connect(dic)

    def _notify_on_checkin(self, conn_record):
        if self._on_checkin:
            dic = self._get_dic()
            dic['level'] = 'info'
            dic['conn_record'] = conn_record
            for l in self._on_checkin:
                l.checkin(dic)

    def _notify_on_checkout(self, conn_record):
        if self._on_checkout:
            dic = self._get_dic()
            dic['level'] = 'info'
            dic['conn_record'] = conn_record
            for l in self._on_checkout:
                l.checkout(dic)


    def _get_dic(self):
        return {'pool_type': self.__class__,
                'pool_id': self.logging_name}

class _ConnectionRecord(object):
    """
    Contains a pycassa/Thrift connection throughout its lifecycle.

    """

    _IN_QUEUE = 0
    _CHECKED_OUT = 1
    _DISPOSED = 2

    def __init__(self, pool):
        self.__pool = pool
        self._lock = threading.Lock()
        self._state = _ConnectionRecord._CHECKED_OUT
        self._connection = self.__connect()
        self.info = {}
        self.__pool._notify_on_connect(self)

    def return_to_pool(self):
        self.__pool.return_conn(self)

    def get_connection(self):
        if self._connection is None:
            self._connection = self.__connect()
            self.info.clear()
            self.__pool._notify_on_connect(self)
        elif self.__pool._recycle > -1 and \
                time.time() - self.starttime > self.__pool._recycle:
            self.__pool._notify_on_recycle(self)
            self._close()
            self._connection = self.__connect()
            self.info.clear()
            self.__pool._notify_on_connect(self)
        return self._connection

    def ensure_connection(self):
        if self._connection is None:
            self.get_connection()
        self._connection._ensure_connection()

    def _checkin(self):
        try:
            self._lock.acquire()
            if self._state != _ConnectionRecord._CHECKED_OUT:
                raise InvalidRequestError("A connection has been returned to "
                        "the connection pool twice.")
            self._state = _ConnectionRecord._IN_QUEUE
        finally:
            self._lock.release()

    def _checkout(self):
        try:
            self._lock.acquire()
            if self._state != _ConnectionRecord._IN_QUEUE:
                raise InvalidRequestError("A connection has been checked "
                        "out twice.")
            self._state = _ConnectionRecord._CHECKED_OUT
        finally:
            self._lock.release()

    def _dispose(self):
        try:
            self._lock.acquire()
            if self._state == _ConnectionRecord._DISPOSED:
                raise InvalidRequestError("A connection has been disposed "
                        "twice.")
            self._state = _ConnectionRecord._DISPOSED
        finally:
            self._lock.release()

    def _in_queue(self):
        try:
            self._lock.acquire()
            ret = self._state == _ConnectionRecord._IN_QUEUE
        finally:
            self._lock.release()
        return ret

    def _close(self, e=None):
        """Closes the Record's connection; it will be reopened when
        ``get_connection()`` is called.

        ``info`` will persist until ``get_connection()`` is called.

        """
        self._dispose()

        if e is not None:
            msg = "Closing connection %r (reason: %s:%s)" % \
                self._connection, e.__class__.__name__, e
        else:
            msg = "Closing connection %r" % self._connection 

        try:
            if self._connection:
                self._connection.close()
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, e:
            self.__pool._notify_on_close(self, error=e)
            raise

        self._connection = None
        self.__pool._notify_on_close(self, msg=msg)

    def __connect(self):
        try:
            self.starttime = time.time()
            connection = self.__pool._creator()
            connection.connect()
            return connection
        except Exception, e:
            self.__pool._notify_on_connect(self, error=e)
            raise


class SingletonThreadPool(Pool):
    """A Pool that maintains one connection per thread.

    Maintains one connection per each thread, never moving a connection to a
    thread other than the one which it was created in.

    Options are the same as those of :class:`Pool`, as well as:

    :param pool_size: The number of threads in which to maintain connections 
        at once.  Defaults to five.
      
    """

    def __init__(self, pool_size=5, **kw):
        kw['use_threadlocal'] = True
        Pool.__init__(self, **kw)
        self._all_conns = set()
        self.size = pool_size

    def recreate(self):
        self._notify_on_pool_recreate()
        return SingletonThreadPool( 
            pool_size=self.size, 
            keyspace=self.keyspace,
            server_list=self.server_list,
            credentials=self.credentials,
            recycle=self._recycle, 
            logging_name=self._orig_logging_name,
            use_threadlocal=self._use_threadlocal, 
            listeners=self.listeners)

    def dispose(self):
        """Dispose of this pool."""

        for conn in self._all_conns:
            try:
                conn._close()
            except (SystemExit, KeyboardInterrupt):
                raise
        
        self._all_conns.clear()
        self._notify_on_pool_dispose()

    def status(self):
        return "SingletonThreadPool id:%d size: %d" % \
                            (id(self), len(self._all_conns))

    def _do_return_conn(self, conn):
        if hasattr(self._tlocal, 'current'):
            conn = self._tlocal.current()
            self._all_conns.discard(conn)
            del self._tlocal.current
            self._notify_on_checkin(conn)
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
            c.ensure_connection()
            self._tlocal.current = weakref.ref(c)
            self._all_conns.add(c)
        return c

class QueuePool(Pool):
    """A Pool that imposes a limit on the number of open connections."""

    def __init__(self, pool_size=5, max_overflow=10, timeout=30,
                 **kw):
        """
        Construct a QueuePool.

        :param pool_size: The size of the pool to be maintained,
          defaults to 5. This is the largest number of connections that
          will be kept persistently in the pool. Note that the pool
          begins with no connections; once this number of connections
          is requested, that number of connections will remain.
          ``pool_size`` can be set to 0 to indicate no size limit; to
          disable pooling, use a :class:`~pycassa.pool.NullPool`
          instead.

        :param max_overflow: The maximum overflow size of the
          pool. When the number of checked-out connections reaches the
          size set in pool_size, additional connections will be
          returned up to this limit. When those additional connections
          are returned to the pool, they are disconnected and
          discarded. It follows then that the total number of
          simultaneous connections the pool will allow is pool_size +
          `max_overflow`, and the total number of "sleeping"
          connections the pool will allow is pool_size. `max_overflow`
          can be set to -1 to indicate no overflow limit; no limit
          will be placed on the total number of concurrent
          connections. Defaults to 10.

        :param timeout: The number of seconds to wait before giving up
          on returning a connection. Defaults to 30.

        :param recycle: If set to non -1, number of seconds between
          connection recycling, which means upon checkout, if this
          timeout is surpassed the connection will be closed and
          replaced with a newly opened connection. Defaults to -1.

        :param use_threadlocal: If set to True, repeated calls to
          :meth:`connect` within the same application thread will be
          guaranteed to return the same connection object, if one has
          already been retrieved from the pool and has not been
          returned yet.  Offers a slight performance advantage at the
          cost of individual transactions by default.  The
          :meth:`unique_connection` method is provided to bypass the
          threadlocal behavior installed into :meth:`connect`.

        :param listeners: A list of
          :class:`PoolListener`-like objects or
          dictionaries of callables that receive events when Cassandra
          connections are created, checked out and checked in to the
          pool.

        """
        Pool.__init__(self, **kw)
        self._q = pool_queue.Queue(pool_size)
        self._overflow = 0 - pool_size
        self._max_overflow = max_overflow
        self._timeout = timeout
        self._overflow_lock = self._max_overflow > -1 and \
                                    threading.Lock() or None

    def recreate(self):
        self._notify_on_pool_recreate()
        return QueuePool(pool_size=self._q.maxsize, 
                          max_overflow=self._max_overflow,
                          timeout=self._timeout, 
                          keyspace=self.keyspace,
                          server_list=self.server_list,
                          credentials=self.credentials,
                          recycle=self._recycle, 
                          logging_name=self._orig_logging_name,
                          use_threadlocal=self._use_threadlocal,
                          listeners=self.listeners)

    def _do_return_conn(self, conn):
        try:
            if self._use_threadlocal:
                if hasattr(self._tlocal, 'current'):
                    if self._tlocal.current:
                        conn = self._tlocal.current()
                        self._tlocal.current = None
                        self._q.put(conn, False)
                        self._notify_on_checkin(conn)
            else:
                self._q.put(conn, False)
                self._notify_on_checkin(conn)
        except pool_queue.Full:
            if conn._in_queue():
                raise InvalidRequestError("A connection was returned to "
                        " the connection pool pull twice.")
            conn._close()
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

    def _do_get(self):
        if self._use_threadlocal:
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
            conn = self._q.get(wait, self._timeout)
            conn.ensure_connection()
            if self._use_threadlocal:
                self._tlocal.current = weakref.ref(conn)
            self._notify_on_checkout(conn)
            return conn
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
                            "connection timed out, timeout %d" % 
                            (self.size(), self.overflow(), self._timeout))

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

            # Notify listeners
            conn.ensure_connection()
            if self._use_threadlocal:
                self._tlocal.current = weakref.ref(conn)
            self._notify_on_checkout(conn)

            return conn

    def dispose(self):
        while True:
            try:
                conn = self._q.get(False)
                conn._close()
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
        return self._q.maxsize

    def checkedin(self):
        return self._q.qsize()

    def overflow(self):
        return self._overflow

    def checkedout(self):
        return self._q.maxsize - self._q.qsize() + self._overflow

class NullPool(Pool):
    """A Pool which does not pool connections.

    Instead it literally opens and closes the underlying Cassandra connection
    per each connection open/close.

    Reconnect-related functions such as ``recycle`` and connection
    invalidation are not supported by this Pool implementation, since
    no connections are held persistently.

    """

    def status(self):
        return "NullPool"

    def _do_return_conn(self, conn):
        conn._close()
        self._notify_on_checkin(conn)

    def _do_get(self):
        conn = self._create_connection()
        conn.ensure_connection()
        self._notify_on_checkout(conn)
        return conn

    def recreate(self):
        self._notify_on_pool_recreate()
        return NullPool(keyspace=self.keyspace,
            server_list=self.server_list,
            credentials=self.credentials, 
            recycle=self._recycle, 
            logging_name=self._orig_logging_name,
            use_threadlocal=self._use_threadlocal, 
            listeners=self.listeners)

    def dispose(self):
        self._notify_on_pool_dispose()


class StaticPool(Pool):
    """A Pool of exactly one connection, used for all requests.

    Reconnect-related functions such as ``recycle`` and connection
    invalidation (which is also used to support auto-reconnect) are not
    currently supported by this Pool implementation but may be implemented
    in a future release.

    """

    @memoized_property
    def _conn(self):
        return self._creator()

    @memoized_property
    def connection(self):
        return _ConnectionRecord(self)
        
    def status(self):
        return "StaticPool"

    def dispose(self):
        if '_conn' in self.__dict__:
            self._conn._close()
            self._conn = None
        self._notify_on_pool_dispose()

    def recreate(self):
        self._notify_on_pool_recreate()
        return self.__class__(keyspace=self.keyspace,
                              server_list=self.server_list,
                              credentials=self.credentials,
                              recycle=self._recycle,
                              use_threadlocal=self._use_threadlocal,
                              logging_name=self._orig_logging_name,
                              listeners=self.listeners)

    def _create_connection(self):
        return self._conn

    def _do_return_conn(self, conn):
        self._notify_on_checkin(conn)
        pass

    def _do_get(self):
        self._notify_on_checkout(self.connection)
        self.connection.ensure_connection()
        return self.connection

class AssertionPool(Pool):
    """A Pool that allows at most one checked out connection at any given
    time.

    This will raise an exception if more than one connection is checked out
    at a time.  Useful for debugging code that is using more connections
    than desired.

    """

    def __init__(self, **kw):
        self._conn = None
        self._checked_out = False
        Pool.__init__(self, **kw)
        
    def status(self):
        return "AssertionPool"

    def _do_return_conn(self, conn):
        if not self._checked_out:
            raise AssertionError("connection is not checked out")
        self._checked_out = False
        self._notify_on_checkin(conn)
        assert conn is self._conn

    def dispose(self):
        self._checked_out = False
        if self._conn:
            self._conn._close()
        self._notify_on_pool_dispose()

    def recreate(self):
        self._notify_on_pool_recreate()
        return AssertionPool(keyspace=self.keyspace,
                             server_list=self.server_list,
                             credentials=self.credentials,
                             logging_name=self._orig_logging_name,
                             listeners=self.listeners)
        
    def _do_get(self):
        if self._checked_out:
            self._notify_on_pool_max(pool_max=1)
            raise AssertionError("connection is already checked out")
            
        if not self._conn:
            self._conn = self._create_connection()
        
        self._checked_out = True
        self._conn.ensure_connection()
        self._notify_on_checkout(self._conn)
        return self._conn


class PoolListener(object):
    """Hooks into the lifecycle of connections in a ``Pool``.

    Usage::
    
        class MyListener(PoolListener):
            def connect(self, conn_record):
                '''perform connect operations'''
            # etc. 
            
        # create a new pool with a listener
        p = QueuePool(..., listeners=[MyListener()])
        
        # add a listener after the fact
        p.add_listener(MyListener())
        
    All of the standard connection :class:`~pycassa.pool.Pool` types can
    accept event listeners for key connection lifecycle events:
    creation, pool check-out and check-in.  There are no events fired
    when a connection closes.

    For any given Cassandra connection, there will be one ``connect``
    event, `n` number of ``checkout`` events, and either `n` or `n - 1`
    ``checkin`` events.  (If a ``Connection`` is detached from its
    pool via the ``detach()`` method, it won't be checked back in.)

    Events receive a ``_ConnectionRecord``, a long-lived internal
    ``Pool`` object that basically represents a "slot" in the
    connection pool.  ``_ConnectionRecord`` objects have one public
    attribute of note: ``info``, a dictionary whose contents are
    scoped to the lifetime of the Cassandra connection managed by the
    record.  You can use this shared storage area however you like.

    There is no need to subclass ``PoolListener`` to handle events.
    Any class that implements one or more of these methods can be used
    as a pool listener.  The ``Pool`` will inspect the methods
    provided by a listener object and add the listener to one or more
    internal event queues based on its capabilities.  In terms of
    efficiency and function call overhead, you're much better off only
    providing implementations for the hooks you'll be using.
    
    """

    def connect(self, conn_record):
        """Called once for each new Cassandra connection or Pool's ``creator()``.

        conn_record
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def checkout(self, conn_record):
        """Called when a connection is retrieved from the Pool.

        conn_record
          The ``_ConnectionRecord`` that persistently manages the connection

        """

    def checkin(self, conn_record):
        """Called when a connection returns to the pool.

        Note that the connection may be None if the connection has been closed.

        conn_record
          The ``_ConnectionRecord`` that persistently manages the connection

        """
 
    def close(self, conn_record, err):
        """Called when a connection is closed.

        conn_record
            The ``_ConnectionRecord`` that persistently manages the connection

        err
            A reason for closing the connection, if any.

        """

    def server_list_obtained(self, server_list):
        """Called when the pool finalizes its server list.
        
        server_list
            The randomly permuted list of servers.
        """

    def pool_recreated(self):
        """Called when a pool is recreated."""

    def pool_disposed(self):
        """Called when a pool is recreated."""

    def pool_max(self):
        """
        Called when an attempt is made to get a new connection from the
        pool, but the pool is already at its max size. 

        """


class DisconnectionError(Exception):
    """A disconnect is detected on a raw connection.

    This error is raised and consumed internally by a connection pool.  It can
    be raised by a ``PoolListener`` so that the host pool forces a disconnect.

    """

class NoConnectionAvailable(Exception):
    """Raised when there are no connections left in a pool."""


class InvalidRequestError(Exception):
    """Pycassa was asked to do something it can't do.

    This error generally corresponds to runtime state errors.

    """
