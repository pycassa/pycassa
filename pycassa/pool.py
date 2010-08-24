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

import log, connection
import queue as pool_queue
from util import threading, as_interface, memoized_property

class Pool(log.Identified):
    """Abstract base class for connection pools."""

    def __init__(self, 
                    keyspace, server_list=['localhost:9160'],
                    credentials=None, recycle=-1, echo=None, 
                    logging_name=None, use_threadlocal=False,
                    reset_on_return=True, listeners=None):
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
          "sqlalchemy.pool" logger. Defaults to a hexstring of the object's 
          id.

        :param echo: If True, connections being pulled and retrieved
          from the pool will be logged to the standard output, as well
          as pool sizing information.  Echoing can also be achieved by
          enabling logging for the "sqlalchemy.pool"
          namespace. Defaults to False.

        :param use_threadlocal: If set to True, repeated calls to
          :meth:`connect` within the same application thread will be
          guaranteed to return the same connection object, if one has
          already been retrieved from the pool and has not been
          returned yet.  Offers a slight performance advantage at the
          cost of individual transactions by default.  The
          :meth:`unique_connection` method is provided to bypass the
          threadlocal behavior installed into :meth:`connect`.

        :param reset_on_return: If true, reset the database state of
          connections returned to the pool.  This is typically a
          ROLLBACK to release locks and transaction resources.
          Disable at your own peril.  Defaults to True.

        :param listeners: A list of
          :class:`~sqlalchemy.interfaces.PoolListener`-like objects or
          dictionaries of callables that receive events when Cassandra
          connections are created, checked out and checked in to the
          pool.

        """
        if logging_name:
            self.logging_name = self._orig_logging_name = logging_name
        else:
            self._orig_logging_name = None
            
        self.logger = log.instance_logger(self, echoflag=echo)
        self._threadconns = threading.local()
        self._recycle = recycle
        self._use_threadlocal = use_threadlocal
        self._reset_on_return = reset_on_return
        self.keyspace = keyspace
        self.credentials = credentials
        self.echo = echo
        self._creator = self.create
        self._list_position = 0

        # Listener groups
        self.listeners = []
        self._on_connect = []
        self._on_first_connect = []
        self._on_checkout = []
        self._on_checkin = []
        self._on_close = []
        self._on_server_list = []
        self._on_pool_recreate = []
        self._on_pool_dispose = []
        self._on_pool_max = []

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

        if listeners:
            for l in listeners:
                self.add_listener(l)

        if self._on_server_list:
            for l in self._on_server_list:
                l.obtained_server_list(self.server_list)

    def _create_connection(self):
        return _ConnectionRecord(self)

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
        if self._use_threadlocal and hasattr(self._threadconns, "current"):
            del self._threadconns.current
        self._do_return_conn(record)
        if self._on_checkin:
            for l in self._on_checkin:
                l.checkin(record)

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
            methods=('connect', 'first_connect', 'checkout', 'checkin',
                     'close', 'obtained_server_list', 'pool_recreated',
                     'pool_disposed', 'close', 'pool_max'))

        self.listeners.append(listener)
        if hasattr(listener, 'connect'):
            self._on_connect.append(listener)
        if hasattr(listener, 'first_connect'):
            self._on_first_connect.append(listener)
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

    def create(self):
        """Creates a new pycassa/Thrift connection."""
        server = self.server_list[self._list_position % len(self.server_list)]
        self._list_position = self._list_position + 1
        return connection.connect(keyspace=self.keyspace, servers=[server],
                                  credentials=self.credentials)

    def _notify_on_pool_recreate(self):
        if self._on_pool_recreate:
            for l in self._on_pool_recreate:
                l.pool_recreated()

    def _notify_on_pool_dispose(self):
        if self._on_pool_dispose:
            for l in self._on_pool_dispose:
                l.pool_disposed()

    def _notify_on_pool_max(self):
        if self._on_pool_max:
            for l in self._on_pool_max:
                l.pool_max()

    def _notify_on_close(self, conn_record, e):
        if self._on_close:
            for l in self._on_close:
                l.close(conn_record, e)


class _ConnectionRecord(object):
    """
    Contains a pycassa/Thrift connection throughout its lifecycle.

    """
    def __init__(self, pool):
        self.__pool = pool
        self._connection = self.__connect()
        self.info = {}
        ls = pool.__dict__.pop('_on_first_connect', None)
        if ls is not None:
            for l in ls:
                l.first_connect(self)
        if pool._on_connect:
            for l in pool._on_connect:
                l.connect(self)

    def close(self, e=None):
        """Closes the Record's connection; it will be reopened when
        ``get_connection()`` is called.

        ``info`` will persist until ``get_connection()`` is called.

        """
        if e is not None:
            msg = "Closing connection %r (reason: %s:%s)" % \
                self._connection, e.__class__.__name__, e
        else:
            msg = "Closing connection %r" % self._connection 
        self.__pool.logger.info(msg)
        self.__pool._notify_on_close(self, msg)
        self.__close()
        self._connection = None

    def get_connection(self):
        if self._connection is None:
            self._connection = self.__connect()
            self.info.clear()
            if self.__pool._on_connect:
                for l in self.__pool._on_connect:
                    l.connect(self._connection, self)
        elif self.__pool._recycle > -1 and \
                time.time() - self.starttime > self.__pool._recycle:
            self.__pool.logger.info(
                    "Connection %r exceeded timeout; recycling",
                    self._connection)
            self.__close()
            self._connection = self.__connect()
            self.info.clear()
            if self.__pool._on_connect:
                for l in self.__pool._on_connect:
                    l.connect(self._connection, self)
        return self._connection

    def __close(self):
        try:
            self.__pool.logger.debug("Closing connection %r", self._connection)
            self._connection.close()
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, e:
            self.__pool.logger.debug(
                        "Connection %r threw an error on close: %s",
                        self._connection, e)

    def __connect(self):
        try:
            self.starttime = time.time()
            connection = self.__pool._creator()
            self.__pool.logger.debug("Created new connection %r", connection)
            return connection
        except Exception, e:
            self.__pool.logger.debug("Error on connect(): %s", e)
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
        self._conn = threading.local()
        self._all_conns = set()
        self.size = pool_size

    def recreate(self):
        self.logger.info("Pool recreating")
        self._notify_on_pool_recreate()
        return SingletonThreadPool( 
            pool_size=self.size, 
            keyspace=self.keyspace,
            server_list=self.server_list,
            credentials=self.credentials,
            recycle=self._recycle, 
            echo=self.echo, 
            logging_name=self._orig_logging_name,
            use_threadlocal=self._use_threadlocal, 
            listeners=self.listeners)

    def dispose(self):
        """Dispose of this pool."""

        for conn in self._all_conns:
            try:
                conn.close()
            except (SystemExit, KeyboardInterrupt):
                raise
        
        self._all_conns.clear()
        self._notify_on_pool_dispose()
            
    def dispose_local(self):
        if hasattr(self._conn, 'current'):
            conn = self._conn.current()
            self._all_conns.discard(conn)
            del self._conn.current

    def cleanup(self):
        while len(self._all_conns) > self.size:
            self._all_conns.pop()

    def status(self):
        return "SingletonThreadPool id:%d size: %d" % \
                            (id(self), len(self._all_conns))

    def _do_return_conn(self, conn):
        pass

    def _do_get(self):
        try:
            c = self._conn.current()
            if c:
                return c
        except AttributeError:
            pass
        c = self._create_connection()
        self._conn.current = weakref.ref(c)
        self._all_conns.add(c)
        if len(self._all_conns) > self.size:
            self._notify_on_pool_max()
            self.cleanup()
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

        :param echo: If True, connections being pulled and retrieved
          from the pool will be logged to the standard output, as well
          as pool sizing information.  Echoing can also be achieved by
          enabling logging for the "sqlalchemy.pool"
          namespace. Defaults to False.

        :param use_threadlocal: If set to True, repeated calls to
          :meth:`connect` within the same application thread will be
          guaranteed to return the same connection object, if one has
          already been retrieved from the pool and has not been
          returned yet.  Offers a slight performance advantage at the
          cost of individual transactions by default.  The
          :meth:`unique_connection` method is provided to bypass the
          threadlocal behavior installed into :meth:`connect`.

        :param reset_on_return: If true, reset the database state of
          connections returned to the pool.  This is typically a
          ROLLBACK to release locks and transaction resources.
          Disable at your own peril.  Defaults to True.

        :param listeners: A list of
          :class:`PoolListener`-like objects or
          dictionaries of callables that receive events when Cassandra
          connections are created, checked out and checked in to the
          pool.

        """
        Pool.__init__(self, **kw)
        self._pool = pool_queue.Queue(pool_size)
        self._overflow = 0 - pool_size
        self._max_overflow = max_overflow
        self._timeout = timeout
        self._overflow_lock = self._max_overflow > -1 and \
                                    threading.Lock() or None

    def recreate(self):
        self.logger.info("Pool recreating")
        self._notify_on_pool_recreate()
        return QueuePool(pool_size=self._pool.maxsize, 
                          max_overflow=self._max_overflow,
                          timeout=self._timeout, 
                          keyspace=self.keyspace,
                          server_list=self.server_list,
                          credentials=self.credentials,
                          recycle=self._recycle, echo=self.echo, 
                          logging_name=self._orig_logging_name,
                          use_threadlocal=self._use_threadlocal,
                          listeners=self.listeners)

    def _do_return_conn(self, conn):
        try:
            self._pool.put(conn, False)
        except pool_queue.Full:
            if self._overflow_lock is None:
                self._overflow -= 1
            else:
                self._overflow_lock.acquire()
                try:
                    self._overflow -= 1
                finally:
                    self._overflow_lock.release()

    def _do_get(self):
        try:
            wait = self._max_overflow > -1 and \
                        self._overflow >= self._max_overflow
            con = self._pool.get(wait, self._timeout)
            # If successful, Notify listeners
            if self._on_checkout:
                for l in self._on_checkout:
                    l.checkout(con)
            return con
        except pool_queue.Empty:
            if self._max_overflow > -1 and \
                        self._overflow >= self._max_overflow:
                if not wait:
                    return self._do_get()
                else:
                    self._notify_on_pool_max()
                    raise TimeoutError(
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
                con = self._create_connection()
                self._overflow += 1
            finally:
                if self._overflow_lock is not None:
                    self._overflow_lock.release()

            # Notify listeners
            if self._on_checkout:
                for l in self._on_checkout:
                    l.checkout(con)

            return con

    def dispose(self):
        while True:
            try:
                conn = self._pool.get(False)
                conn.close()
            except pool_queue.Empty:
                break

        self._overflow = 0 - self.size()
        self._notify_on_pool_dispose()
        self.logger.info("Pool disposed. %s", self.status())

    def status(self):
        return "Pool size: %d  Connections in pool: %d "\
                "Current Overflow: %d Current Checked out "\
                "connections: %d" % (self.size(), 
                                    self.checkedin(), 
                                    self.overflow(), 
                                    self.checkedout())

    def size(self):
        return self._pool.maxsize

    def checkedin(self):
        return self._pool.qsize()

    def overflow(self):
        return self._overflow

    def checkedout(self):
        return self._pool.maxsize - self._pool.qsize() + self._overflow

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
        conn.close()

    def _do_return_invalid(self, conn):
        pass

    def _do_get(self):
        return self._create_connection()

    def recreate(self):
        self.logger.info("Pool recreating")
        self._notify_on_pool_recreate()

        return NullPool(keyspace=self.keyspace,
            server_list=self.server_list,
            credentials=self.credentials, 
            recycle=self._recycle, 
            echo=self.echo, 
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
            self._conn.close()
            self._conn = None
        self._notify_on_pool_dispose()

    def recreate(self):
        self.logger.info("Pool recreating")
        self._notify_on_pool_recreate()
        return self.__class__(keyspace=self.keyspace,
                              server_list=self.server_list,
                              credentials=self.credentials,
                              recycle=self._recycle,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              echo=self.echo,
                              logging_name=self._orig_logging_name,
                              listeners=self.listeners)

    def _create_connection(self):
        return self._conn

    def _do_return_conn(self, conn):
        pass

    def _do_return_invalid(self, conn):
        pass

    def _do_get(self):
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
        assert conn is self._conn

    def _do_return_invalid(self, conn):
        self._conn = None
        self._checked_out = False
    
    def dispose(self):
        self._checked_out = False
        if self._conn:
            self._conn.close()
        self._notify_on_pool_dispose()

    def recreate(self):
        self.logger.info("Pool recreating")
        self._notify_on_pool_recreate()
        return AssertionPool(keyspace=self.keyspace,
                             server_list=self.server_list,
                             credentials=self.credentials,
                             echo=self.echo, 
                             logging_name=self._orig_logging_name,
                             listeners=self.listeners)
        
    def _do_get(self):
        if self._checked_out:
            self._notify_on_pool_max()
            raise AssertionError("connection is already checked out")
            
        if not self._conn:
            self._conn = self._create_connection()
        
        self._checked_out = True
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

    def first_connect(self, conn_record):
        """Called exactly once for the first Cassandra connection.

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

class TimeoutError(Exception):
    """Raised when a connection pool times out on getting a connection."""


class InvalidRequestError(Exception):
    """Pycassa was asked to do something it can't do.

    This error generally corresponds to runtime state errors.

    """
