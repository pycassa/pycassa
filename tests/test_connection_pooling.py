import threading
import unittest

from nose.tools import assert_raises
from pycassa import connect, connect_thread_local, NullPool, StaticPool,\
                    AssertionPool, SingletonThreadPool, QueuePool,\
                    ColumnFamily, PoolListener, TimeoutError

_credentials = {'username':'jsmith', 'password':'havebadpass'}
_pools = [NullPool, StaticPool, AssertionPool, SingletonThreadPool, QueuePool]

class PoolingCase(unittest.TestCase):

    def test_basic_pools(self):
        for pool_cls in _pools:
            print "Pool class: %s" % pool_cls.__name__
            pool = pool_cls(keyspace='Keyspace1', credentials=_credentials)
            pool = pool.recreate()
            conn_record = pool.get()
            cf = ColumnFamily(conn_record.connection, 'Standard1')
            cf.insert('key1', {'col':'val'})
            pool.status()
            pool.return_conn(conn_record)
            pool.dispose()

    def test_queue_pool(self):
        pool = QueuePool(keyspace='Keyspace1', credentials=_credentials,
                         pool_size=5, max_overflow=5, timeout=1)
        listener = TestListener()
        pool.add_listener(listener)

        conns = []
        for i in range(10):
            conns.append(pool.get())
            print pool.status()

        assert listener.connect_count == 10
        assert listener.checkout_count == 10
        assert listener.first_connect_count == 1

        assert_raises(TimeoutError, pool.get)
        assert listener.connect_count == 10

        for i in range(0, 5):
            pool.return_conn(conns[i])
            print pool.status()

        assert listener.checkin_count == 5

        for i in range(5, 10):
            pool.return_conn(conns[i])
            print pool.status()

        assert listener.checkin_count == 10

        # These connections should come from the pool
        for i in range(5):
            conns.append(pool.get())
            print pool.status()

        assert listener.connect_count == 10
        assert listener.checkout_count == 15

        # But these will need to be made
        for i in range(5):
            conns.append(pool.get())
            print pool.status()

        assert listener.connect_count == 15
        assert listener.checkout_count == 20


class TestListener(PoolListener):

    def __init__(self):
        self.connect_count = 0
        self.first_connect_count = 0
        self.checkout_count = 0
        self.checkin_count = 0

    def connect(self, conn_record):
        """Called once for each new Cassandra connection or Pool's ``creator()``.

        conn_record
          The ``_ConnectionRecord`` that persistently manages the connection

        """
        self.connect_count += 1

    def first_connect(self, conn_record):
        """Called exactly once for the first Cassandra connection.

        conn_record
          The ``_ConnectionRecord`` that persistently manages the connection

        """
        self.first_connect_count += 1

    def checkout(self, conn_record):
        """Called when a connection is retrieved from the Pool.

        conn_record
          The ``_ConnectionRecord`` that persistently manages the connection

        """
        self.checkout_count += 1

    def checkin(self, conn_record):
        """Called when a connection returns to the pool.

        Note that the connection may be closed, and may be None if the
        connection has been invalidated.  ``checkin`` will not be called
        for detached connections.  (They do not return to the pool.)

        conn_record
          The ``_ConnectionRecord`` that persistently manages the connection

        """
        self.checkin_count += 1
