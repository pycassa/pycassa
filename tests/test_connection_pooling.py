import threading
import unittest
import time

from nose.tools import assert_raises, assert_equal
from pycassa import connect, connect_thread_local, NullPool, StaticPool,\
                    AssertionPool, SingletonThreadPool, QueuePool,\
                    ColumnFamily, PoolListener, TimeoutError,\
                    NoConnectionAvailable

_credentials = {'username':'jsmith', 'password':'havebadpass'}
_pools = [NullPool, StaticPool, AssertionPool, SingletonThreadPool, QueuePool]

def _get_list():
    return ['foo:bar']

class PoolingCase(unittest.TestCase):

    def test_basic_pools(self):
        for pool_cls in _pools:
            print "Pool class: %s" % pool_cls.__name__
            pool = pool_cls(keyspace='Keyspace1', credentials=_credentials)
            pool.dispose()
            pool = pool.recreate()
            conn_record = pool.get()
            cf = ColumnFamily(conn_record.get_connection(), 'Standard1')
            cf.insert('key1', {'col':'val'})
            pool.status()
            pool.return_conn(conn_record)

    def test_server_list_func(self):
        listener = _TestListener()
        pool = NullPool(keyspace='Keyspace1', server_list=_get_list,
                        listeners=[listener])
        assert_equal(listener.serv_list, ['foo:bar'])
        assert_equal(listener.list_count, 1)

    def test_queue_pool(self):
        listener = _TestListener()
        pool = QueuePool(keyspace='Keyspace1', credentials=_credentials,
                         pool_size=5, max_overflow=5, timeout=1,
                         listeners=[listener])
        conns = []
        for i in range(10):
            conns.append(pool.get())

        assert_equal(listener.connect_count, 10)
        assert_equal(listener.checkout_count, 10)

        # Pool is maxed out now
        assert_raises(TimeoutError, pool.get)
        assert_equal(listener.connect_count, 10)
        assert_equal(listener.max_count, 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])

        assert_equal(listener.checkin_count, 5)

        for i in range(5, 10):
            pool.return_conn(conns[i])

        assert_equal(listener.checkin_count, 10)

        conns = []

        # These connections should come from the pool
        for i in range(5):
            conns.append(pool.get())

        assert_equal(listener.connect_count, 10)
        assert_equal(listener.checkout_count, 15)

        # But these will need to be made
        for i in range(5):
            conns.append(pool.get())

        assert_equal(listener.connect_count, 15)
        assert_equal(listener.checkout_count, 20)

        assert_equal(listener.close_count, 0)
        for i in range(10):
            conns[i].get_connection()
            conns[i].close()
        assert_equal(listener.close_count, 10)

        # Checkin closed connections
        assert_equal(listener.checkin_count, 10)
        for i in range(10):
            pool.return_conn(conns[i])
        assert_equal(listener.checkin_count, 20)

        pool.dispose()
        assert_equal(listener.dispose_count, 1)
        pool.recreate()
        assert_equal(listener.recreate_count, 1)

    def test_singleton_thread_pool(self):
        listener = _TestListener()
        pool = SingletonThreadPool(keyspace='Keyspace1',
                         credentials=_credentials, pool_size=5,
                         listeners=[listener])

        # Make sure we get the same connection every time
        conn = pool.get()
        assert_equal(pool.get(), conn)
        for i in range(10):
            conn = pool.get()
        assert_equal(listener.connect_count, 1)

        pool.return_conn(conn)

        conn = pool.get()
        assert_equal(pool.get(), conn)
        assert_equal(listener.connect_count, 2)
        pool.return_conn(conn)

        def get_return():
            conn = pool.get()
            print pool.status()

        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=get_return))
            threads[-1].start()
        for thread in threads:
            thread.join()
        assert_equal(listener.connect_count, 7)

        assert_raises(NoConnectionAvailable, pool.get)

class _TestListener(PoolListener):

    def __init__(self):
        self.connect_count = 0
        self.checkout_count = 0
        self.checkin_count = 0
        self.close_count = 0
        self.list_count = 0
        self.recreate_count = 0
        self.dispose_count = 0
        self.max_count = 0
        self.serv_list = []

    def connect(self, dic):
        self.connect_count += 1

    def checkout(self, dic):
        self.checkout_count += 1

    def checkin(self, dic):
        self.checkin_count += 1

    def close(self, dic):
        self.close_count += 1

    def obtained_server_list(self, dic):
        self.list_count += 1
        self.serv_list = dic.get('server_list')

    def pool_recreated(self, dic):
        self.recreate_count += 1

    def pool_disposed(self, dic):
        self.dispose_count += 1

    def pool_max(self, dic):
        self.max_count += 1
