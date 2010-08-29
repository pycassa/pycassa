import threading
import unittest
import time

from nose.tools import assert_raises, assert_equal, assert_not_equal
from pycassa import connect, connect_thread_local, NullPool, StaticPool,\
                    AssertionPool, SingletonThreadPool, QueuePool,\
                    ColumnFamily, PoolListener, InvalidRequestError,\
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
            conn = pool.get()
            cf = ColumnFamily(conn, 'Standard1')
            cf.insert('key1', {'col':'val'})
            pool.status()
            pool.return_conn(conn)

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
                         listeners=[listener], use_threadlocal=False)
        conns = []
        for i in range(10):
            conns.append(pool.get())

        assert_equal(listener.connect_count, 10)
        assert_equal(listener.checkout_count, 10)

        # Pool is maxed out now
        assert_raises(NoConnectionAvailable, pool.get)
        assert_equal(listener.connect_count, 10)
        assert_equal(listener.max_count, 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])
        assert_equal(listener.close_count, 0)
        assert_equal(listener.checkin_count, 5)

        for i in range(5, 10):
            pool.return_conn(conns[i])
        assert_equal(listener.close_count, 5)
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

        assert_equal(listener.close_count, 5)
        for i in range(10):
            conns[i].return_to_pool()
        assert_equal(listener.checkin_count, 20)
        assert_equal(listener.close_count, 10)

        assert_raises(InvalidRequestError, conns[0].return_to_pool)
        assert_raises(InvalidRequestError, conns[-1].return_to_pool)

        assert_equal(listener.checkin_count, 20)
        assert_equal(listener.close_count, 10)

        pool.dispose()
        assert_equal(listener.dispose_count, 1)
        pool.recreate()
        assert_equal(listener.recreate_count, 1)

    def test_queue_pool_threadlocal(self):
        listener = _TestListener()
        pool = QueuePool(keyspace='Keyspace1', credentials=_credentials,
                         pool_size=5, max_overflow=5, timeout=1,
                         listeners=[listener], use_threadlocal=True)
        conns = []

        # These connections should all be the same
        for i in range(10):
            conns.append(pool.get())
        assert_equal(listener.connect_count, 1)
        assert_equal(listener.checkout_count, 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])
        assert_equal(listener.checkin_count, 1)
        for i in range(5, 10):
            pool.return_conn(conns[i])
        assert_equal(listener.checkin_count, 1)

        conns = []

        # A single connection should come from the pool
        for i in range(5):
            conns.append(pool.get())

        assert_equal(listener.connect_count, 1)
        assert_equal(listener.checkout_count, 2)

        for conn in conns:
            pool.return_conn(conn)

        conns = []
        threads = []
        listener.reset()
        def checkout_return():
            conn = pool.get()
            time.sleep(1)
            pool.return_conn(conn)

        for i in range(5):
            threads.append(threading.Thread(target=checkout_return))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.connect_count, 4) # Still was one conn in pool
        assert_equal(listener.checkout_count, 5)
        assert_equal(listener.checkin_count, 5)

        # These should come from the pool
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=checkout_return))
            threads[-1].start()
        for thread in threads:
            thread.join()
        assert_equal(listener.connect_count, 4)
        assert_equal(listener.checkout_count, 10)
        assert_equal(listener.checkin_count, 10)

        pool.dispose()

    def test_singleton_thread_pool(self):
        listener = _TestListener()
        pool = SingletonThreadPool(keyspace='Keyspace1',
                         credentials=_credentials, pool_size=5,
                         listeners=[listener], use_threadlocal=False)

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
        assert_equal(listener.max_count, 1)

    def test_singleton_pool_threadlocal(self):
        """Should be the same as non-threadlocal."""
        listener = _TestListener()
        pool = SingletonThreadPool(keyspace='Keyspace1',
                         credentials=_credentials, pool_size=5,
                         listeners=[listener], use_threadlocal=True)

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
        assert_equal(listener.max_count, 1)

    def test_static_pool(self):
        def static_pool_tester(pool, listener):
            orig_conn = pool.get()
            def compare():
                conn = pool.get()
                assert_equal(orig_conn, conn)

            threads = []
            for i in range(5):
                threads.append(threading.Thread(target=compare))
                threads[-1].start()
            for thread in threads:
                thread.join()
            assert_equal(listener.connect_count, 1)
            assert_equal(listener.checkout_count, 6)

        listener = _TestListener()
        pool = StaticPool(keyspace='Keyspace1', credentials=_credentials,
                          listeners=[listener], use_threadlocal=False)
        static_pool_tester(pool, listener)

        listener = _TestListener()
        pool = StaticPool(keyspace='Keyspace1', credentials=_credentials,
                          listeners=[listener], use_threadlocal=True)
        static_pool_tester(pool, listener)

    def test_assertion_pool(self):
        def assertion_pool_tester(pool, listener):
            conn = pool.get()
            assert_raises(AssertionError, pool.get)

            def return_c():
                pool.return_conn(conn)

            pool.return_conn(conn)
            assert_raises(AssertionError, return_c)

            conn = pool.get()

            def thread_get():
                assert_raises(AssertionError, pool.get)

            t = threading.Thread(target=thread_get)
            t.start()
            t.join()
            pool.return_conn(conn)

            assert_equal(listener.checkout_count, 2)
            assert_equal(listener.max_count, 2)
            assert_equal(listener.checkin_count, 2)

        listener = _TestListener()
        pool = AssertionPool(keyspace='Keyspace1', credentials=_credentials,
                          listeners=[listener], use_threadlocal=False)
        assertion_pool_tester(pool, listener)
        
        # This should perform the same
        listener = _TestListener()
        pool = AssertionPool(keyspace='Keyspace1', credentials=_credentials,
                          listeners=[listener], use_threadlocal=True)
        assertion_pool_tester(pool, listener)

    def test_null_pool(self):
        listener = _TestListener()
        pool = NullPool(keyspace='Keyspace1', credentials=_credentials,
                          listeners=[listener])

        conn = pool.get()
        pool.return_conn(conn)
        assert_equal(listener.checkout_count, 1)
        assert_equal(listener.checkin_count, 1)

class _TestListener(PoolListener):

    def __init__(self):
        self.serv_list = []
        self.reset()

    def reset(self):
        self.connect_count = 0
        self.checkout_count = 0
        self.checkin_count = 0
        self.close_count = 0
        self.list_count = 0
        self.recreate_count = 0
        self.dispose_count = 0
        self.max_count = 0

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
