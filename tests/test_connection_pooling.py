import threading
import unittest
import time

from nose.tools import assert_raises, assert_equal, assert_not_equal
from pycassa import connect, connect_thread_local, NullPool, StaticPool,\
                    AssertionPool, SingletonThreadPool, QueuePool,\
                    ColumnFamily, PoolListener, InvalidRequestError,\
                    NoConnectionAvailable, MaximumRetryException
from cassandra.ttypes import TimedOutException

_credentials = {'username':'jsmith', 'password':'havebadpass'}
_pools = [NullPool, StaticPool, AssertionPool, SingletonThreadPool, QueuePool]

def _get_list():
    return ['foo:bar']

def _timeout(*args, **kwargs):
    raise TimedOutException()

def _five_tlocal_fails(pool, key, column):
    conn = pool.get()
    cf = ColumnFamily(conn, 'Standard1')
    for i in range(0,5):
        setattr(cf.client._local.conn.client, 'batch_mutate', _timeout)

        # The first insert attempt should fail, but failover should occur
        # and the insert should succeed
        cf.insert(key, column)
        cf.get(key)

def _five_fails(pool, key, column):
    conn = pool.get()
    cf = ColumnFamily(conn, 'Standard1')
    for i in range(0,5):
        setattr(cf.client._connection.client, 'batch_mutate', _timeout)

        # The first insert attempt should fail, but failover should occur
        # and the insert should succeed
        cf.insert(key, column)
        cf.get(key)

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
        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, timeout=1,
                         keyspace='Keyspace1', credentials=_credentials,
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
        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, timeout=1,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True)
        conns = []

        assert_equal(listener.connect_count, 5)
        # These connections should all be the same
        for i in range(10):
            conns.append(pool.get())
        assert_equal(listener.connect_count, 5)
        assert_equal(listener.checkout_count, 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])
        assert_equal(listener.checkin_count, 1)
        for i in range(5, 10):
            pool.return_conn(conns[i])
        assert_equal(listener.checkin_count, 1)

        conns = []

        assert_equal(listener.connect_count, 5)
        # A single connection should come from the pool
        for i in range(5):
            conns.append(pool.get())
        assert_equal(listener.connect_count, 5)
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

        assert_equal(listener.connect_count, 0) # Still 5 connections in pool
        assert_equal(listener.checkout_count, 5)
        assert_equal(listener.checkin_count, 5)

        # These should come from the pool
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=checkout_return))
            threads[-1].start()
        for thread in threads:
            thread.join()
        assert_equal(listener.connect_count, 0)
        assert_equal(listener.checkout_count, 10)
        assert_equal(listener.checkin_count, 10)

        pool.dispose()

    def test_queue_pool_recycle(self):
        listener = _TestListener()
        pool = QueuePool(pool_size=5, max_overflow=5, recycle=1,
                         prefill=True, timeout=1,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False)

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')
        for i in range(10):
            cf.insert('key', {'col': 'val'})

        conn.return_to_pool()
        assert_equal(listener.recycle_count, 1)

        pool.dispose()
        listener.reset()

        # Try with threadlocal=True
        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10,
                         prefill=True, timeout=1,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True)

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')
        for i in range(10):
            cf.insert('key', {'col': 'val'})

        conn.return_to_pool()
        assert_equal(listener.recycle_count, 1)


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

    def test_pool_connection_failure(self):
        listener = _TestListener()
 
        def get_extra():
            """Make failure count adjustments based on whether or not
            the permuted list starts with a good host:port"""
            if listener.serv_list[0] == 'localhost:9160':
                return 0
            else:
                return 1

        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'foobar:1'])

        assert_equal(listener.failure_count, 4 + get_extra()) 

        for i in range(0,7):
            pool.get()

        assert_equal(listener.failure_count, 6 + get_extra())

        pool.dispose()
        listener.reset()

        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True,
                         server_list=['localhost:9160', 'foobar:1'])

        assert_equal(listener.failure_count, 4 + get_extra())

        threads = []
        for i in range(0, 7):
            threads.append(threading.Thread(target=pool.get))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.failure_count, 6 + get_extra())

        pool.dispose()
        listener.reset()

        pool = SingletonThreadPool(pool_size=5, keyspace='Keyspace1',
                         credentials=_credentials, listeners=[listener],
                         server_list=['localhost:9160', 'foobar:1'])

        threads = []
        for i in range(0, 5):
            threads.append(threading.Thread(target=pool.get))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.failure_count, 4 + get_extra())

        pool.dispose()
        listener.reset()

        pool = StaticPool(keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener],
                         server_list=['localhost:9160', 'foobar:1'])

        pool.get()
        assert_equal(listener.failure_count, 0 + get_extra())
        
        pool.dispose()
        listener.reset()

        pool = NullPool(keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'foobar:1'])
 
        for i in range(0, 5):
            pool.get()

        assert_equal(listener.failure_count, 4 + get_extra()) 

        pool.dispose()
        listener.reset()
       
        pool = AssertionPool(keyspace='Keyspace1', credentials=_credentials,
                             listeners=[listener], use_threadlocal=False,
                             server_list=['localhost:9160', 'foobar:1'])
        while True:
            if listener.serv_list[0] == 'foobar:1':
                break
            pool.set_server_list(['localhost:9160', 'foobar:1'])

        conn = pool.get()
        assert_equal(listener.failure_count, 1) 
        
        pool.dispose()
        listener.reset()
 
        pool = AssertionPool(keyspace='Keyspace1', credentials=_credentials,
                             listeners=[listener], use_threadlocal=True,
                             server_list=['localhost:9160', 'foobar:1'])
        while True:
            if listener.serv_list[0] == 'foobar:1':
                break
            pool.set_server_list(['localhost:9160', 'foobar:1'])

        conn = pool.get()
        assert_equal(listener.failure_count, 1) 

        pool.dispose()

    def test_queue_failover(self):
        listener = _TestListener()
        pool = QueuePool(pool_size=2, max_overflow=0, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'localhost:9160'])

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')

        for i in range(1,5):
            setattr(cf.client._connection.client, 'batch_mutate', _timeout)

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val'})
            assert_equal(listener.failure_count, i)
            cf.get('key')

        pool.dispose()
        listener.reset()

        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'localhost:9160'])
        threads = []
        args = (pool, 'key', {'col':'val'})
        for i in range(5):
            threads.append(threading.Thread(target=_five_fails, args=args))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.failure_count, 25)

        pool.dispose()

    def test_queue_threadlocal_failover(self):
        listener = _TestListener()
        pool = QueuePool(pool_size=2, max_overflow=0, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')

        for i in range(1,5):
            setattr(cf.client._local.conn.client, 'batch_mutate', _timeout)

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val'})
            assert_equal(listener.failure_count, i)
            cf.get('key')

        pool.dispose()
        listener.reset()

        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])
        threads = []
        args = (pool, 'key', {'col':'val'})
        for i in range(0, 5):
            threads.append(threading.Thread(target=_five_tlocal_fails, args=args))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.failure_count, 25)
        pool.dispose()

    def test_queue_retry_limit(self):
        listener = _TestListener()
        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, max_retries=3, # allow 3 retries
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'localhost:9160'])

        # Corrupt all of the connections
        for i in range(5):
            conn = pool.get()
            cf = ColumnFamily(conn, 'Standard1')
            setattr(cf.client._connection.client, 'batch_mutate', _timeout)
            conn.return_to_pool()

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')
        assert_raises(MaximumRetryException, cf.insert, 'key', {'col':'val'})
        assert_equal(listener.failure_count, 4) # On the 4th failure, didn't retry

        pool.dispose()

    def test_queue_threadlocal_retry_limit(self):
        listener = _TestListener()
        pool = QueuePool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, max_retries=3, # allow 3 retries
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        # Corrupt all of the connections
        for i in range(5):
            conn = pool.get()
            cf = ColumnFamily(conn, 'Standard1')
            setattr(cf.client._local.conn.client, 'batch_mutate', _timeout)
            conn.return_to_pool()

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')
        assert_raises(MaximumRetryException, cf.insert, 'key', {'col':'val'})
        assert_equal(listener.failure_count, 4) # On the 4th failure, didn't retry

        pool.dispose()

    def test_null_pool_failover(self):
        listener = _TestListener()
        pool = NullPool(keyspace='Keyspace1', credentials=_credentials,
                        listeners=[listener], use_threadlocal=False,
                        server_list=['localhost:9160', 'localhost:9160'])

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')

        for i in range(1,5):
            setattr(cf.client._connection.client, 'batch_mutate', _timeout)

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val'})
            assert_equal(listener.failure_count, i)
            cf.get('key')

        pool.dispose()
        listener.reset()

        pool = NullPool(keyspace='Keyspace1', credentials=_credentials,
                        listeners=[listener], use_threadlocal=False,
                        server_list=['localhost:9160', 'localhost:9160'])

        threads = []
        args = (pool, 'key', {'col':'val'})
        for i in range(0, 5):
            threads.append(threading.Thread(target=_five_fails, args=args))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.failure_count, 25)
        pool.dispose()

    def test_singleton_thread_failover(self):
        listener = _TestListener()
        pool = SingletonThreadPool(pool_size=5,
                                   keyspace='Keyspace1', credentials=_credentials,
                                   listeners=[listener],
                                   server_list=['localhost:9160', 'localhost:9160'])

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')

        for i in range(1,5):
            setattr(cf.client._local.conn.client, 'batch_mutate', _timeout)

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val'})
            assert_equal(listener.failure_count, i)
            cf.get('key')

        pool.dispose()
        listener.reset()

        pool = SingletonThreadPool(pool_size=5,
                                   keyspace='Keyspace1', credentials=_credentials,
                                   listeners=[listener],
                                   server_list=['localhost:9160', 'localhost:9160'])

        threads = []
        args = (pool, 'key', {'col':'val'})
        for j in range(0,5):
            threads.append(threading.Thread(target=_five_tlocal_fails, args=args))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.failure_count, 25)

    def test_assertion_failover(self):
        listener = _TestListener()
        pool = AssertionPool(keyspace='Keyspace1', credentials=_credentials,
                             listeners=[listener], use_threadlocal=True,
                             server_list=['localhost:9160', 'localhost:9160'])

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')

        for i in range(1,5):
            setattr(cf.client._local.conn.client, 'batch_mutate', _timeout)

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val'})
            assert_equal(listener.failure_count, i)
            cf.get('key')

        pool.dispose()
        listener.reset()

    def test_assertion_threadlocal_failover(self):
        listener = _TestListener()
        pool = AssertionPool(keyspace='Keyspace1', credentials=_credentials,
                             listeners=[listener], use_threadlocal=False,
                             server_list=['localhost:9160', 'localhost:9160'])

        conn = pool.get()
        cf = ColumnFamily(conn, 'Standard1')

        for i in range(1,5):
            setattr(cf.client._connection.client, 'batch_mutate', _timeout)

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val'})
            assert_equal(listener.failure_count, i)
            cf.get('key')

        pool.dispose()

class _TestListener(PoolListener):

    def __init__(self):
        self.serv_list = []
        self.reset()

    def reset(self):
        self.connect_count = 0
        self.checkout_count = 0
        self.checkin_count = 0
        self.close_count = 0
        self.recycle_count = 0
        self.failure_count = 0
        self.list_count = 0
        self.recreate_count = 0
        self.dispose_count = 0
        self.max_count = 0

    def connection_created(self, dic):
        self.connect_count += 1

    def connection_checked_out(self, dic):
        self.checkout_count += 1

    def connection_checked_in(self, dic):
        self.checkin_count += 1

    def connection_disposed(self, dic):
        self.close_count += 1

    def connection_recycled(self, dic):
        self.recycle_count += 1

    def connection_failed(self, dic):
        self.failure_count += 1

    def obtained_server_list(self, dic):
        self.list_count += 1
        self.serv_list = dic.get('server_list')

    def pool_recreated(self, dic):
        self.recreate_count += 1

    def pool_disposed(self, dic):
        self.dispose_count += 1

    def pool_at_max(self, dic):
        self.max_count += 1
