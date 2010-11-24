import threading
import unittest
import time

from nose.tools import assert_raises, assert_equal, assert_not_equal
from pycassa import ColumnFamily, ConnectionPool,\
                    ColumnFamily, PoolListener, InvalidRequestError,\
                    NoConnectionAvailable, MaximumRetryException,\
                    AllServersUnavailable, PycassaLogger

import pycassa.pool

from pycassa.cassandra.ttypes import TimedOutException
from pycassa.cassandra.Cassandra import Client

_credentials = {'username':'jsmith', 'password':'havebadpass'}
_pools = [ConnectionPool]
_should_tlocal_fail = True

logger = PycassaLogger()
logger.set_logger_level('info')

def _get_list():
    return ['foo:bar']

class PoolingCase(unittest.TestCase):

    def tearDown(self):
        pool = ConnectionPool(keyspace='Keyspace1')
        cf = ColumnFamily(pool, 'Standard1')
        for key, cols in cf.get_range():
            cf.remove(key)

    def test_basic_pools(self):
        for pool_cls in _pools:
            pool = pool_cls(keyspace='Keyspace1', credentials=_credentials)
            pool.dispose()
            pool = pool.recreate()
            cf = ColumnFamily(pool, 'Standard1')
            cf.insert('key1', {'col':'val'})
            pool.status()
            pool.dispose()

    def test_server_list_func(self):
        listener = _TestListener()
        pool = ConnectionPool(keyspace='Keyspace1', server_list=_get_list,
                         listeners=[listener], prefill=False)
        assert_equal(listener.serv_list, ['foo:bar'])
        assert_equal(listener.list_count, 1)

    def test_queue_pool(self):
        listener = _TestListener()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, pool_timeout=0.5, timeout=1,
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
        pool.dispose()

    def test_queue_pool_threadlocal(self):
        listener = _TestListener()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, pool_timeout=0.5, timeout=1,
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
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=1,
                         prefill=True, pool_timeout=0.5, timeout=1,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False)

        cf = ColumnFamily(pool, 'Standard1')
        columns = {'col1': 'val', 'col2': 'val'}
        for i in range(10):
            cf.insert('key', columns)

        assert_equal(listener.recycle_count, 5)

        pool.dispose()
        listener.reset()

        # Try with threadlocal=True
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=1,
                         prefill=False, pool_timeout=0.5, timeout=1,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True)

        cf = ColumnFamily(pool, 'Standard1')
        for i in range(10):
            cf.insert('key', columns)

        pool.dispose()
        assert_equal(listener.recycle_count, 5)

    def test_pool_connection_failure(self):
        listener = _TestListener()
 
        def get_extra():
            """Make failure count adjustments based on whether or not
            the permuted list starts with a good host:port"""
            if listener.serv_list[0] == 'localhost:9160':
                return 0
            else:
                return 1

        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         timeout=0.05,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'foobar:1'])

        assert_equal(listener.failure_count, 4 + get_extra()) 

        for i in range(0,7):
            pool.get()

        assert_equal(listener.failure_count, 6 + get_extra())

        pool.dispose()
        listener.reset()

        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True,
                         keyspace='Keyspace1', credentials=_credentials,
                         timeout=0.05,
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

    def test_queue_failover(self):
        listener = _TestListener()
        pool = ConnectionPool(pool_size=1, max_overflow=0, recycle=10000,
                         prefill=True, timeout=0.05,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'localhost:9160'])

        cf = ColumnFamily(pool, 'Standard1')

        for i in range(1,5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val%d' % i, 'col2': 'val'})
            assert_equal(listener.failure_count, i)
            assert_equal(cf.get('key'), {'col': 'val%d' % i, 'col2': 'val'})

        pool.dispose()

    def test_queue_threadlocal_failover(self):
        listener = _TestListener()
        pool = ConnectionPool(pool_size=1, max_overflow=0, recycle=10000,
                         prefill=True, timeout=0.05,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        cf = ColumnFamily(pool, 'Standard1')

        for i in range(1,5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val%d' % i, 'col2': 'val'})
            assert_equal(listener.failure_count, i)
            assert_equal(cf.get('key'), {'col': 'val%d' % i, 'col2': 'val'})

        pool.dispose()
        listener.reset()

        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, timeout=0.05,
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        cf = ColumnFamily(pool, 'Standard1')

        for i in range(5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

        threads = []
        args=('key', {'col': 'val', 'col2': 'val'})
        for i in range(5):
            threads.append(threading.Thread(target=cf.insert, args=args))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(listener.failure_count, 5)

        pool.dispose()

    def test_queue_retry_limit(self):
        listener = _TestListener()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, max_retries=3, # allow 3 retries
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=False,
                         server_list=['localhost:9160', 'localhost:9160'])

        # Corrupt all of the connections
        for i in range(5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

        cf = ColumnFamily(pool, 'Standard1')
        assert_raises(MaximumRetryException, cf.insert, 'key', {'col':'val', 'col2': 'val'})
        assert_equal(listener.failure_count, 4) # On the 4th failure, didn't retry

        pool.dispose()

    def test_queue_threadlocal_retry_limit(self):
        listener = _TestListener()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, max_retries=3, # allow 3 retries
                         keyspace='Keyspace1', credentials=_credentials,
                         listeners=[listener], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        # Corrupt all of the connections
        for i in range(5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

        cf = ColumnFamily(pool, 'Standard1')
        assert_raises(MaximumRetryException, cf.insert, 'key', {'col':'val', 'col2': 'val'})
        assert_equal(listener.failure_count, 4) # On the 4th failure, didn't retry

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
