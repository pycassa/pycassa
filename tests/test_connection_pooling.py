import threading
import unittest
import time

from nose.tools import assert_raises, assert_equal, assert_true
from pycassa import ColumnFamily, ConnectionPool, InvalidRequestError,\
                    NoConnectionAvailable, MaximumRetryException, AllServersUnavailable
from pycassa.logging.pool_stats_logger import StatsLogger
from pycassa.cassandra.ttypes import ColumnPath
from pycassa.cassandra.ttypes import InvalidRequestException
from pycassa.cassandra.ttypes import NotFoundException


_credentials = {'username': 'jsmith', 'password': 'havebadpass'}

def _get_list():
    return ['foo:bar']

class PoolingCase(unittest.TestCase):

    def tearDown(self):
        pool = ConnectionPool('PycassaTestKeyspace')
        cf = ColumnFamily(pool, 'Standard1')
        for key, cols in cf.get_range():
            cf.remove(key)

    def test_basic_pools(self):
        pool = ConnectionPool('PycassaTestKeyspace', credentials=_credentials)
        cf = ColumnFamily(pool, 'Standard1')
        cf.insert('key1', {'col': 'val'})
        pool.dispose()

    def test_empty_list(self):
        assert_raises(AllServersUnavailable, ConnectionPool, 'PycassaTestKeyspace', server_list=[])

    def test_server_list_func(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool('PycassaTestKeyspace', server_list=_get_list,
                         listeners=[stats_logger], prefill=False)
        assert_equal(stats_logger.serv_list, ['foo:bar'])
        assert_equal(stats_logger.stats['list'], 1)
        pool.dispose()

    def test_queue_pool(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, pool_timeout=0.1, timeout=1,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=False)
        conns = []
        for i in range(10):
            conns.append(pool.get())

        assert_equal(stats_logger.stats['created']['success'], 10)
        assert_equal(stats_logger.stats['checked_out'], 10)

        # Pool is maxed out now
        assert_raises(NoConnectionAvailable, pool.get)
        assert_equal(stats_logger.stats['created']['success'], 10)
        assert_equal(stats_logger.stats['at_max'], 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])
        assert_equal(stats_logger.stats['disposed']['success'], 0)
        assert_equal(stats_logger.stats['checked_in'], 5)

        for i in range(5, 10):
            pool.return_conn(conns[i])
        assert_equal(stats_logger.stats['disposed']['success'], 5)
        assert_equal(stats_logger.stats['checked_in'], 10)

        conns = []

        # These connections should come from the pool
        for i in range(5):
            conns.append(pool.get())
        assert_equal(stats_logger.stats['created']['success'], 10)
        assert_equal(stats_logger.stats['checked_out'], 15)

        # But these will need to be made
        for i in range(5):
            conns.append(pool.get())
        assert_equal(stats_logger.stats['created']['success'], 15)
        assert_equal(stats_logger.stats['checked_out'], 20)

        assert_equal(stats_logger.stats['disposed']['success'], 5)
        for i in range(10):
            conns[i].return_to_pool()
        assert_equal(stats_logger.stats['checked_in'], 20)
        assert_equal(stats_logger.stats['disposed']['success'], 10)

        assert_raises(InvalidRequestError, conns[0].return_to_pool)
        assert_equal(stats_logger.stats['checked_in'], 20)
        assert_equal(stats_logger.stats['disposed']['success'], 10)

        print "in test:", id(conns[-1])
        conns[-1].return_to_pool()
        assert_equal(stats_logger.stats['checked_in'], 20)
        assert_equal(stats_logger.stats['disposed']['success'], 10)

        pool.dispose()

    def test_queue_pool_threadlocal(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, pool_timeout=0.01, timeout=1,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=True)
        conns = []

        assert_equal(stats_logger.stats['created']['success'], 5)
        # These connections should all be the same
        for i in range(10):
            conns.append(pool.get())
        assert_equal(stats_logger.stats['created']['success'], 5)
        assert_equal(stats_logger.stats['checked_out'], 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])
        assert_equal(stats_logger.stats['checked_in'], 1)
        for i in range(5, 10):
            pool.return_conn(conns[i])
        assert_equal(stats_logger.stats['checked_in'], 1)

        conns = []

        assert_equal(stats_logger.stats['created']['success'], 5)
        # A single connection should come from the pool
        for i in range(5):
            conns.append(pool.get())
        assert_equal(stats_logger.stats['created']['success'], 5)
        assert_equal(stats_logger.stats['checked_out'], 2)

        for conn in conns:
            pool.return_conn(conn)

        conns = []
        threads = []
        stats_logger.reset()

        def checkout_return():
            conn = pool.get()
            time.sleep(1)
            pool.return_conn(conn)

        for i in range(5):
            threads.append(threading.Thread(target=checkout_return))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(stats_logger.stats['created']['success'], 0) # Still 5 connections in pool
        assert_equal(stats_logger.stats['checked_out'], 5)
        assert_equal(stats_logger.stats['checked_in'], 5)

        # These should come from the pool
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=checkout_return))
            threads[-1].start()
        for thread in threads:
            thread.join()
        assert_equal(stats_logger.stats['created']['success'], 0)
        assert_equal(stats_logger.stats['checked_out'], 10)
        assert_equal(stats_logger.stats['checked_in'], 10)

        pool.dispose()

    def test_queue_pool_no_prefill(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=False, pool_timeout=0.1, timeout=1,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=False)
        conns = []
        for i in range(10):
            conns.append(pool.get())
            assert_equal(stats_logger.stats['created']['success'], i + 1)
            assert_equal(stats_logger.stats['checked_out'], i + 1)

        # Pool is maxed out now
        assert_raises(NoConnectionAvailable, pool.get)
        assert_equal(stats_logger.stats['created']['success'], 10)
        assert_equal(stats_logger.stats['at_max'], 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])
            assert_equal(stats_logger.stats['checked_in'], i + 1)
            assert_equal(stats_logger.stats['disposed']['success'], 0)

        for i in range(5, 10):
            pool.return_conn(conns[i])
            assert_equal(stats_logger.stats['checked_in'], i + 1)
            assert_equal(stats_logger.stats['disposed']['success'], (i - 5) + 1)

        conns = []

        # These connections should come from the pool
        for i in range(5):
            conns.append(pool.get())
            assert_equal(stats_logger.stats['created']['success'], 10)
            assert_equal(stats_logger.stats['checked_out'], (i + 10) + 1)

        # But these will need to be made
        for i in range(5):
            conns.append(pool.get())
            assert_equal(stats_logger.stats['created']['success'], (i + 10) + 1)
            assert_equal(stats_logger.stats['checked_out'], (i + 15) + 1)

        assert_equal(stats_logger.stats['disposed']['success'], 5)
        for i in range(10):
            conns[i].return_to_pool()
            assert_equal(stats_logger.stats['checked_in'], (i + 10) + 1)
        assert_equal(stats_logger.stats['disposed']['success'], 10)

        # Make sure a double return doesn't change our counts
        assert_raises(InvalidRequestError, conns[0].return_to_pool)
        assert_equal(stats_logger.stats['checked_in'], 20)
        assert_equal(stats_logger.stats['disposed']['success'], 10)

        conns[-1].return_to_pool()
        assert_equal(stats_logger.stats['checked_in'], 20)
        assert_equal(stats_logger.stats['disposed']['success'], 10)

        pool.dispose()

    def test_queue_pool_recycle(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=1,
                         prefill=True, pool_timeout=0.5, timeout=1,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=False)

        cf = ColumnFamily(pool, 'Standard1')
        columns = {'col1': 'val', 'col2': 'val'}
        for i in range(10):
            cf.insert('key', columns)

        assert_equal(stats_logger.stats['recycled'], 5)

        pool.dispose()
        stats_logger.reset()

        # Try with threadlocal=True
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=1,
                         prefill=False, pool_timeout=0.5, timeout=1,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=True)

        cf = ColumnFamily(pool, 'Standard1')
        for i in range(10):
            cf.insert('key', columns)

        pool.dispose()
        assert_equal(stats_logger.stats['recycled'], 5)

    def test_pool_connection_failure(self):
        stats_logger = StatsLoggerWithListStorage()

        def get_extra():
            """Make failure count adjustments based on whether or not
            the permuted list starts with a good host:port"""
            if stats_logger.serv_list[0] == 'localhost:9160':
                return 0
            else:
                return 1

        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000, prefill=True,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         pool_timeout=0.01, timeout=0.05,
                         listeners=[stats_logger], use_threadlocal=False,
                         server_list=['localhost:9160', 'foobar:1'])

        assert_equal(stats_logger.stats['failed'], 4 + get_extra())

        for i in range(0, 7):
            pool.get()

        assert_equal(stats_logger.stats['failed'], 6 + get_extra())

        pool.dispose()
        stats_logger.reset()

        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000, prefill=True,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         pool_timeout=0.01, timeout=0.05,
                         listeners=[stats_logger], use_threadlocal=True,
                         server_list=['localhost:9160', 'foobar:1'])

        assert_equal(stats_logger.stats['failed'], 4 + get_extra())

        threads = []
        for i in range(0, 7):
            threads.append(threading.Thread(target=pool.get))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(stats_logger.stats['failed'], 6 + get_extra())

        pool.dispose()

    def test_queue_failover(self):
        for prefill in (True, False):
            stats_logger = StatsLoggerWithListStorage()
            pool = ConnectionPool(pool_size=1, max_overflow=0, recycle=10000,
                             prefill=prefill, timeout=1,
                             keyspace='PycassaTestKeyspace', credentials=_credentials,
                             listeners=[stats_logger], use_threadlocal=False,
                             server_list=['localhost:9160', 'localhost:9160'])

            cf = ColumnFamily(pool, 'Standard1')

            for i in range(1, 5):
                conn = pool.get()
                setattr(conn, 'send_batch_mutate', conn._fail_once)
                conn._should_fail = True
                conn.return_to_pool()

                # The first insert attempt should fail, but failover should occur
                # and the insert should succeed
                cf.insert('key', {'col': 'val%d' % i, 'col2': 'val'})
                assert_equal(stats_logger.stats['failed'], i)
                assert_equal(cf.get('key'), {'col': 'val%d' % i, 'col2': 'val'})

            pool.dispose()

    def test_queue_threadlocal_failover(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=1, max_overflow=0, recycle=10000,
                         prefill=True, timeout=0.05,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        cf = ColumnFamily(pool, 'Standard1')

        for i in range(1, 5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

            # The first insert attempt should fail, but failover should occur
            # and the insert should succeed
            cf.insert('key', {'col': 'val%d' % i, 'col2': 'val'})
            assert_equal(stats_logger.stats['failed'], i)
            assert_equal(cf.get('key'), {'col': 'val%d' % i, 'col2': 'val'})

        pool.dispose()
        stats_logger.reset()

        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, timeout=0.05,
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        cf = ColumnFamily(pool, 'Standard1')

        for i in range(5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

        threads = []
        args = ('key', {'col': 'val', 'col2': 'val'})
        for i in range(5):
            threads.append(threading.Thread(target=cf.insert, args=args))
            threads[-1].start()
        for thread in threads:
            thread.join()

        assert_equal(stats_logger.stats['failed'], 5)

        pool.dispose()

    def test_queue_retry_limit(self):
        for prefill in (True, False):
            stats_logger = StatsLoggerWithListStorage()
            pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                             prefill=prefill, max_retries=3, # allow 3 retries
                             keyspace='PycassaTestKeyspace', credentials=_credentials,
                             listeners=[stats_logger], use_threadlocal=False,
                             server_list=['localhost:9160', 'localhost:9160'])

            # Corrupt all of the connections
            for i in range(5):
                conn = pool.get()
                setattr(conn, 'send_batch_mutate', conn._fail_once)
                conn._should_fail = True
                conn.return_to_pool()

            cf = ColumnFamily(pool, 'Standard1')
            assert_raises(MaximumRetryException, cf.insert, 'key', {'col': 'val', 'col2': 'val'})
            assert_equal(stats_logger.stats['failed'], 4) # On the 4th failure, didn't retry

            pool.dispose()

    def test_queue_failure_on_retry(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, max_retries=3, # allow 3 retries
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=False,
                         server_list=['localhost:9160', 'localhost:9160'])

        def raiser():
            raise IOError
        # Replace wrapper will open a connection to get the version, so if it
        # fails we need to retry as with any other connection failure
        pool._replace_wrapper = raiser

        # Corrupt all of the connections
        for i in range(5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

        cf = ColumnFamily(pool, 'Standard1')
        assert_raises(MaximumRetryException, cf.insert, 'key', {'col': 'val', 'col2': 'val'})
        assert_equal(stats_logger.stats['failed'], 4) # On the 4th failure, didn't retry

        pool.dispose()

    def test_queue_threadlocal_retry_limit(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, max_retries=3, # allow 3 retries
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=True,
                         server_list=['localhost:9160', 'localhost:9160'])

        # Corrupt all of the connections
        for i in range(5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

        cf = ColumnFamily(pool, 'Standard1')
        assert_raises(MaximumRetryException, cf.insert, 'key', {'col': 'val', 'col2': 'val'})
        assert_equal(stats_logger.stats['failed'], 4) # On the 4th failure, didn't retry

        pool.dispose()

    def test_queue_failure_with_no_retries(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                         prefill=True, max_retries=3, # allow 3 retries
                         keyspace='PycassaTestKeyspace', credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=False,
                         server_list=['localhost:9160', 'localhost:9160'])

        # Corrupt all of the connections
        for i in range(5):
            conn = pool.get()
            setattr(conn, 'send_batch_mutate', conn._fail_once)
            conn._should_fail = True
            conn.return_to_pool()

        cf = ColumnFamily(pool, 'Counter1')
        assert_raises(MaximumRetryException, cf.insert, 'key', {'col': 2, 'col2': 2})
        assert_equal(stats_logger.stats['failed'], 1)  # didn't retry at all

        pool.dispose()

    def test_failure_connection_info(self):
        stats_logger = StatsLoggerRequestInfo()
        pool = ConnectionPool(pool_size=1, max_overflow=0, recycle=10000,
                              prefill=True, max_retries=0,
                              keyspace='PycassaTestKeyspace', credentials=_credentials,
                              listeners=[stats_logger], use_threadlocal=True,
                              server_list=['localhost:9160'])
        cf = ColumnFamily(pool, 'Counter1')

        # Corrupt the connection
        conn = pool.get()
        setattr(conn, 'send_get', conn._fail_once)
        conn._should_fail = True
        conn.return_to_pool()

        assert_raises(MaximumRetryException, cf.get, 'greunt', columns=['col'])
        assert_true('request' in stats_logger.failure_dict['connection'].info)
        request = stats_logger.failure_dict['connection'].info['request']
        assert_equal(request['method'], 'get')
        assert_equal(request['args'], ('greunt', ColumnPath('Counter1', None, 'col'), 1))
        assert_equal(request['kwargs'], {})

    def test_pool_invalid_request(self):
        stats_logger = StatsLoggerWithListStorage()
        pool = ConnectionPool(pool_size=1, max_overflow=0, recycle=10000,
                         prefill=True, max_retries=3,
                         keyspace='PycassaTestKeyspace',
                         credentials=_credentials,
                         listeners=[stats_logger], use_threadlocal=False,
                         server_list=['localhost:9160'])
        cf = ColumnFamily(pool, 'Standard1')
        # Make sure the pool doesn't hide and retries invalid requests
        assert_raises(InvalidRequestException, cf.add, 'key', 'col')
        assert_raises(NotFoundException, cf.get, 'none')
        pool.dispose()


class StatsLoggerWithListStorage(StatsLogger):

    def obtained_server_list(self, dic):
        StatsLogger.obtained_server_list(self, dic)
        self.serv_list = dic.get('server_list')


class StatsLoggerRequestInfo(StatsLogger):

    def connection_failed(self, dic):
        StatsLogger.connection_failed(self, dic)
        self.failure_dict = dic
