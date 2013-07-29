from unittest import TestCase
from nose.tools import assert_equal, assert_raises

from pycassa.logging.pool_stats_logger import StatsLogger
from pycassa.pool import ConnectionPool, NoConnectionAvailable, InvalidRequestError

__author__ = 'gilles'

_credentials = {'username': 'jsmith', 'password': 'havebadpass'}

class TestStatsLogger(TestCase):
    def __init__(self, methodName='runTest'):
        super(TestStatsLogger, self).__init__(methodName)

    def setUp(self):
        super(TestStatsLogger, self).setUp()
        self.logger = StatsLogger()

    def test_empty(self):
        assert_equal(self.logger.stats, self.logger._stats)

    def test_connection_created(self):
        self.logger.connection_created({'level': 'info'})
        self.logger.connection_created({'level': 'error'})

        stats = self.logger.stats
        assert_equal(stats['created']['success'], 1)
        assert_equal(stats['created']['failure'], 1)

    def test_connection_checked(self):
        self.logger.connection_checked_out({})
        self.logger.connection_checked_out({})
        self.logger.connection_checked_in({})
        stats = self.logger.stats
        assert_equal(stats['checked_out'], 2)
        assert_equal(stats['checked_in'], 1)
        assert_equal(stats['opened'], {'current': 1, 'max': 2})

    def test_connection_disposed(self):
        self.logger.connection_disposed({'level': 'info'})
        self.logger.connection_disposed({'level': 'error'})

        stats = self.logger.stats
        assert_equal(stats['disposed']['success'], 1)
        assert_equal(stats['disposed']['failure'], 1)

    def test_connection_recycled(self):
        self.logger.connection_recycled({})
        stats = self.logger.stats
        assert_equal(stats['recycled'], 1)

    def test_connection_failed(self):
        self.logger.connection_failed({})
        stats = self.logger.stats
        assert_equal(stats['failed'], 1)

    def test_obtained_server_list(self):
        self.logger.obtained_server_list({})
        stats = self.logger.stats
        assert_equal(stats['list'], 1)

    def test_pool_at_max(self):
        self.logger.pool_at_max({})
        stats = self.logger.stats
        assert_equal(stats['at_max'], 1)


class TestInPool(TestCase):
    def __init__(self, methodName='runTest'):
        super(TestInPool, self).__init__(methodName)

    def test_pool(self):
        listener = StatsLogger()
        pool = ConnectionPool(pool_size=5, max_overflow=5, recycle=10000,
                              prefill=True, pool_timeout=0.1, timeout=1,
                              keyspace='PycassaTestKeyspace', credentials=_credentials,
                              listeners=[listener], use_threadlocal=False)
        conns = []
        for i in range(10):
            conns.append(pool.get())
        assert_equal(listener.stats['created']['success'], 10)
        assert_equal(listener.stats['created']['failure'], 0)
        assert_equal(listener.stats['checked_out'], 10)
        assert_equal(listener.stats['opened'], {'current': 10, 'max': 10})

        # Pool is maxed out now
        assert_raises(NoConnectionAvailable, pool.get)
        assert_equal(listener.stats['created']['success'], 10)
        assert_equal(listener.stats['checked_out'], 10)
        assert_equal(listener.stats['opened'], {'current': 10, 'max': 10})
        assert_equal(listener.stats['at_max'], 1)

        for i in range(0, 5):
            pool.return_conn(conns[i])
        assert_equal(listener.stats['disposed']['success'], 0)
        assert_equal(listener.stats['checked_in'], 5)
        assert_equal(listener.stats['opened'], {'current': 5, 'max': 10})

        for i in range(5, 10):
            pool.return_conn(conns[i])
        assert_equal(listener.stats['disposed']['success'], 5)
        assert_equal(listener.stats['checked_in'], 10)

        conns = []

        # These connections should come from the pool
        for i in range(5):
            conns.append(pool.get())
        assert_equal(listener.stats['created']['success'], 10)
        assert_equal(listener.stats['checked_out'], 15)

        # But these will need to be made
        for i in range(5):
            conns.append(pool.get())
        assert_equal(listener.stats['created']['success'], 15)
        assert_equal(listener.stats['checked_out'], 20)

        assert_equal(listener.stats['disposed']['success'], 5)
        for i in range(10):
            conns[i].return_to_pool()
        assert_equal(listener.stats['checked_in'], 20)
        assert_equal(listener.stats['disposed']['success'], 10)

        assert_raises(InvalidRequestError, conns[0].return_to_pool)
        assert_equal(listener.stats['checked_in'], 20)
        assert_equal(listener.stats['disposed']['success'], 10)

        print "in test:", id(conns[-1])
        conns[-1].return_to_pool()
        assert_equal(listener.stats['checked_in'], 20)
        assert_equal(listener.stats['disposed']['success'], 10)

        pool.dispose()
