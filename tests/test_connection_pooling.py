import threading
import unittest

from nose.tools import assert_raises
from pycassa import connect, connect_thread_local, NullPool, StaticPool,\
                    AssertionPool, SingletonThreadPool, QueuePool,\
                    ColumnFamily

_credentials = {'username':'jsmith', 'password':'havebadpass'}

class PoolingCase(unittest.TestCase):

    def test_basic_null_pool(self):
        pool = NullPool(keyspace='Keyspace1', credentials=_credentials)
        pool = pool.recreate()
        conn_record = pool.get()
        cf = ColumnFamily(conn_record.connection, 'Standard1')
        cf.insert('key1', {'col':'val'})
        pool.status()
        pool.return_conn(conn_record)
        pool.dispose()

    def test_basic_static_pool(self):
        pool = StaticPool(keyspace='Keyspace1', credentials=_credentials)
        pool = pool.recreate()
        conn_record = pool.get()
        cf = ColumnFamily(conn_record.connection, 'Standard1')
        cf.insert('key1', {'col':'val'})
        pool.status()
        pool.return_conn(conn_record)
        pool.dispose()

    def test_basic_assertion_pool(self):
        pool = AssertionPool(keyspace='Keyspace1', credentials=_credentials)
        pool = pool.recreate()
        conn_record = pool.get()
        cf = ColumnFamily(conn_record.connection, 'Standard1')
        cf.insert('key1', {'col':'val'})
        pool.status()
        pool.return_conn(conn_record)
        pool.dispose()

    def test_basic_singleton_thread_pool(self):
        pool = SingletonThreadPool(keyspace='Keyspace1', credentials=_credentials)
        pool = pool.recreate()
        conn_record = pool.get()
        cf = ColumnFamily(conn_record.connection, 'Standard1')
        cf.insert('key1', {'col':'val'})
        pool.status()
        pool.return_conn(conn_record)
        pool.dispose()

    def test_basic_queue_pool(self):
        pool = QueuePool(keyspace='Keyspace1', credentials=_credentials)
        pool = pool.recreate()
        conn_record = pool.get()
        cf = ColumnFamily(conn_record.connection, 'Standard1')
        cf.insert('key1', {'col':'val'})
        pool.status()
        pool.return_conn(conn_record)
        pool.dispose()

    def test_connections(self):
        def version_check(connection, version):
            assert connection.describe_version() == version

        version = connect('Keyspace1').describe_version()

        thread_local = connect_thread_local('Keyspace1')
        threads = []
        for i in xrange(10):
            threads.append(threading.Thread(target=version_check,
                                            args=(thread_local, version)))
            threads[-1].start()
        for thread in threads:
            thread.join()

    def test_api_version_check(self):
        import pycassa.connection
        pycassa.connection.API_VERSION = ['FOO']
        try:
            assert_raises(AssertionError, connect('Keyspace1').describe_version)
        finally:
            reload(pycassa.connection)



