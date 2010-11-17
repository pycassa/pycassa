import threading
import unittest

from nose.tools import assert_raises
from pycassa import connect, connect_thread_local
from pycassa.cassandra.ttypes import CfDef, KsDef

class ConnectionCase(unittest.TestCase):
    def test_connections(self):
        def version_check(connection, version):
            assert connection.describe_version() >= version

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
        conn = connect('Keyspace1')
        ver = int(conn.describe_version().split('.',1)[0]) + 1
        pycassa.connection.LOWEST_COMPATIBLE_VERSION = ver
        conn.close()
        try:
            assert_raises(AssertionError, connect('Keyspace1').describe_version)
        finally:
            reload(pycassa.connection)

    def test_system_calls(self):
        conn = connect('Keyspace1')

        # keyspace modifications
        try:
            conn.drop_keyspace('TestKeyspace')
        except:
            pass
        conn.add_keyspace(KsDef('TestKeyspace',
            'org.apache.cassandra.locator.SimpleStrategy', None, 3, []))
        conn.update_keyspace(KsDef('TestKeyspace',
            'org.apache.cassandra.locator.SimpleStrategy', None, 2, []))
        conn.drop_keyspace('TestKeyspace')

        # column family modifications
        try:
            conn.drop_column_family('TestCF')
        except:
            pass
        conn.add_column_family(CfDef('Keyspace1', 'TestCF', 'Standard'))
        cfdef = conn.get_keyspace_description()['TestCF']
        cfdef.comment = 'this is a test'
        conn.update_column_family(cfdef)
        conn.drop_column_family('TestCF')
