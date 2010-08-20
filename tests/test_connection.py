import threading
import unittest

from nose.tools import assert_raises
from pycassa import connect, connect_thread_local

class ConnectionCase(unittest.TestCase):
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



