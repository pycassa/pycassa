import unittest
import pycassa

class ConnectionCase(unittest.TestCase):

    def test_api_version_check(self):
        original_ver = pycassa.connection.LOWEST_COMPATIBLE_VERSION
        conn = pycassa.Connection('PycassaTestKeyspace', 'localhost:9160')
        ver = int(conn.describe_version().split('.',1)[0]) + 1
        pycassa.connection.LOWEST_COMPATIBLE_VERSION = ver
        conn.close()
        try:
            conn = pycassa.Connection('PycassaTestKeyspace', 'localhost:9160')
            assert False
        except AssertionError:
            pass
        finally:
            pycassa.connection.LOWEST_COMPATIBLE_VERSION = original_ver
