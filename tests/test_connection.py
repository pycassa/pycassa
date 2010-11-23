import threading
import unittest

from nose.tools import assert_raises
import pycassa
from pycassa.cassandra.ttypes import CfDef, KsDef, InvalidRequestException

class ConnectionCase(unittest.TestCase):

    def test_api_version_check(self):
        original_ver = pycassa.connection.LOWEST_COMPATIBLE_VERSION
        conn = pycassa.Connection('Keyspace1', 'localhost:9160')
        ver = int(conn.describe_version().split('.',1)[0]) + 1
        pycassa.connection.LOWEST_COMPATIBLE_VERSION = ver
        conn.close()
        try:
            conn = pycassa.Connection('Keyspace1', 'localhost:9160')
            assert False
        except AssertionError:
            pass
        finally:
            pycassa.connection.LOWEST_COMPATIBLE_VERSION = original_ver
