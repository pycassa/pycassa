import threading
import unittest

from nose.tools import assert_raises
from pycassa import SystemManager
from pycassa.cassandra.ttypes import CfDef, KsDef, InvalidRequestException

class SystemManagerTest(unittest.TestCase):

    def setUp(self):
        self.sys = SystemManager()

    def tearDown(self):
        self.sys.close()

    def test_system_calls(self):

        # keyspace modifications
        try:
            self.sys.drop_keyspace('TestKeyspace')
        except InvalidRequestException:
            pass
        self.sys.create_keyspace('TestKeyspace', 3, 'SimpleStrategy')
        self.sys.alter_keyspace('TestKeyspace', replication_factor=2)
        self.sys.drop_keyspace('TestKeyspace')

        # column family modifications
        try:
            self.sys.drop_column_family('TestCF', 'Keyspace1')
        except InvalidRequestException:
            pass
        self.sys.create_column_family('Keyspace1', 'TestCF')
        self.sys.alter_column_family('Keyspace1', 'TestCF', comment='testing')
        self.sys.drop_column_family('Keyspace1', 'TestCF')

    def test_misc(self):
        
        self.sys.describe_ring('Keyspace1')
        self.sys.describe_cluster_name()
        self.sys.describe_version()
        self.sys.describe_schema_versions()
