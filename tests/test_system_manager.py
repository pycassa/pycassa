import unittest
from pycassa.system_manager import *
from pycassa.cassandra.ttypes import InvalidRequestException

class SystemManagerTest(unittest.TestCase):

    def test_system_calls(self):
        sys = SystemManager()

        # keyspace modifications
        try:
            sys.drop_keyspace('TestKeyspace')
        except InvalidRequestException:
            pass
        sys.create_keyspace('TestKeyspace', 3, SIMPLE_STRATEGY)
        sys.alter_keyspace('TestKeyspace', replication_factor=1)

        sys.create_column_family('TestKeyspace', 'TestCF')
        sys.alter_column_family('TestKeyspace', 'TestCF', comment='testing')
        sys.create_index('TestKeyspace', 'TestCF', 'column', LONG_TYPE)
        sys.drop_column_family('TestKeyspace', 'TestCF')
 
        sys.describe_ring('TestKeyspace')
        sys.describe_cluster_name()
        sys.describe_version()
        sys.describe_schema_versions()
        sys.list_keyspaces()

        sys.drop_keyspace('TestKeyspace')
