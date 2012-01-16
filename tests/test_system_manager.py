import unittest

from nose.tools import assert_equal, assert_raises

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.system_manager import *
from pycassa.cassandra.ttypes import InvalidRequestException
from pycassa.types import LongType

TEST_KS = 'PycassaTestKeyspace'

def setup_module():
    global sys
    sys = SystemManager()

def teardown_module():
    sys.close()

class SystemManagerTest(unittest.TestCase):

    def test_system_calls(self):
        # keyspace modifications
        try:
            sys.drop_keyspace('TestKeyspace')
        except InvalidRequestException:
            pass
        sys.create_keyspace('TestKeyspace', SIMPLE_STRATEGY, {'replication_factor': '3'})
        sys.alter_keyspace('TestKeyspace', strategy_options={'replication_factor': '1'})

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

    def test_bad_comparator(self):
        sys.create_keyspace('TestKeyspace', SIMPLE_STRATEGY, {'replication_factor': '3'})
        for comparator in [types.LongType, 123]:
            assert_raises(TypeError, sys.create_column_family,
                    'TestKeyspace', 'TestBadCF', comparator_type=comparator)
        sys.drop_keyspace('TestKeyspace')

    def test_alter_column_non_bytes_type(self):
        sys.create_column_family(TEST_KS, 'LongCF', comparator_type=LONG_TYPE)
        sys.create_index(TEST_KS, 'LongCF', 3, LONG_TYPE)
        pool = ConnectionPool(TEST_KS)
        cf = ColumnFamily(pool, 'LongCF')
        cf.insert('key', {3: 3})
        assert_equal(cf.get('key')[3], 3)

        sys.alter_column(TEST_KS, 'LongCF', 2, LONG_TYPE)
        cf = ColumnFamily(pool, 'LongCF')
        cf.insert('key', {2: 2})
        assert_equal(cf.get('key')[2], 2)

    def test_alter_column_super_cf(self):
        sys.create_column_family(TEST_KS, 'SuperCF', super=True, comparator_type=TIME_UUID_TYPE,
                                 subcomparator_type=UTF8_TYPE)
        sys.alter_column(TEST_KS, 'SuperCF', 'foobar_col', UTF8_TYPE)
