from datetime import datetime
import unittest

import pycassa.types as types
from pycassa import index, ColumnFamily, ConnectionPool, \
    ColumnFamilyMap, NotFoundException, SystemManager
from nose.tools import assert_raises, assert_equal, assert_true
from nose.plugins.skip import *


CF = 'Standard1'
SCF = 'Super1'
INDEXED_CF = 'Indexed1'

def setup_module():
    global pool, sys_man
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace', credentials=credentials)
    sys_man = SystemManager()

def teardown_module():
    pool.dispose()

class TestUTF8(object):
    strcol = types.AsciiType(default='default')
    intcol = types.LongType(default=0)
    floatcol = types.FloatType(default=0.0)
    datetimecol = types.DateType()

    def __str__(self):
        return str(map(str, [self.strcol, self.intcol, self.floatcol, self.datetimecol]))

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return self.__dict__ != other.__dict__

class TestIndex(object):
    birthdate = types.LongType(default=0)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return self.__dict__ != other.__dict__

class TestEmpty(object):
    pass

class TestColumnFamilyMap(unittest.TestCase):

    def setUp(self):
        self.map = ColumnFamilyMap(TestUTF8, pool, CF)
        self.indexed_map = ColumnFamilyMap(TestIndex, pool, INDEXED_CF)
        self.empty_map = ColumnFamilyMap(TestEmpty, pool, CF, raw_columns=True)

    def tearDown(self):
        for instance in self.map.get_range():
            self.map.remove(instance)
        for instance in self.indexed_map.get_range():
            self.indexed_map.remove(instance)

    def instance(self, key):
        instance = TestUTF8()
        instance.key = key
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)

        return instance

    def test_empty(self):
        key = 'TestColumnFamilyMap.test_empty'
        assert_raises(NotFoundException, self.map.get, key)
        assert_equal(len(self.map.multiget([key])), 0)

    def test_insert_get(self):
        instance = self.instance('TestColumnFamilyMap.test_insert_get')
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        assert_equal(self.map.get(instance.key), instance)

    def test_insert_get_indexed_slices(self):
        instance1 = TestIndex()
        instance1.key = 'key1'
        instance1.birthdate = 1L
        self.indexed_map.insert(instance1)

        instance2 = TestIndex()
        instance2.key = 'key2'
        instance2.birthdate = 1L
        self.indexed_map.insert(instance2)

        instance3 = TestIndex()
        instance3.key = 'key3'
        instance3.birthdate = 2L
        self.indexed_map.insert(instance3)

        expr = index.create_index_expression(column_name='birthdate', value=2L)
        clause = index.create_index_clause([expr])

        result = self.indexed_map.get_indexed_slices(index_clause=clause)
        count = 0
        for instance in result:
            assert_equal(instance, instance3)
            count +=1
        assert_equal(count, 1)

    def test_insert_multiget(self):
        instance1 = self.instance('TestColumnFamilyMap.test_insert_multiget1')
        instance2 = self.instance('TestColumnFamilyMap.test_insert_multiget2')
        missing_key = 'TestColumnFamilyMap.test_insert_multiget3'

        self.map.insert(instance1)
        self.map.insert(instance2)
        rows = self.map.multiget([instance1.key, instance2.key, missing_key])
        assert_equal(len(rows), 2)
        assert_equal(rows[instance1.key], instance1)
        assert_equal(rows[instance2.key], instance2)
        assert_true(missing_key not in rows)

    def test_insert_get_range(self):
        if sys_man.describe_partitioner() == 'RandomPartitioner':
            raise SkipTest('Cannot use RandomPartitioner for this test')

        instances = []
        for i in xrange(5):
            instance = self.instance('TestColumnFamilyMap.test_insert_get_range%s' % i)
            instances.append(instance)

        for instance in instances:
            self.map.insert(instance)

        rows = list(self.map.get_range(start=instances[0].key, finish=instances[-1].key))
        assert_equal(len(rows), len(instances))
        assert_equal(rows, instances)

    def test_remove(self):
        instance = self.instance('TestColumnFamilyMap.test_remove')

        self.map.insert(instance)
        self.map.remove(instance)
        assert_raises(NotFoundException, self.map.get, instance.key)

    def test_does_not_insert_extra_column(self):
        instance = self.instance('TestColumnFamilyMap.test_does_not_insert_extra_column')
        instance.othercol = 'Test'

        self.map.insert(instance)

        get_instance = self.map.get(instance.key)
        assert_equal(get_instance.strcol, instance.strcol)
        assert_equal(get_instance.intcol, instance.intcol)
        assert_equal(get_instance.floatcol, instance.floatcol)
        assert_equal(get_instance.datetimecol, instance.datetimecol)
        assert_raises(AttributeError, getattr, get_instance, 'othercol')

    def test_has_defaults(self):
        key = 'TestColumnFamilyMap.test_has_defaults'
        ColumnFamily.insert(self.map, key, {'strcol': '1'})
        instance = self.map.get(key)

        assert_equal(instance.intcol, TestUTF8.intcol.default)
        assert_equal(instance.floatcol, TestUTF8.floatcol.default)
        assert_equal(instance.datetimecol, TestUTF8.datetimecol.default)

class TestSuperColumnFamilyMap(unittest.TestCase):

    def setUp(self):
        self.map = ColumnFamilyMap(TestUTF8, pool, SCF)

    def tearDown(self):
        for scols in self.map.get_range():
            for instance in scols.values():
                self.map.remove(instance)

    def instance(self, key, super_column):
        instance = TestUTF8()
        instance.key = key
        instance.super_column = super_column
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)

        return instance

    def test_super(self):
        instance = self.instance('TestSuperColumnFamilyMap.test_super', 'super1')
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        res = self.map.get(instance.key)[instance.super_column]
        assert_equal(res, instance)
        assert_equal(self.map.multiget([instance.key])[instance.key][instance.super_column], instance)
        assert_equal(list(self.map.get_range(start=instance.key, finish=instance.key)), [{instance.super_column: instance}])

    def test_super_remove(self):
        instance1 = self.instance('TestSuperColumnFamilyMap.test_super_remove', 'super1')
        assert_raises(NotFoundException, self.map.get, instance1.key)
        self.map.insert(instance1)

        instance2 = self.instance('TestSuperColumnFamilyMap.test_super_remove', 'super2')
        self.map.insert(instance2)

        self.map.remove(instance2)
        assert_equal(len(self.map.get(instance1.key)), 1)
        assert_equal(self.map.get(instance1.key)[instance1.super_column], instance1)
