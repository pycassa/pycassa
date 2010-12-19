from datetime import datetime

from pycassa import index, ColumnFamily, ConnectionPool, \
    ColumnFamilyMap, ConsistencyLevel, NotFoundException, String, Int64, \
    Float64, DateTime, IntString, FloatString, DateTimeString
from nose.tools import assert_raises, assert_equal, assert_true

import struct

def setup_module():
    global pool, cf, scf, indexed_cf
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace', credentials=credentials)
    cf = ColumnFamily(pool, 'Standard1', autopack_names=False, autopack_values=False)
    scf = ColumnFamily(pool, 'Super1', autopack_names=False, autopack_values=False)
    indexed_cf = ColumnFamily(pool, 'Indexed1', autopack_names=False, autopack_values=False)

def teardown_module():
    pool.dispose()

class TestUTF8(object):
    strcol = String(default='default')
    intcol = Int64(default=0)
    floatcol = Float64(default=0.0)
    datetimecol = DateTime(default=None)
    intstrcol = IntString()
    floatstrcol = FloatString()
    datetimestrcol = DateTimeString()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return self.__dict__ != other.__dict__

class TestIndex(object):
    birthdate = Int64(default=0)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return self.__dict__ != other.__dict__

class TestEmpty(object):
    pass

class TestColumnFamilyMap:
    def setUp(self):
        self.map = ColumnFamilyMap(TestUTF8, cf)
        self.indexed_map = ColumnFamilyMap(TestIndex, indexed_cf)
        self.empty_map = ColumnFamilyMap(TestEmpty, cf, raw_columns=True)

    def tearDown(self):
        for key, columns in cf.get_range():
            cf.remove(key)
        for key, columns in indexed_cf.get_range():
            cf.remove(key)

    def instance(self, key):
        instance = TestUTF8()
        instance.key = key
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)
        instance.intstrcol = 8
        instance.floatstrcol = 4.6
        instance.datetimestrcol = datetime.now().replace(microsecond=0)

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
        assert_equal(self.empty_map.get(instance.key).raw_columns['intstrcol'], str(instance.intstrcol))

    def test_insert_get_indexed_slices(self):
        instance = TestIndex()
        instance.key = 'key'
        instance.birthdate = 1L
        self.indexed_map.insert(instance)
        instance.key = 'key2'
        self.indexed_map.insert(instance)
        instance.key = 'key3'
        self.indexed_map.insert(instance)

        expr = index.create_index_expression(column_name='birthdate', value=1L)
        clause = index.create_index_clause([expr])
        result = self.indexed_map.get_indexed_slices(instance, index_clause=clause)
        assert_equal(len(result), 3)
        assert_equal(result.get('key3'), instance)

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
        assert_equal(self.empty_map.multiget([instance1.key])[instance1.key].raw_columns['intstrcol'], str(instance1.intstrcol))

    def test_insert_get_count(self):
        instance = self.instance('TestColumnFamilyMap.test_insert_get_count')
        self.map.insert(instance)
        assert_equal(self.map.get_count(instance.key), 7)

    def test_insert_get_range(self):
        instances = []
        for i in xrange(5):
            instance = self.instance('TestColumnFamilyMap.test_insert_get_range%s' % i)
            instances.append(instance)

        for instance in instances:
            self.map.insert(instance)

        rows = list(self.map.get_range(start=instances[0].key, finish=instances[-1].key))
        assert_equal(len(rows), len(instances))
        assert_equal(rows, instances)
        assert_equal(list(self.empty_map.get_range(start=instances[0].key, finish=instances[0].key))[0].raw_columns['intstrcol'], str(instances[0].intstrcol))

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
        cf.insert(key, {'strcol': '1'})
        instance = self.map.get(key)

        assert_equal(instance.intcol, TestUTF8.intcol.default)
        assert_equal(instance.floatcol, TestUTF8.floatcol.default)
        assert_equal(instance.datetimecol, TestUTF8.datetimecol.default)
        assert_equal(instance.intstrcol, TestUTF8.intstrcol.default)
        assert_equal(instance.floatstrcol, TestUTF8.floatstrcol.default)
        assert_equal(instance.datetimestrcol, TestUTF8.datetimestrcol.default)

class TestSuperColumnFamilyMap:
    def setUp(self):
        self.map = ColumnFamilyMap(TestUTF8, scf)

    def tearDown(self):
        for key, columns in scf.get_range():
            scf.remove(key)

    def instance(self, key, super_column):
        instance = TestUTF8()
        instance.key = key
        instance.super_column = super_column
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)
        instance.intstrcol = 8
        instance.floatstrcol = 4.6
        instance.datetimestrcol = datetime.now().replace(microsecond=0)

        return instance

    def test_super(self):
        instance = self.instance('TestSuperColumnFamilyMap.test_super', 'super1')
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        res = self.map.get(instance.key)[instance.super_column]
        assert_equal(res, instance)
        assert_equal(self.map.multiget([instance.key])[instance.key][instance.super_column], instance)
        assert_equal(list(self.map.get_range(start=instance.key, finish=instance.key)), [{instance.super_column: instance}])
