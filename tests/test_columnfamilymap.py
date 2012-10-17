from datetime import datetime
import unittest
import uuid

from nose.tools import assert_raises, assert_equal, assert_true
from nose.plugins.skip import SkipTest

import pycassa.types as types
from pycassa import index, ColumnFamily, ConnectionPool, \
    ColumnFamilyMap, NotFoundException, SystemManager

from tests.util import requireOPP

CF = 'Standard1'
SCF = 'Super1'
INDEXED_CF = 'Indexed1'
pool = None
sys_man = None

def setup_module():
    global pool, sys_man
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace',
            credentials=credentials, timeout=1.0)
    sys_man = SystemManager()

def teardown_module():
    pool.dispose()


class TestUTF8(object):
    key = types.LexicalUUIDType()
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
        self.sys_man = sys_man
        self.map = ColumnFamilyMap(TestUTF8, pool, CF)
        self.indexed_map = ColumnFamilyMap(TestIndex, pool, INDEXED_CF)
        self.empty_map = ColumnFamilyMap(TestEmpty, pool, CF, raw_columns=True)

    def tearDown(self):
        for instance in self.map.get_range():
            self.map.remove(instance)
        for instance in self.indexed_map.get_range():
            self.indexed_map.remove(instance)

    def instance(self):
        instance = TestUTF8()
        instance.key = uuid.uuid4()
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)

        return instance

    def test_empty(self):
        key = uuid.uuid4()
        assert_raises(NotFoundException, self.map.get, key)
        assert_equal(len(self.map.multiget([key])), 0)

    def test_insert_get(self):
        instance = self.instance()
        assert_raises(NotFoundException, self.map.get, instance.key)
        ts = self.map.insert(instance)
        assert_true(isinstance(ts, (int, long)))
        assert_equal(self.map.get(instance.key), instance)

    def test_insert_get_omitting_columns(self):
        """
        When omitting columns, pycassa should not try to insert the CassandraType
        instance on a ColumnFamilyMap object
        """
        instance2 = TestUTF8()
        instance2.key = uuid.uuid4()
        instance2.strcol = 'lol'
        instance2.intcol = 2
        assert_raises(NotFoundException, self.map.get, instance2.key)
        self.map.insert(instance2)
        ret_inst = self.map.get(instance2.key)
        assert_equal(ret_inst.key, instance2.key)
        assert_equal(ret_inst.strcol, instance2.strcol)
        assert_equal(ret_inst.intcol, instance2.intcol)

        ## these lines are commented out because, though they should work, wont
        ## because CassandraTypes are not descriptors when used on a ColumnFamilyMap
        ## instance, they are merely class attributes that are overwritten at runtime

        # assert_equal(ret_inst.floatcol, instance2.floatcol)
        # assert_equal(ret_inst.datetimecol, instance2.datetimecol)
        # assert_equal(self.map.get(instance2.key), instance2)

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
            count += 1
        assert_equal(count, 1)

    def test_insert_multiget(self):
        instance1 = self.instance()
        instance2 = self.instance()
        missing_key = uuid.uuid4()

        self.map.insert(instance1)
        self.map.insert(instance2)
        rows = self.map.multiget([instance1.key, instance2.key, missing_key])
        assert_equal(len(rows), 2)
        assert_equal(rows[instance1.key], instance1)
        assert_equal(rows[instance2.key], instance2)
        assert_true(missing_key not in rows)

    @requireOPP
    def test_insert_get_range(self):
        instances = [self.instance() for i in range(5)]
        instances = sorted(instances, key=lambda instance: instance.key)
        for instance in instances:
            self.map.insert(instance)

        rows = list(self.map.get_range(start=instances[0].key, finish=instances[-1].key))
        assert_equal(len(rows), len(instances))
        assert_equal(rows, instances)

    def test_remove(self):
        instance = self.instance()

        self.map.insert(instance)
        self.map.remove(instance)
        assert_raises(NotFoundException, self.map.get, instance.key)

    def test_does_not_insert_extra_column(self):
        instance = self.instance()
        instance.othercol = 'Test'

        self.map.insert(instance)

        get_instance = self.map.get(instance.key)
        assert_equal(get_instance.strcol, instance.strcol)
        assert_equal(get_instance.intcol, instance.intcol)
        assert_equal(get_instance.floatcol, instance.floatcol)
        assert_equal(get_instance.datetimecol, instance.datetimecol)
        assert_raises(AttributeError, getattr, get_instance, 'othercol')

    def test_has_defaults(self):
        key = uuid.uuid4()
        ColumnFamily.insert(self.map, key, {'strcol': '1'})
        instance = self.map.get(key)

        assert_equal(instance.intcol, TestUTF8.intcol.default)
        assert_equal(instance.floatcol, TestUTF8.floatcol.default)
        assert_equal(instance.datetimecol, TestUTF8.datetimecol.default)

    def test_batch_insert(self):
        instances = []
        for i in range(3):
            instance = TestUTF8()
            instance.key = uuid.uuid4()
            instance.strcol = 'instance%s' % (i + 1)
            instances.append(instance)

        for i in instances:
            assert_raises(NotFoundException, self.map.get, i.key)

        self.map.batch_insert(instances)

        for i in instances:
            get_instance = self.map.get(i.key)
            assert_equal(get_instance.key, i.key)
            assert_equal(get_instance.strcol, i.strcol)

class TestSuperColumnFamilyMap(unittest.TestCase):

    def setUp(self):
        self.map = ColumnFamilyMap(TestUTF8, pool, SCF)

    def tearDown(self):
        for scols in self.map.get_range():
            for instance in scols.values():
                self.map.remove(instance)

    def instance(self, super_column):
        instance = TestUTF8()
        instance.key = uuid.uuid4()
        instance.super_column = super_column
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)

        return instance

    def test_super(self):
        instance = self.instance('super1')
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        res = self.map.get(instance.key)[instance.super_column]
        assert_equal(res, instance)
        assert_equal(self.map.multiget([instance.key])[instance.key][instance.super_column], instance)
        assert_equal(list(self.map.get_range(start=instance.key, finish=instance.key)), [{instance.super_column: instance}])

    def test_super_remove(self):
        instance1 = self.instance('super1')
        assert_raises(NotFoundException, self.map.get, instance1.key)
        self.map.insert(instance1)

        instance2 = self.instance('super2')
        self.map.insert(instance2)

        self.map.remove(instance2)
        assert_equal(len(self.map.get(instance1.key)), 1)
        assert_equal(self.map.get(instance1.key)[instance1.super_column], instance1)

    def test_batch_insert_super(self):
        instances = []
        for i in range(3):
            instance = self.instance('super_batch%s' % (i + 1))
            instances.append(instance)

        for i in instances:
            assert_raises(NotFoundException, self.map.get, i.key)

        self.map.batch_insert(instances)

        for i in instances:
            result = self.map.get(i.key)
            get_instance = result[i.super_column]
            assert_equal(len(result), 1)
            assert_equal(get_instance.key, i.key)
            assert_equal(get_instance.super_column, i.super_column)
            assert_equal(get_instance.strcol, i.strcol)
