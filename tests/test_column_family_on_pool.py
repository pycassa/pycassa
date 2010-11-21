from pycassa import index, ColumnFamily,\
                    ConsistencyLevel, NotFoundException, ConnectionPool

import unittest
from nose.tools import assert_raises, assert_equal

import struct

class TestDict(dict):
    pass

class TestColumnFamilyWithPools(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = ConnectionPool(keyspace='Keyspace1', credentials=credentials)
        self.cf = ColumnFamily(self.pool, 'StdUTF8', dict_class=TestDict)

    def tearDown(self):
        self.cf.truncate()

    def test_empty(self):
        key = 'TestColumnFamily.test_empty'
        assert_raises(NotFoundException, self.cf.get, key)
        assert_equal(len(self.cf.multiget([key])), 0)
        for key, columns in self.cf.get_range():
            assert_equal(len(columns), 0)

    def test_insert_get(self):
        key = 'TestColumnFamily.test_insert_get'
        columns = {'1': 'val1', '2': 'val2'}
        assert_raises(NotFoundException, self.cf.get, key)
        self.cf.insert(key, columns)
        assert_equal(self.cf.get(key), columns)

    def test_insert_multiget(self):
        key1 = 'TestColumnFamily.test_insert_multiget1'
        columns1 = {'1': 'val1', '2': 'val2'}
        key2 = 'test_insert_multiget1'
        columns2 = {'3': 'val1', '4': 'val2'}
        missing_key = 'key3'

        self.cf.insert(key1, columns1)
        self.cf.insert(key2, columns2)
        rows = self.cf.multiget([key1, key2, missing_key])
        assert_equal(len(rows), 2)
        assert_equal(rows[key1], columns1)
        assert_equal(rows[key2], columns2)
        assert missing_key not in rows

    def test_insert_get_count(self):
        key = 'TestColumnFamily.test_insert_get_count'
        columns = {'1': 'val1', '2': 'val2'}
        self.cf.insert(key, columns)
        assert_equal(self.cf.get_count(key), 2)

    def test_insert_get_range(self):
        keys = ['TestColumnFamily.test_insert_get_range%s' % i for i in xrange(5)]
        columns = {'1': 'val1', '2': 'val2'}
        for key in keys:
            self.cf.insert(key, columns)

        rows = list(self.cf.get_range(start=keys[0], finish=keys[-1]))
        assert_equal(len(rows), len(keys))
        for i, (k, c) in enumerate(rows):
            assert_equal(k, keys[i])
            assert_equal(c, columns)

    def test_remove(self):
        key = 'TestColumnFamily.test_remove'
        columns = {'1': 'val1', '2': 'val2'}
        self.cf.insert(key, columns)

        self.cf.remove(key, columns=['2'])
        del columns['2']
        assert_equal(self.cf.get(key), {'1': 'val1'})

        self.cf.remove(key)
        assert_raises(NotFoundException, self.cf.get, key)

    def test_dict_class(self):
        key = 'TestColumnFamily.test_dict_class'
        self.cf.insert(key, {'1': 'val1'})
        assert isinstance(self.cf.get(key), TestDict)

class TestSuperColumnFamily:
    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = ConnectionPool(keyspace='Keyspace1', credentials=credentials)
        self.cf = ColumnFamily(self.pool, 'SuperUTF8')

    def tearDown(self):
        self.cf.truncate()

    def test_super(self):
        key = 'TestSuperColumnFamily.test_super'
        columns = {'1': {'sub1': 'val1', 'sub2': 'val2'}, '2': {'sub3': 'val3', 'sub4': 'val4'}}
        assert_raises(NotFoundException, self.cf.get, key)
        self.cf.insert(key, columns)
        assert_equal(self.cf.get(key), columns)
        assert_equal(self.cf.multiget([key]), {key: columns})
        assert_equal(list(self.cf.get_range(start=key, finish=key)), [(key, columns)])

    def test_super_column_argument(self):
        key = 'TestSuperColumnFamily.test_super_columns_argument'
        sub12 = {'sub1': 'val1', 'sub2': 'val2'}
        sub34 = {'sub3': 'val3', 'sub4': 'val4'}
        columns = {'1': sub12, '2': sub34}
        self.cf.insert(key, columns)
        assert_equal(self.cf.get(key, super_column='1'), sub12)
        assert_raises(NotFoundException, self.cf.get, key, super_column='3')
        assert_equal(self.cf.multiget([key], super_column='1'), {key: sub12})
        assert_equal(list(self.cf.get_range(start=key, finish=key, super_column='1')), [(key, sub12)])
