from pycassa import index, ColumnFamily, ConsistencyLevel, ConnectionPool, NotFoundException

from nose.tools import assert_raises, assert_equal, assert_true

import struct
import unittest

def setup_module():
    global pool, cf, scf, indexed_cf
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace', credentials=credentials)
    cf = ColumnFamily(pool, 'Standard1', dict_class=TestDict)
    scf = ColumnFamily(pool, 'Super1', dict_class=TestDict)
    indexed_cf = ColumnFamily(pool, 'Indexed1')

def teardown_module():
    pool.dispose()

class TestDict(dict):
    pass

class TestColumnFamily(unittest.TestCase):

    def tearDown(self):
        for key, columns in cf.get_range():
            cf.remove(key)

    def test_empty(self):
        key = 'TestColumnFamily.test_empty'
        assert_raises(NotFoundException, cf.get, key)
        assert_equal(len(cf.multiget([key])), 0)
        for key, columns in cf.get_range():
            assert_equal(len(columns), 0)

    def test_insert_get(self):
        key = 'TestColumnFamily.test_insert_get'
        columns = {'1': 'val1', '2': 'val2'}
        assert_raises(NotFoundException, cf.get, key)
        cf.insert(key, columns)
        assert_equal(cf.get(key), columns)

    def test_insert_multiget(self):
        key1 = 'TestColumnFamily.test_insert_multiget1'
        columns1 = {'1': 'val1', '2': 'val2'}
        key2 = 'test_insert_multiget1'
        columns2 = {'3': 'val1', '4': 'val2'}
        missing_key = 'key3'

        cf.insert(key1, columns1)
        cf.insert(key2, columns2)
        rows = cf.multiget([key1, key2, missing_key])
        assert_equal(len(rows), 2)
        assert_equal(rows[key1], columns1)
        assert_equal(rows[key2], columns2)
        assert_true(missing_key not in rows)

    def test_insert_get_count(self):
        key = 'TestColumnFamily.test_insert_get_count'
        columns = {'1': 'val1', '2': 'val2'}
        cf.insert(key, columns)
        assert_equal(cf.get_count(key), 2)

        assert_equal(cf.get_count(key, column_start='1'), 2)
        assert_equal(cf.get_count(key, column_finish='2'), 2)
        assert_equal(cf.get_count(key, column_start='1', column_finish='2'), 2)
        assert_equal(cf.get_count(key, column_start='1', column_finish='1'), 1)
        assert_equal(cf.get_count(key, columns=['1','2']), 2)
        assert_equal(cf.get_count(key, columns=['1']), 1)

    def test_insert_multiget_count(self):
        keys = ['TestColumnFamily.test_insert_multiget_count1',
               'TestColumnFamily.test_insert_multiget_count2',
               'TestColumnFamily.test_insert_multiget_count3']
        columns = {'1': 'val1', '2': 'val2'}
        for key in keys:
            cf.insert(key, columns)
        result = cf.multiget_count(keys)
        assert_equal(result[keys[0]], 2)
        assert_equal(result[keys[1]], 2)
        assert_equal(result[keys[2]], 2)

        result = cf.multiget_count(keys, column_start='1')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = cf.multiget_count(keys, column_finish='2')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = cf.multiget_count(keys, column_start='1', column_finish='2')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = cf.multiget_count(keys, column_start='1', column_finish='1')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 1)

        result = cf.multiget_count(keys, columns=['1','2'])
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = cf.multiget_count(keys, columns=['1'])
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 1)

    def test_insert_get_range(self):
        keys = ['TestColumnFamily.test_insert_get_range%s' % i for i in xrange(5)]
        columns = {'1': 'val1', '2': 'val2'}
        for key in keys:
            cf.insert(key, columns)

        rows = list(cf.get_range(start=keys[0], finish=keys[-1]))
        assert_equal(len(rows), len(keys))
        for i, (k, c) in enumerate(rows):
            assert_equal(k, keys[i])
            assert_equal(c, columns)

    def test_get_range_batching(self):
        cf.truncate()

        keys = []
        columns = {'c': 'v'}
        for i in range(100, 201):
            keys.append('key%d' % i)
            cf.insert('key%d' % i, columns)

        for i in range(201, 301):
            cf.insert('key%d' % i, columns)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=10):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=1000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=150):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=7):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in cf.get_range(row_count=100, buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        # Put the remaining keys in our list
        for i in range(201, 301):
            keys.append('key%d' % i)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=7):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=200):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(row_count=10000, buffer_size=10000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        # Don't give a row count
        count = 0
        for (k,v) in cf.get_range(buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(buffer_size=77):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(buffer_size=200):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in cf.get_range(buffer_size=10000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        cf.truncate()

    def insert_insert_get_indexed_slices(self):
        indexed_cf = ColumnFamily(pool, 'Indexed1')

        columns = {'birthdate': 1L}

        keys = []
        for i in range(1,4):
            indexed_cf.insert('key%d' % i, columns)
            keys.append('key%d')

        expr = index.create_index_expression(column_name='birthdate', value=1L)
        clause = index.create_index_clause([expr])

        count = 0
        for key,cols in indexed_cf.get_indexed_slices(clause):
            assert_equal(cols, columns)
            assert key in keys
            count += 1
        assert_equal(count, 3)

    def test_get_indexed_slices_batching(self):
        indexed_cf = ColumnFamily(pool, 'Indexed1')

        columns = {'birthdate': 1L}

        for i in range(200):
            indexed_cf.insert('key%d' % i, columns)

        expr = index.create_index_expression(column_name='birthdate', value=1L)
        clause = index.create_index_clause([expr], count=10)

        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=2))
        assert_equal(len(result), 10)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=10))
        assert_equal(len(result), 10)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=77))
        assert_equal(len(result), 10)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=200))
        assert_equal(len(result), 10)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=1000))
        assert_equal(len(result), 10)

        clause = index.create_index_clause([expr], count=250)

        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=2))
        assert_equal(len(result), 200)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=10))
        assert_equal(len(result), 200)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=77))
        assert_equal(len(result), 200)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=200))
        assert_equal(len(result), 200)
        result = list(indexed_cf.get_indexed_slices(clause, buffer_size=1000))
        assert_equal(len(result), 200)

    def test_remove(self):
        key = 'TestColumnFamily.test_remove'
        columns = {'1': 'val1', '2': 'val2'}
        cf.insert(key, columns)

        cf.remove(key, columns=['2'])
        del columns['2']
        assert_equal(cf.get(key), {'1': 'val1'})

        cf.remove(key)
        assert_raises(NotFoundException, cf.get, key)

    def test_dict_class(self):
        key = 'TestColumnFamily.test_dict_class'
        cf.insert(key, {'1': 'val1'})
        assert isinstance(cf.get(key), TestDict)

class TestSuperColumnFamily:

    def tearDown(self):
        for key, columns in scf.get_range():
            scf.remove(key)

    def test_super(self):
        key = 'TestSuperColumnFamily.test_super'
        columns = {'1': {'sub1': 'val1', 'sub2': 'val2'}, '2': {'sub3': 'val3', 'sub4': 'val4'}}
        assert_raises(NotFoundException, scf.get, key)
        scf.insert(key, columns)
        assert_equal(scf.get(key), columns)
        assert_equal(scf.multiget([key]), {key: columns})
        assert_equal(list(scf.get_range(start=key, finish=key)), [(key, columns)])

    def test_super_column_argument(self):
        key = 'TestSuperColumnFamily.test_super_columns_argument'
        sub12 = {'sub1': 'val1', 'sub2': 'val2'}
        sub34 = {'sub3': 'val3', 'sub4': 'val4'}
        columns = {'1': sub12, '2': sub34}
        scf.insert(key, columns)
        assert_equal(scf.get(key, super_column='1'), sub12)
        assert_raises(NotFoundException, scf.get, key, super_column='3')
        assert_equal(scf.multiget([key], super_column='1'), {key: sub12})
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1')), [(key, sub12)])
