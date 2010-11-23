from pycassa import index, ColumnFamily, ConsistencyLevel, ConnectionPool, NotFoundException

from nose.tools import assert_raises, assert_equal, assert_true

import struct
import unittest

class TestDict(dict):
    pass

class TestColumnFamily(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = ConnectionPool(keyspace='Keyspace1', credentials=credentials)
        self.cf = ColumnFamily(self.pool, 'Standard2', dict_class=TestDict)

    def tearDown(self):
        for key, columns in self.cf.get_range():
            self.cf.remove(key)

    def clear(self):
        for key, columns in self.cf.get_range():
            self.cf.remove(key)

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
        assert_true(missing_key not in rows)

    def test_insert_get_count(self):
        key = 'TestColumnFamily.test_insert_get_count'
        columns = {'1': 'val1', '2': 'val2'}
        self.cf.insert(key, columns)
        assert_equal(self.cf.get_count(key), 2)

        assert_equal(self.cf.get_count(key, column_start='1'), 2)
        assert_equal(self.cf.get_count(key, column_finish='2'), 2)
        assert_equal(self.cf.get_count(key, column_start='1', column_finish='2'), 2)
        assert_equal(self.cf.get_count(key, column_start='1', column_finish='1'), 1)
        assert_equal(self.cf.get_count(key, columns=['1','2']), 2)
        assert_equal(self.cf.get_count(key, columns=['1']), 1)

    def test_insert_multiget_count(self):
        keys = ['TestColumnFamily.test_insert_multiget_count1',
               'TestColumnFamily.test_insert_multiget_count2',
               'TestColumnFamily.test_insert_multiget_count3']
        columns = {'1': 'val1', '2': 'val2'}
        for key in keys:
            self.cf.insert(key, columns)
        result = self.cf.multiget_count(keys)
        assert_equal(result[keys[0]], 2)
        assert_equal(result[keys[1]], 2)
        assert_equal(result[keys[2]], 2)

        result = self.cf.multiget_count(keys, column_start='1')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = self.cf.multiget_count(keys, column_finish='2')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = self.cf.multiget_count(keys, column_start='1', column_finish='2')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = self.cf.multiget_count(keys, column_start='1', column_finish='1')
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 1)

        result = self.cf.multiget_count(keys, columns=['1','2'])
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 2)

        result = self.cf.multiget_count(keys, columns=['1'])
        assert_equal(len(result), 3)
        assert_equal(result[keys[0]], 1)

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

    def test_get_range_batching(self):
        self.cf.truncate()

        keys = []
        columns = {'c': 'v'}
        for i in range(100, 201):
            keys.append('key%d' % i)
            self.cf.insert('key%d' % i, columns)

        for i in range(201, 301):
            self.cf.insert('key%d' % i, columns)

        count = 0
        for (k,v) in self.cf.get_range(row_count=100, buffer_size=10):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in self.cf.get_range(row_count=100, buffer_size=1000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in self.cf.get_range(row_count=100, buffer_size=150):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in self.cf.get_range(row_count=100, buffer_size=7):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for (k,v) in self.cf.get_range(row_count=100, buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        # Put the remaining keys in our list
        for i in range(201, 301):
            keys.append('key%d' % i)

        count = 0
        for (k,v) in self.cf.get_range(row_count=10000, buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in self.cf.get_range(row_count=10000, buffer_size=7):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in self.cf.get_range(row_count=10000, buffer_size=200):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in self.cf.get_range(row_count=10000, buffer_size=10000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        # Don't give a row count
        count = 0
        for (k,v) in self.cf.get_range(buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in self.cf.get_range(buffer_size=77):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in self.cf.get_range(buffer_size=200):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for (k,v) in self.cf.get_range(buffer_size=10000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        self.cf.truncate()

    def insert_insert_get_indexed_slices(self):
        indexed_cf = ColumnFamily(self.pool, 'Indexed1')

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
        indexed_cf = ColumnFamily(self.pool, 'Indexed1')

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
        self.cf = ColumnFamily(self.pool, 'Super2', timestamp=self.timestamp)

        try:
            self.timestamp_n = int(self.cf.get('meta')['meta']['timestamp'])
        except NotFoundException:
            self.timestamp_n = 0
        self.clear()

    def tearDown(self):
        self.cf.insert('meta', {'meta': {'timestamp': str(self.timestamp_n)}})

    # Since the timestamp passed to Cassandra will be in the same second
    # with the default timestamp function, causing problems with removing
    # and inserting (Cassandra doesn't know which is later), we supply our own
    def timestamp(self):
        self.timestamp_n += 1
        return self.timestamp_n

    def clear(self):
        for key, columns in self.cf.get_range(include_timestamp=True):
            for subcolumns in columns.itervalues():
                for value, timestamp in subcolumns.itervalues():
                    self.timestamp_n = max(self.timestamp_n, timestamp)
            self.cf.remove(key)

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
