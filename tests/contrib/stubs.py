import unittest

from nose.tools import assert_raises, assert_equal, assert_true

from pycassa import index, ColumnFamily, ConnectionPool,\
                    NotFoundException, SystemManager

from pycassa.contrib.stubs import ColumnFamilyStub, ConnectionPoolStub, \
                                  SystemManagerStub

pool = cf = None
pool_stub = cf_stub = None


def setup_module():
    global pool, cf, indexed_cf, pool_stub, indexed_cf_stub, cf_stub
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace',
            credentials=credentials, timeout=1.0)
    cf = ColumnFamily(pool, 'Standard1', dict_class=TestDict)
    indexed_cf = ColumnFamily(pool, 'Indexed1')

    pool_stub = ConnectionPoolStub(keyspace='PycassaTestKeyspace',
            credentials=credentials, timeout=1.0)
    cf_stub = ColumnFamilyStub(pool_stub, 'Standard1', dict_class=TestDict)
    indexed_cf_stub = ColumnFamilyStub(pool_stub, 'Indexed1')


def teardown_module():
    cf.truncate()
    cf_stub.truncate()
    indexed_cf.truncate()
    indexed_cf_stub.truncate()
    pool.dispose()


class TestDict(dict):
    pass


class TestColumnFamilyStub(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        for test_cf in (cf, cf_stub):
            for key, columns in test_cf.get_range():
                test_cf.remove(key)

    def test_empty(self):
        key = 'TestColumnFamily.test_empty'

        for test_cf in (cf, cf_stub):
            assert_raises(NotFoundException, test_cf.get, key)
            assert_equal(len(test_cf.multiget([key])), 0)
            for key, columns in test_cf.get_range():
                assert_equal(len(columns), 0)

    def test_insert_get(self):
        key = 'TestColumnFamily.test_insert_get'
        columns = {'1': 'val1', '2': 'val2'}
        for test_cf in (cf, cf_stub):
            assert_raises(NotFoundException, test_cf.get, key)
            ts = test_cf.insert(key, columns)
            assert_true(isinstance(ts, (int, long)))
            assert_equal(test_cf.get(key), columns)

    def test_insert_get_column_start(self):
        key = 'TestColumnFamily.test_insert_get_column_start'
        columns = {'1': 'val1', '2': 'val2', '3': 'val3'}
        for test_cf in (cf, cf_stub):
            assert_raises(NotFoundException, test_cf.get, key)
            ts = test_cf.insert(key, columns)
            assert_true(isinstance(ts, (int, long)))
            assert_equal(test_cf.get(key, column_start='2'), {'2': 'val2', '3': 'val3'})


    def test_insert_get_column_finish(self):
        key = 'TestColumnFamily.test_insert_get_column_finish'
        columns = {'a': 'val1', 'b': 'val2', 'c': 'val3'}
        for test_cf in (cf, cf_stub):
            assert_raises(NotFoundException, test_cf.get, key)
            ts = test_cf.insert(key, columns)
            assert_true(isinstance(ts, (int, long)))
            assert_equal(test_cf.get(key, column_finish='b'), {'a': 'val1', 'b': 'val2'})


    def test_insert_get_column_start_and_finish(self):
        key = 'TestColumnFamily.test_insert_get_column_start_and_finish'
        columns = {'a': 'val1', 'b': 'val2', 'c': 'val3', 'd': 'val4'}
        for test_cf in (cf, cf_stub):
            assert_raises(NotFoundException, test_cf.get, key)
            ts = test_cf.insert(key, columns)
            assert_true(isinstance(ts, (int, long)))
            assert_equal(test_cf.get(key, column_start='b', column_finish='c'), {'b': 'val2', 'c': 'val3'})

    def test_insert_multiget(self):
        key1 = 'TestColumnFamily.test_insert_multiget1'
        columns1 = {'1': 'val1', '2': 'val2'}
        key2 = 'test_insert_multiget1'
        columns2 = {'3': 'val1', '4': 'val2'}
        missing_key = 'key3'

        for test_cf in (cf, cf_stub):
            test_cf.insert(key1, columns1)
            test_cf.insert(key2, columns2)
            rows = test_cf.multiget([key1, key2, missing_key])
            assert_equal(len(rows), 2)
            assert_equal(rows[key1], columns1)
            assert_equal(rows[key2], columns2)
            assert_true(missing_key not in rows)


    def insert_insert_get_indexed_slices(self):
        columns = {'birthdate': 1L}

        keys = set()
        for i in range(1, 4):
            indexed_cf.insert('key%d' % i, columns)
            indexed_cf_stub.insert('key%d' % i, columns)
            keys.add('key%d' % i)

        expr = index.create_index_expression(column_name='birthdate', value=1L)
        clause = index.create_index_clause([expr])

        for test_indexed_cf in (indexed_cf, indexed_cf_stub):
            count = 0
            for key, cols in test_indexed_cf.get_indexed_slices(clause):
                assert_equal(cols, columns)
                assert key in keys
                count += 1
            assert_equal(count, 3)


    def test_remove(self):
        key = 'TestColumnFamily.test_remove'
        for test_cf in (cf, cf_stub):
            columns = {'1': 'val1', '2': 'val2'}
            test_cf.insert(key, columns)

            # An empty list for columns shouldn't delete anything
            test_cf.remove(key, columns=[])
            assert_equal(test_cf.get(key), columns)

            test_cf.remove(key, columns=['2'])
            del columns['2']
            assert_equal(test_cf.get(key), {'1': 'val1'})

            test_cf.remove(key)
            assert_raises(NotFoundException, test_cf.get, key)
