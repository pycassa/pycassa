import unittest

from nose.tools import assert_raises, assert_equal, assert_true

from pycassa import index, ColumnFamily, ConnectionPool,\
                    NotFoundException, SystemManager
from pycassa.util import OrderedDict

from tests.util import requireOPP

pool = cf = scf = indexed_cf = counter_cf = counter_scf = sys_man = None

def setup_module():
    global pool, cf, scf, indexed_cf, counter_cf, counter_scf, sys_man
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(keyspace='PycassaTestKeyspace',
            credentials=credentials, timeout=1.0)
    cf = ColumnFamily(pool, 'Standard1', dict_class=TestDict)
    scf = ColumnFamily(pool, 'Super1', dict_class=dict)
    indexed_cf = ColumnFamily(pool, 'Indexed1')
    sys_man = SystemManager()
    counter_cf = ColumnFamily(pool, 'Counter1')
    counter_scf = ColumnFamily(pool, 'SuperCounter1')

def teardown_module():
    cf.truncate()
    indexed_cf.truncate()
    counter_cf.truncate()
    counter_scf.truncate()
    pool.dispose()


class TestDict(dict):
    pass


class TestColumnFamily(unittest.TestCase):

    def setUp(self):
        self.sys_man = sys_man

    def tearDown(self):
        for key, columns in cf.get_range():
            cf.remove(key)
        for key, columns in indexed_cf.get_range():
            cf.remove(key)

    def test_bad_kwarg(self):
        assert_raises(TypeError,
                ColumnFamily.__init__, pool, 'test', bar='foo')

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
        ts = cf.insert(key, columns)
        assert_true(isinstance(ts, (int, long)))
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

    def test_multiget_multiple_bad_key(self):
        key = 'efefefef'
        cf.multiget([key, key, key])

    def test_insert_get_count(self):
        key = 'TestColumnFamily.test_insert_get_count'
        columns = {'1': 'val1', '2': 'val2'}
        cf.insert(key, columns)
        assert_equal(cf.get_count(key), 2)

        assert_equal(cf.get_count(key, column_start='1'), 2)
        assert_equal(cf.get_count(key, column_finish='2'), 2)
        assert_equal(cf.get_count(key, column_start='1', column_finish='2'), 2)
        assert_equal(cf.get_count(key, column_start='1', column_finish='1'), 1)
        assert_equal(cf.get_count(key, columns=['1', '2']), 2)
        assert_equal(cf.get_count(key, columns=['1']), 1)
        assert_equal(cf.get_count(key, max_count=1), 1)
        assert_equal(cf.get_count(key, max_count=1, column_reversed=True), 1)
        assert_equal(cf.get_count(key, column_reversed=True), 2)
        assert_equal(cf.get_count(key, column_start='1', column_reversed=True), 1)

    def test_insert_multiget_count(self):
        keys = ['TestColumnFamily.test_insert_multiget_count1',
                'TestColumnFamily.test_insert_multiget_count2',
                'TestColumnFamily.test_insert_multiget_count3']
        for key in keys:
            cf.insert(key, {'1': 'val1', '2': 'val2'})

        result = cf.multiget_count(keys)
        assert_equal([result[k] for k in keys], [2 for key in keys])

        result = cf.multiget_count(keys, column_start='1')
        assert_equal([result[k] for k in keys], [2 for key in keys])

        result = cf.multiget_count(keys, column_finish='2')
        assert_equal([result[k] for k in keys], [2 for key in keys])

        result = cf.multiget_count(keys, column_start='1', column_finish='2')
        assert_equal([result[k] for k in keys], [2 for key in keys])

        result = cf.multiget_count(keys, column_start='1', column_finish='1')
        assert_equal([result[k] for k in keys], [1 for key in keys])

        result = cf.multiget_count(keys, columns=['1', '2'])
        assert_equal([result[k] for k in keys], [2 for key in keys])

        result = cf.multiget_count(keys, columns=['1'])
        assert_equal([result[k] for k in keys], [1 for key in keys])

        result = cf.multiget_count(keys, max_count=1)
        assert_equal([result[k] for k in keys], [1 for key in keys])

        result = cf.multiget_count(keys, column_start='1', column_reversed=True)
        assert_equal([result[k] for k in keys], [1 for key in keys])

    @requireOPP
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

    @requireOPP
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
        for k, v in cf.get_range(row_count=100, buffer_size=10):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for k, v in cf.get_range(row_count=100, buffer_size=1000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for k, v in cf.get_range(row_count=100, buffer_size=150):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for k, v in cf.get_range(row_count=100, buffer_size=7):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        count = 0
        for k, v in cf.get_range(row_count=100, buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 100)

        # Put the remaining keys in our list
        for i in range(201, 301):
            keys.append('key%d' % i)

        count = 0
        for k, v in cf.get_range(row_count=10000, buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for k, v in cf.get_range(row_count=10000, buffer_size=7):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for k, v in cf.get_range(row_count=10000, buffer_size=200):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for k, v in cf.get_range(row_count=10000, buffer_size=10000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        # Don't give a row count
        count = 0
        for k, v in cf.get_range(buffer_size=2):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for k, v in cf.get_range(buffer_size=77):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for k, v in cf.get_range(buffer_size=200):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        count = 0
        for k, v in cf.get_range(buffer_size=10000):
            assert_true(k in keys, 'key "%s" should be in keys' % k)
            count += 1
        assert_equal(count, 201)

        cf.truncate()

    @requireOPP
    def test_get_range_tokens(self):
        cf.truncate()
        columns = {'c': 'v'}
        for i in range(100, 201):
            cf.insert('key%d' % i, columns)

        results = list(cf.get_range(start_token="key100".encode('hex'), finish_token="key200".encode('hex')))
        assert_equal(100, len(results))

        results = list(cf.get_range(start_token="key100".encode('hex'), finish_token="key200".encode('hex'), buffer_size=10))
        assert_equal(100, len(results))

        results = list(cf.get_range(start_token="key100".encode('hex'), buffer_size=10))
        assert_equal(100, len(results))

        results = list(cf.get_range(finish_token="key201".encode('hex'), buffer_size=10))
        assert_equal(101, len(results))

    def insert_insert_get_indexed_slices(self):
        indexed_cf = ColumnFamily(pool, 'Indexed1')

        columns = {'birthdate': 1L}

        keys = []
        for i in range(1, 4):
            indexed_cf.insert('key%d' % i, columns)
            keys.append('key%d')

        expr = index.create_index_expression(column_name='birthdate', value=1L)
        clause = index.create_index_clause([expr])

        count = 0
        for key, cols in indexed_cf.get_indexed_slices(clause):
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

    def test_multiget_batching(self):
        key_prefix = "TestColumnFamily.test_multiget_batching"
        keys = []
        expected = OrderedDict()
        for i in range(10):
            key = key_prefix + str(i)
            keys.append(key)
            expected[key] = {'col': 'val'}
            cf.insert(key, {'col': 'val'})

        assert_equal(cf.multiget(keys, buffer_size=1), expected)
        assert_equal(cf.multiget(keys, buffer_size=2), expected)
        assert_equal(cf.multiget(keys, buffer_size=3), expected)
        assert_equal(cf.multiget(keys, buffer_size=9), expected)
        assert_equal(cf.multiget(keys, buffer_size=10), expected)
        assert_equal(cf.multiget(keys, buffer_size=11), expected)
        assert_equal(cf.multiget(keys, buffer_size=100), expected)

    def test_add(self):
        counter_cf.add('key', 'col')
        result = counter_cf.get('key')
        assert_equal(result['col'], 1)

        counter_cf.add('key', 'col')
        result = counter_cf.get('key')
        assert_equal(result['col'], 2)

        counter_cf.add('key', 'col2')
        result = counter_cf.get('key')
        assert_equal(result, {'col': 2, 'col2': 1})

    def test_insert_counters(self):
        counter_cf.insert('counter_key', {'col1': 1})
        result = counter_cf.get('counter_key')
        assert_equal(result['col1'], 1)

        counter_cf.insert('counter_key', {'col1': 1, 'col2': 1})
        result = counter_cf.get('counter_key')
        assert_equal(result, {'col1': 2, 'col2': 1})

    def test_remove(self):
        key = 'TestColumnFamily.test_remove'
        columns = {'1': 'val1', '2': 'val2'}
        cf.insert(key, columns)

        # An empty list for columns shouldn't delete anything
        cf.remove(key, columns=[])
        assert_equal(cf.get(key), columns)

        cf.remove(key, columns=['2'])
        del columns['2']
        assert_equal(cf.get(key), {'1': 'val1'})

        cf.remove(key)
        assert_raises(NotFoundException, cf.get, key)

    def test_remove_counter(self):
        key = 'test_remove_counter'
        counter_cf.add(key, 'col')
        result = counter_cf.get(key)
        assert_equal(result['col'], 1)

        counter_cf.remove_counter(key, 'col')
        assert_raises(NotFoundException, cf.get, key)

    def test_dict_class(self):
        key = 'TestColumnFamily.test_dict_class'
        cf.insert(key, {'1': 'val1'})
        assert isinstance(cf.get(key), TestDict)

    def test_xget(self):
        key = "test_xget_batching"
        cf.insert(key, dict((str(i), str(i)) for i in range(100, 300)))

        combos = [(100, 10),
                  (100, 1000),
                  (100, 199),
                  (100, 200),
                  (100, 201),
                  (100, 7),
                  (100, 2)]

        for count, bufsz in combos:
            res = list(cf.xget(key, column_count=count, buffer_size=bufsz))
            assert_equal(len(res), count)
            assert_equal(res, [(str(i), str(i)) for i in range(100, 200)])

        combos = [(10000, 2),
                  (10000, 7),
                  (10000, 199),
                  (10000, 200),
                  (10000, 201),
                  (10000, 10000)]

        for count, bufsz in combos:
            res = list(cf.xget(key, column_count=count, buffer_size=bufsz))
            assert_equal(len(res), 200)
            assert_equal(res, [(str(i), str(i)) for i in range(100, 300)])

        for bufsz in [2, 77, 199, 200, 201, 10000]:
            res = list(cf.xget(key, column_count=None, buffer_size=bufsz))
            assert_equal(len(res), 200)
            assert_equal(res, [(str(i), str(i)) for i in range(100, 300)])

    def test_xget_counter(self):
        key = 'test_xget_counter'
        counter_cf.insert(key, {'col1': 1})
        res = list(counter_cf.xget(key))
        assert_equal(res, [('col1', 1)])

        counter_cf.insert(key, {'col1': 1, 'col2': 1})
        res = list(counter_cf.xget(key))
        assert_equal(res, [('col1', 2), ('col2', 1)])

class TestSuperColumnFamily(unittest.TestCase):

    def tearDown(self):
        for key, columns in scf.get_range():
            scf.remove(key)

    def test_empty(self):
        key = 'TestSuperColumnFamily.test_empty'
        assert_raises(NotFoundException, cf.get, key)
        assert_equal(len(cf.multiget([key])), 0)
        for key, columns in cf.get_range():
            assert_equal(len(columns), 0)

    def test_get_whole_row(self):
        key = 'TestSuperColumnFamily.test_get_whole_row'
        columns = {'1': {'sub1': 'val1', 'sub2': 'val2'}, '2': {'sub3': 'val3', 'sub4': 'val4'}}
        scf.insert(key, columns)
        assert_equal(scf.get(key), columns)

    def test_get_super_column(self):
        key = 'TestSuperColumnFamily.test_get_super_column'
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key, columns)
        assert_equal(scf.get(key), columns)
        assert_equal(scf.get(key, super_column='1'), subcolumns)
        assert_equal(scf.get(key, super_column='1', columns=['sub1']),     {'sub1': 'val1'})
        assert_equal(scf.get(key, super_column='1', column_start='sub3'),  {'sub3': 'val3'})
        assert_equal(scf.get(key, super_column='1', column_finish='sub1'), {'sub1': 'val1'})
        assert_equal(scf.get(key, super_column='1', column_count=1),       {'sub1': 'val1'})
        assert_equal(scf.get(key, super_column='1', column_count=1, column_reversed=True), {'sub3': 'val3'})

    def test_get_super_columns(self):
        key = 'TestSuperColumnFamily.test_get_super_columns'
        super1 = {'sub1': 'val1', 'sub2': 'val2'}
        super2 = {'sub3': 'val3', 'sub4': 'val4'}
        super3 = {'sub5': 'val5', 'sub6': 'val6'}
        columns = {'1': super1, '2': super2, '3': super3}
        scf.insert(key, columns)
        assert_equal(scf.get(key), columns)
        assert_equal(scf.get(key, columns=['1']),     {'1': super1})
        assert_equal(scf.get(key, column_start='3'),  {'3': super3})
        assert_equal(scf.get(key, column_finish='1'), {'1': super1})
        assert_equal(scf.get(key, column_count=1),    {'1': super1})
        assert_equal(scf.get(key, column_count=1, column_reversed=True), {'3': super3})

    def test_multiget_supercolumn(self):
        key1 = 'TestSuerColumnFamily.test_multiget_supercolumn1'
        key2 = 'TestSuerColumnFamily.test_multiget_supercolumn2'
        keys = [key1, key2]
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key1, columns)
        scf.insert(key2, columns)

        assert_equal(scf.multiget(keys),
                     {key1: columns, key2: columns})

        assert_equal(scf.multiget(keys, super_column='1'),
                     {key1: subcolumns, key2: subcolumns})

        assert_equal(scf.multiget(keys, super_column='1', columns=['sub1']),
                     {key1: {'sub1': 'val1'}, key2: {'sub1': 'val1'}})

        assert_equal(scf.multiget(keys, super_column='1', column_start='sub3'),
                     {key1: {'sub3': 'val3'}, key2: {'sub3': 'val3'}})

        assert_equal(scf.multiget(keys, super_column='1', column_finish='sub1'),
                     {key1: {'sub1': 'val1'}, key2: {'sub1': 'val1'}})

        assert_equal(scf.multiget(keys, super_column='1', column_count=1),
                     {key1: {'sub1': 'val1'}, key2: {'sub1': 'val1'}})

        assert_equal(scf.multiget(keys, super_column='1', column_count=1, column_reversed=True),
                     {key1: {'sub3': 'val3'}, key2: {'sub3': 'val3'}})

    def test_multiget_supercolumns(self):
        key1 = 'TestSuerColumnFamily.test_multiget_supercolumns1'
        key2 = 'TestSuerColumnFamily.test_multiget_supercolumns2'
        keys = [key1, key2]
        super1 = {'sub1': 'val1', 'sub2': 'val2'}
        super2 = {'sub3': 'val3', 'sub4': 'val4'}
        super3 = {'sub5': 'val5', 'sub6': 'val6'}
        columns = {'1': super1, '2': super2, '3': super3}
        scf.insert(key1, columns)
        scf.insert(key2, columns)
        assert_equal(scf.multiget(keys), {key1: columns, key2: columns})
        assert_equal(scf.multiget(keys, columns=['1']),     {key1: {'1': super1}, key2: {'1': super1}})
        assert_equal(scf.multiget(keys, column_start='3'),  {key1: {'3': super3}, key2: {'3': super3}})
        assert_equal(scf.multiget(keys, column_finish='1'), {key1: {'1': super1}, key2: {'1': super1}})
        assert_equal(scf.multiget(keys, column_count=1),    {key1: {'1': super1}, key2: {'1': super1}})
        assert_equal(scf.multiget(keys, column_count=1, column_reversed=True), {key1: {'3': super3}, key2: {'3': super3}})

    def test_get_range_super_column(self):
        key = 'TestSuperColumnFamily.test_get_range_super_column'
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key, columns)
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1')),
                     [(key, subcolumns)])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', columns=['sub1'])),
                     [(key, {'sub1': 'val1'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_start='sub3')),
                     [(key, {'sub3': 'val3'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_finish='sub1')),
                     [(key, {'sub1': 'val1'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_count=1)),
                     [(key, {'sub1': 'val1'})])
        assert_equal(list(scf.get_range(start=key, finish=key, super_column='1', column_count=1, column_reversed=True)),
                     [(key, {'sub3': 'val3'})])

    def test_get_range_super_columns(self):
        key = 'TestSuperColumnFamily.test_get_range_super_columns'
        super1 = {'sub1': 'val1', 'sub2': 'val2'}
        super2 = {'sub3': 'val3', 'sub4': 'val4'}
        super3 = {'sub5': 'val5', 'sub6': 'val6'}
        columns = {'1': super1, '2': super2, '3': super3}
        scf.insert(key, columns)
        assert_equal(list(scf.get_range(start=key, finish=key, columns=['1'])),
                     [(key, {'1': super1})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_start='3')),
                     [(key, {'3': super3})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_finish='1')),
                     [(key, {'1': super1})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_count=1)),
                     [(key, {'1': super1})])
        assert_equal(list(scf.get_range(start=key, finish=key, column_count=1, column_reversed=True)),
                     [(key, {'3': super3})])

    def test_get_count_super_column(self):
        key = 'TestSuperColumnFamily.test_get_count_super_column'
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key, columns)
        assert_equal(scf.get_count(key, super_column='1'),                       3)
        assert_equal(scf.get_count(key, super_column='1', columns=['sub1']),     1)
        assert_equal(scf.get_count(key, super_column='1', column_start='sub3'),  1)
        assert_equal(scf.get_count(key, super_column='1', column_finish='sub1'), 1)

    def test_get_count_super_columns(self):
        key = 'TestSuperColumnFamily.test_get_count_super_columns'
        columns = {'1': {'sub1': 'val1'}, '2': {'sub2': 'val2'}, '3': {'sub3': 'val3'}}
        scf.insert(key, columns)
        assert_equal(scf.get_count(key),                    3)
        assert_equal(scf.get_count(key, columns=['1']),     1)
        assert_equal(scf.get_count(key, column_start='3'),  1)
        assert_equal(scf.get_count(key, column_finish='1'), 1)

    def test_multiget_count_super_column(self):
        key1 = 'TestSuperColumnFamily.test_multiget_count_super_column1'
        key2 = 'TestSuperColumnFamily.test_multiget_count_super_column2'
        keys = [key1, key2]
        subcolumns = {'sub1': 'val1', 'sub2': 'val2', 'sub3': 'val3'}
        columns = {'1': subcolumns}
        scf.insert(key1, columns)
        scf.insert(key2, columns)
        assert_equal(scf.multiget_count(keys, super_column='1'),                       {key1: 3, key2: 3})
        assert_equal(scf.multiget_count(keys, super_column='1', columns=['sub1']),     {key1: 1, key2: 1})
        assert_equal(scf.multiget_count(keys, super_column='1', column_start='sub3'),  {key1: 1, key2: 1})
        assert_equal(scf.multiget_count(keys, super_column='1', column_finish='sub1'), {key1: 1, key2: 1})

    def test_multiget_count_super_columns(self):
        key1 = 'TestSuperColumnFamily.test_multiget_count_super_columns1'
        key2 = 'TestSuperColumnFamily.test_multiget_count_super_columns2'
        keys = [key1, key2]
        columns = {'1': {'sub1': 'val1'}, '2': {'sub2': 'val2'}, '3': {'sub3': 'val3'}}
        scf.insert(key1, columns)
        scf.insert(key2, columns)
        assert_equal(scf.multiget_count(keys),                    {key1: 3, key2: 3})
        assert_equal(scf.multiget_count(keys, columns=['1']),     {key1: 1, key2: 1})
        assert_equal(scf.multiget_count(keys, column_start='3'),  {key1: 1, key2: 1})
        assert_equal(scf.multiget_count(keys, column_finish='1'), {key1: 1, key2: 1})

    def test_batch_insert(self):
        key1 = 'TestSuperColumnFamily.test_batch_insert1'
        key2 = 'TestSuperColumnFamily.test_batch_insert2'
        columns = {'1': {'sub1': 'val1'}, '2': {'sub2': 'val2', 'sub3': 'val3'}}
        scf.batch_insert({key1: columns, key2: columns})
        assert_equal(scf.get(key1), columns)
        assert_equal(scf.get(key2), columns)

    def test_add(self):
        counter_scf.add('key', 'col', super_column='scol')
        result = counter_scf.get('key', super_column='scol')
        assert_equal(result['col'], 1)

        counter_scf.add('key', 'col', super_column='scol')
        result = counter_scf.get('key', super_column='scol')
        assert_equal(result['col'], 2)

        counter_scf.add('key', 'col2', super_column='scol')
        result = counter_scf.get('key', super_column='scol')
        assert_equal(result, {'col': 2, 'col2': 1})

    def test_remove(self):
        key = 'TestSuperColumnFamily.test_remove'
        columns = {'1': {'sub1': 'val1'}, '2': {'sub2': 'val2', 'sub3': 'val3'}, '3': {'sub4': 'val4'}}
        scf.insert(key, columns)
        assert_equal(scf.get_count(key), 3)
        scf.remove(key, super_column='1')
        assert_equal(scf.get_count(key), 2)
        scf.remove(key, columns=['3'])
        assert_equal(scf.get_count(key), 1)

        assert_equal(scf.get_count(key, super_column='2'), 2)
        scf.remove(key, super_column='2', columns=['sub2'])
        assert_equal(scf.get_count(key, super_column='2'), 1)

    def test_remove_counter(self):
        key = 'test_remove_counter'
        counter_scf.add(key, 'col', super_column='scol')
        result = counter_scf.get(key, super_column='scol')
        assert_equal(result['col'], 1)

        counter_scf.remove_counter(key, 'col', super_column='scol')
        assert_raises(NotFoundException, scf.get, key)

    def test_xget_counter(self):
        key = 'test_xget_counter'
        counter_scf.insert(key, {'scol': {'col1': 1}})
        res = list(counter_scf.xget(key))
        assert_equal(res, [('scol', {'col1': 1})])

        counter_scf.insert(key, {'scol': {'col1': 1, 'col2': 1}})
        res = list(counter_scf.xget(key))
        assert_equal(res, [('scol', {'col1': 2, 'col2': 1})])
