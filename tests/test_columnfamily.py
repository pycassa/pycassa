from pycasso import connect, ColumnFamily, ConsistencyLevel, NotFoundException

from nose.tools import assert_raises

class TestDict(dict):
    pass

class TestColumnFamily:
    def setUp(self):
        self.client = connect()
        self.cf = ColumnFamily(self.client, 'Test Keyspace', 'Test UTF8',
                               write_consistency_level=ConsistencyLevel.ONE,
                               buffer_size=2, timestamp=self.timestamp,
                               dict_class=TestDict)
        try:
            self.timestamp_n = int(self.cf.get('meta')['timestamp'])
        except NotFoundException:
            self.timestamp_n = 0
        self.clear()

    def tearDown(self):
        self.cf.insert('meta', {'timestamp': str(self.timestamp_n)})

    # Since the timestamp passed to Cassandra will be in the same second
    # with the default timestamp function, causing problems with removing
    # and inserting (Cassandra doesn't know which is later), we supply our own
    def timestamp(self):
        self.timestamp_n += 1
        return self.timestamp_n

    def clear(self):
        for key, columns in self.cf.get_range(include_timestamp=True):
            for value, timestamp in columns.itervalues():
                self.timestamp_n = max(self.timestamp_n, timestamp)
            self.cf.remove(key)

    def test_empty(self):
        key = 'random'
        assert_raises(NotFoundException, self.cf.get, key)
        assert len(self.cf.multiget([key])) == 0
        assert len(list(self.cf.get_range())) == 0

    def test_insert_get(self):
        key = 'key1'
        columns = {'1': 'val1', '2': 'val2'}
        assert_raises(NotFoundException, self.cf.get, key)
        self.cf.insert(key, columns)
        assert self.cf.get(key) == columns

    def test_insert_multiget(self):
        key1 = 'key1'
        columns1 = {'1': 'val1', '2': 'val2'}
        key2 = 'key2'
        columns2 = {'3': 'val1', '4': 'val2'}
        missing_key = 'key3'

        self.cf.insert(key1, columns1)
        self.cf.insert(key2, columns2)
        rows = self.cf.multiget([key1, key2, missing_key])
        assert len(rows) == 2
        assert rows[key1] == columns1
        assert rows[key2] == columns2
        assert missing_key not in rows

    def test_insert_get_count(self):
        key = 'count'
        columns = {'1': 'val1', '2': 'val2'}
        self.cf.insert(key, columns)
        assert self.cf.get_count(key) == 2

    def test_insert_get_range(self):
        keys = ['range{0}'.format(i) for i in xrange(5)]
        columns = {'1': 'val1', '2': 'val2'}
        for key in keys:
            self.cf.insert(key, columns)

        rows = list(self.cf.get_range(start=keys[0], finish=keys[-1]))
        assert len(rows) == len(keys)
        for i, (k, c) in enumerate(rows):
            assert k == keys[i]
            assert c == columns

        all_rows = list(self.cf.get_range())
        assert all_rows == rows

    def test_remove(self):
        key = 'key1'
        columns = {'1': 'val1', '2': 'val2'}
        self.cf.insert(key, columns)

        self.cf.remove(key, '2')
        del columns['2']
        assert self.cf.get(key) == {'1': 'val1'}

        self.cf.remove(key)
        assert_raises(NotFoundException, self.cf.get, key)

    def test_dict_class(self):
        self.cf.insert('key1', {'1': 'val1'})
        assert isinstance(self.cf.get('key1'), TestDict)

class TestSuperColumnFamily:
    def setUp(self):
        self.client = connect()
        self.cf = ColumnFamily(self.client, 'Test Keyspace', 'Test Super',
                               write_consistency_level=ConsistencyLevel.ONE,
                               buffer_size=2, timestamp=self.timestamp,
                               super=True)

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
        key = 'key1'
        columns = {'1': {'sub1': 'val1', 'sub2': 'val2'}, '2': {'sub3': 'val3', 'sub4': 'val4'}}
        assert_raises(NotFoundException, self.cf.get, key)
        self.cf.insert(key, columns)
        assert self.cf.get(key) == columns
        assert self.cf.multiget([key]) == {key: columns}
        assert list(self.cf.get_range()) == [(key, columns)]

    def test_super_column_argument(self):
        key = 'key1'
        sub12 = {'sub1': 'val1', 'sub2': 'val2'}
        sub34 = {'sub3': 'val3', 'sub4': 'val4'}
        columns = {'1': sub12, '2': sub34}
        self.cf.insert(key, columns)
        assert self.cf.get(key, super_column='1') == sub12
        assert_raises(NotFoundException, self.cf.get, key, super_column='3')
        assert self.cf.multiget([key], super_column='1') == {key: sub12}
        assert list(self.cf.get_range(super_column='1')) == [(key, {'1': sub12})]
