from pycasso import connect, gm_timestamp, ColumnFamily, ColumnFamilyMap, \
    ConsistencyLevel, NotFoundException
from nose.tools import assert_raises

class TestUTF8(object):
    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return self.__dict__ != other.__dict__

class TestColumnFamilyMap:
    def setUp(self):
        self.client = connect()
        self.cf = ColumnFamily(self.client, 'Test Keyspace', 'Test UTF8',
                               write_consistency_level=ConsistencyLevel.ONE,
                               timestamp=self.timestamp)
        self.map = ColumnFamilyMap(TestUTF8, self.cf,
                       columns={'col1': 'default', 'col2': 'default'})
        self.map_no_columns = ColumnFamilyMap(TestUTF8, self.cf)
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
        for map in (self.map, self.map_no_columns):
            assert_raises(NotFoundException, map.get, key)
            assert len(map.multiget([key])) == 0
            assert len(list(map.get_range())) == 0

    def test_insert_get(self):
        instance = TestUTF8()
        instance.key = 'key1'
        instance.col1 = '1'
        instance.col2 = '2'
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        assert self.map.get(instance.key) == instance

    def test_insert_multiget(self):
        instance1 = TestUTF8()
        instance1.key = 'key1'
        instance1.col1 = '1'
        instance1.col2 = '2'

        instance2 = TestUTF8()
        instance2.key = 'key2'
        instance2.col1 = '3'
        instance2.col2 = '4'

        missing_key = 'key3'

        self.map.insert(instance1)
        self.map.insert(instance2)
        rows = self.map.multiget([instance1.key, instance2.key, missing_key])
        assert len(rows) == 2
        assert rows[instance1.key] == instance1
        assert rows[instance2.key] == instance2
        assert missing_key not in rows

    def test_insert_get_count(self):
        instance = TestUTF8()
        instance.key = 'key1'
        instance.col1 = '1'
        instance.col2 = '2'
        self.map.insert(instance)
        assert self.map.get_count(instance.key) == 2

    def test_insert_get_range(self):
        instances = []
        for i in xrange(5):
            instance = TestUTF8()
            instance.key = 'range%s' % i
            instance.col1 = str(i)
            instance.col2 = str(i+1)
            instances.append(instance)

        for instance in instances:
            self.map.insert(instance)

        rows = list(self.map.get_range(start=instances[0].key, finish=instances[-1].key))
        assert len(rows) == len(instances)
        assert rows == instances

        all_rows = list(self.map.get_range())
        assert all_rows == rows

    def test_remove(self):
        instance = TestUTF8()
        instance.key = 'key1'
        instance.col1 = '1'
        instance.col2 = '2'

        self.map.insert(instance)
        self.map.remove(instance)
        assert_raises(NotFoundException, self.map.get, instance.key)

    def test_overlapping_default(self):
        instance = TestUTF8()
        instance.key = 'default1'
        instance.col2 = '2'
        instance.col3 = '3'

        self.map_no_columns.insert(instance)
        assert self.map_no_columns.get(instance.key) == instance

        get_instance = self.map.get(instance.key)
        assert get_instance.col1 == 'default'
        assert get_instance.col2 == instance.col2
        assert_raises(AttributeError, getattr, get_instance, 'col3')

    def test_disjoint_default(self):
        instance = TestUTF8()
        instance.key = 'default1'
        instance.col3 = '3'
        instance.col4 = '4'

        self.map_no_columns.insert(instance)
        # This is perhaps very strange behavior, but I don't see any way of fixing it
        assert_raises(NotFoundException, self.map.get, instance.key)
