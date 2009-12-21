from pycasso import connect, gm_timestamp, ColumnFamily, ConsistencyLevel, NotFoundException

from nose.tools import assert_raises

class TestColumnFamily:
    def setUp(self):
        self.client = connect()
        self.cf = ColumnFamily(self.client, 'Test Keyspace', 'Test UTF8',
                               write_consistency_level=ConsistencyLevel.ONE,
                               buffer_size=2, timestamp=self.timestamp)
        self.timestamp_n = 0

    # Since the timestamp passed to Cassandra will be in the same second
    # with the default timestamp function, causing problems with removing
    # and inserting (Cassandra doesn't know which is later), we supply our own
    def timestamp(self):
        ts = gm_timestamp() + self.timestamp_n
        self.timestamp_n += 1
        return ts

    def test_insert_get_remove(self):
        d1 = {'1': 'val1'}
        d2 = {'2': 'val2', '3': 'val3'}
        d3 = d1.copy()
        d3.update(d2)
        key1 = 'bar'
        key2 = 'foo'
        self.cf.remove(key1) # Clear out any remnants of the keys
        self.cf.remove(key2)
        assert_raises(NotFoundException, self.cf.get, key1)
        # Test client.insert()
        self.cf.insert(key1, d1)
        # Test client.batch_insert()
        self.cf.insert(key1, d2)
        assert self.cf.get(key1) == d3
        assert self.cf.get(key1, columns=d1.keys()) == d1
        assert self.cf.get_count(key1) == 3
        assert self.cf.multiget([key1]) == {key1: d3}

        self.cf.insert(key2, d1)
        assert self.cf.multiget([key1, key2]) == {key1: d3, key2: d1}
        assert self.cf.get_range(start='bar', finish='foo') == [(key1, d3), (key2, d1)]

    def test_get_range(self):
        keys = ['range{0}'.format(i) for i in xrange(5)]
        for key in keys:
            self.cf.insert(key, {'col1': 'val1'})
        kv_range = self.cf.get_range(start='range0', finish='range4')
        assert len(kv_range) == 5
        for i, kv in enumerate(kv_range):
            assert kv[0] == 'range{0}'.format(i)

        assert self.cf.get_range(start='range0', row_count=1)[0][0] == 'range0'
