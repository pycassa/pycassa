from pycasso import connect, gm_timestamp, ColumnFamily, ColumnFamilyMap, \
    ConsistencyLevel, NotFoundException
from nose.tools import assert_raises

class TestUTF8(object):
    pass

class TestColumnFamilyMap:
    def setUp(self):
        self.client = connect()
        self.cf = ColumnFamily(self.client, 'Test Keyspace', 'Test UTF8',
                               write_consistency_level=ConsistencyLevel.ONE,
                               timestamp=self.timestamp)
        self.map = ColumnFamilyMap(TestUTF8, self.cf, columns=['col1', 'col2'])
        self.map_nocol = ColumnFamilyMap(TestUTF8, self.cf)
        self.timestamp_n = 0

    # Since the timestamp passed to Cassandra will be in the same second
    # with the default timestamp function, causing problems with removing
    # and inserting (Cassandra doesn't know which is later), we supply our own
    def timestamp(self):
        ts = gm_timestamp() + self.timestamp_n
        self.timestamp_n += 1
        return ts

    def test_insert_get_remove(self):
        sample_data = {'col1': '1', 'col2': '2'}
        sample_data2 = sample_data.copy()
        sample_data2['other'] = '3'

        t1 = TestUTF8()
        t1.key = 'map1'
        t1.__dict__.update(sample_data2)
        self.cf.remove('map1')
        self.cf.remove('map2')
        self.cf.remove('map3')
        self.map.insert(t1)
        self.cf.get(t1.key) == sample_data
        self.map_nocol.insert(t1)
        self.cf.get(t1.key) == sample_data2

        t2 = self.map.get(t1.key)
        assert (t2.key, t2.col1, t2.col2) == (t1.key, t1.col1, t1.col2)
        assert getattr(t2, 'other', None) is None

        t3 = self.map_nocol.get(t1.key)
        assert (t3.key, t3.col1, t3.col2, t3.other) == (t1.key, t1.col1, t1.col2, t1.other)

        t2.key = 'map2'
        self.map.insert(t2)

        d = self.map.multiget([t1.key, t2.key])
        t4 = d[t1.key]
        assert (t4.key, t4.col1, t4.col2) == (t1.key, t1.col1, t1.col2)
        t5 = d[t2.key]
        assert (t5.key, t5.col1, t5.col2) == (t2.key, t2.col1, t2.col2)

        self.cf.insert('map3', {'col1': '1'})
        assert_raises(NotFoundException, self.map.get, 'map3')
        assert self.map.get_count('map3') == 1
        assert self.map.multiget(['map3']) == {}
        map_range = self.map.get_range()
        assert len(map_range) == 2
        r1, r2 = map_range[0], map_range[1]
        t1dict = t1.__dict__
        t1dict.pop('other')
        assert r1.__dict__ == t1dict
        assert r2.__dict__ == t2.__dict__

        self.map.remove(t1)
        assert_raises(NotFoundException, self.cf.get, t1.key)
