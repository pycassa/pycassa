from datetime import datetime

from pycasso import connect, gm_timestamp, ColumnFamily, ColumnFamilyMap, \
    ConsistencyLevel, NotFoundException, StringColumn, Int64Column, \
    Float64Column, DateTimeColumn
from nose.tools import assert_raises

class TestUTF8(object):
    strcol = StringColumn(default='default')
    intcol = Int64Column(default=0)
    floatcol = Float64Column(default=0.0)
    datetimecol = DateTimeColumn(default=None)

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
        self.map = ColumnFamilyMap(TestUTF8, self.cf)
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
        key = 'TestColumnFamilyMap.test_empty'
        assert_raises(NotFoundException, self.map.get, key)
        assert len(self.map.multiget([key])) == 0

    def test_insert_get(self):
        instance = TestUTF8()
        instance.key = 'TestColumnFamilyMap.test_insert_get'
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        assert self.map.get(instance.key) == instance

    def test_insert_multiget(self):
        instance1 = TestUTF8()
        instance1.key = 'TestColumnFamilyMap.test_insert_multiget1'
        instance1.strcol = '1'
        instance1.intcol = 2
        instance1.floatcol = 3.5
        instance1.datetimecol = datetime.now().replace(microsecond=0)

        instance2 = TestUTF8()
        instance2.key = 'TestColumnFamilyMap.test_insert_multiget2'
        instance2.strcol = '1'
        instance2.intcol = 2
        instance2.floatcol = 3.5
        instance2.datetimecol = datetime.now().replace(microsecond=0)

        missing_key = 'TestColumnFamilyMap.test_insert_multiget3'

        self.map.insert(instance1)
        self.map.insert(instance2)
        rows = self.map.multiget([instance1.key, instance2.key, missing_key])
        assert len(rows) == 2
        assert rows[instance1.key] == instance1
        assert rows[instance2.key] == instance2
        assert missing_key not in rows

    def test_insert_get_count(self):
        instance = TestUTF8()
        instance.key = 'TestColumnFamilyMap.test_insert_get_count'
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)
        self.map.insert(instance)
        assert self.map.get_count(instance.key) == 4

    def test_insert_get_range(self):
        instances = []
        for i in xrange(5):
            instance = TestUTF8()
            instance.key = 'TestColumnFamilyMap.test_insert_get_range%s' % i
            instance.strcol = '1'
            instance.intcol = 2
            instance.floatcol = 3.5
            instance.datetimecol = datetime.now().replace(microsecond=0)
            instances.append(instance)

        for instance in instances:
            self.map.insert(instance)

        rows = list(self.map.get_range(start=instances[0].key, finish=instances[-1].key))
        assert len(rows) == len(instances)
        assert rows == instances

    def test_remove(self):
        instance = TestUTF8()
        instance.key = 'TestColumnFamilyMap.test_remove'
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)

        self.map.insert(instance)
        self.map.remove(instance)
        assert_raises(NotFoundException, self.map.get, instance.key)

    def test_does_not_insert_extra_column(self):
        instance = TestUTF8()
        instance.key = 'TestColumnFamilyMap.test_does_not_insert_extra_column'
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)
        instance.othercol = 'Test'

        self.map.insert(instance)

        get_instance = self.map.get(instance.key)
        assert get_instance.strcol == instance.strcol
        assert get_instance.intcol == instance.intcol
        assert get_instance.floatcol == instance.floatcol
        assert get_instance.datetimecol == instance.datetimecol
        assert_raises(AttributeError, getattr, get_instance, 'othercol')

    def test_has_defaults(self):
        key = 'TestColumnFamilyMap.test_has_defaults'
        self.cf.insert(key, {'strcol': '1'})
        instance = self.map.get(key)

        assert instance.intcol == TestUTF8.intcol.default
        assert instance.floatcol == TestUTF8.floatcol.default
        assert instance.datetimecol == TestUTF8.datetimecol.default
