from datetime import datetime

from pycassa import connect, connect_thread_local, gm_timestamp, ColumnFamily, \
    ColumnFamilyMap, ConsistencyLevel, NotFoundException, String, Int64, \
    Float64, DateTime, IntString, FloatString, DateTimeString
from nose.tools import assert_raises

class TestUTF8(object):
    strcol = String(default='default')
    intcol = Int64(default=0)
    floatcol = Float64(default=0.0)
    datetimecol = DateTime(default=None)
    intstrcol = IntString()
    floatstrcol = FloatString()
    datetimestrcol = DateTimeString()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return self.__dict__ != other.__dict__

class TestEmpty(object):
    pass

class TestColumnFamilyMap:
    def setUp(self):
        self.client = connect()
        self.client.login('Keyspace1', {'username': 'jsmith', 'password': 'havebadpass'})
        self.cf = ColumnFamily(self.client, 'Standard2',
                               write_consistency_level=ConsistencyLevel.ONE,
                               timestamp=self.timestamp)
        self.map = ColumnFamilyMap(TestUTF8, self.cf)
        self.empty_map = ColumnFamilyMap(TestEmpty, self.cf, raw_columns=True)
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

    def instance(self, key):
        instance = TestUTF8()
        instance.key = key
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)
        instance.intstrcol = 8
        instance.floatstrcol = 4.6
        instance.datetimestrcol = datetime.now().replace(microsecond=0)

        return instance

    def test_will_not_insert_none(self):
        for column in ('strcol', 'intcol', 'floatcol', 'datetimecol',
                       'intstrcol', 'floatstrcol', 'datetimestrcol'):
            instance = self.instance('TestColumnFamilyMap.test_will_not_insert_none')
            setattr(instance, column, None)
            assert_raises(TypeError, self.map.insert, instance)

    def test_empty(self):
        key = 'TestColumnFamilyMap.test_empty'
        assert_raises(NotFoundException, self.map.get, key)
        assert len(self.map.multiget([key])) == 0

    def test_insert_get(self):
        instance = self.instance('TestColumnFamilyMap.test_insert_get')
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        assert self.map.get(instance.key) == instance
        assert self.empty_map.get(instance.key).raw_columns['intstrcol'] == str(instance.intstrcol)

    def test_insert_multiget(self):
        instance1 = self.instance('TestColumnFamilyMap.test_insert_multiget1')
        instance2 = self.instance('TestColumnFamilyMap.test_insert_multiget2')
        missing_key = 'TestColumnFamilyMap.test_insert_multiget3'

        self.map.insert(instance1)
        self.map.insert(instance2)
        rows = self.map.multiget([instance1.key, instance2.key, missing_key])
        assert len(rows) == 2
        assert rows[instance1.key] == instance1
        assert rows[instance2.key] == instance2
        assert missing_key not in rows
        assert self.empty_map.multiget([instance1.key])[instance1.key].raw_columns['intstrcol'] == str(instance1.intstrcol)

    def test_insert_get_count(self):
        instance = self.instance('TestColumnFamilyMap.test_insert_get_count')
        self.map.insert(instance)
        assert self.map.get_count(instance.key) == 7

    def test_insert_get_range(self):
        instances = []
        for i in xrange(5):
            instance = self.instance('TestColumnFamilyMap.test_insert_get_range%s' % i)
            instances.append(instance)

        for instance in instances:
            self.map.insert(instance)

        rows = list(self.map.get_range(start=instances[0].key, finish=instances[-1].key))
        assert len(rows) == len(instances)
        assert rows == instances
        assert list(self.empty_map.get_range(start=instances[0].key, finish=instances[0].key))[0].raw_columns['intstrcol'] == str(instances[0].intstrcol)

    def test_remove(self):
        instance = self.instance('TestColumnFamilyMap.test_remove')

        self.map.insert(instance)
        self.map.remove(instance)
        assert_raises(NotFoundException, self.map.get, instance.key)

    def test_does_not_insert_extra_column(self):
        instance = self.instance('TestColumnFamilyMap.test_does_not_insert_extra_column')
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
        assert instance.intstrcol == TestUTF8.intstrcol.default
        assert instance.floatstrcol == TestUTF8.floatstrcol.default
        assert instance.datetimestrcol == TestUTF8.datetimestrcol.default

class TestSuperColumnFamilyMap:
    def setUp(self):
        self.client = connect_thread_local()
        self.client.login('Keyspace1', {'username': 'jsmith', 'password': 'havebadpass'})
        self.cf = ColumnFamily(self.client, 'Super2',
                               write_consistency_level=ConsistencyLevel.ONE,
                               timestamp=self.timestamp,
                               super=True)
        self.map = ColumnFamilyMap(TestUTF8, self.cf)
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

    def instance(self, key, super_column):
        instance = TestUTF8()
        instance.key = key
        instance.super_column = super_column
        instance.strcol = '1'
        instance.intcol = 2
        instance.floatcol = 3.5
        instance.datetimecol = datetime.now().replace(microsecond=0)
        instance.intstrcol = 8
        instance.floatstrcol = 4.6
        instance.datetimestrcol = datetime.now().replace(microsecond=0)

        return instance

    def test_super(self):
        instance = self.instance('TestSuperColumnFamilyMap.test_super', 'super1')
        assert_raises(NotFoundException, self.map.get, instance.key)
        self.map.insert(instance)
        assert self.map.get(instance.key)[instance.super_column] == instance
        assert self.map.multiget([instance.key])[instance.key][instance.super_column] == instance
        assert list(self.map.get_range(start=instance.key, finish=instance.key)) == [{instance.super_column: instance}]
