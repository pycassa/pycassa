import uuid

from pycassa import QueuePool,\
                    PooledColumnFamily, NotFoundException
from pycassa.util import *

import unittest
from nose.tools import assert_raises, assert_equal, assert_almost_equal

from datetime import datetime
from uuid import uuid1
import time

TIME1 = uuid.UUID(hex='ddc6118e-a003-11df-8abf-00234d21610a')
TIME2 = uuid.UUID(hex='40ad6d4c-a004-11df-8abf-00234d21610a')
TIME3 = uuid.UUID(hex='dc3d5234-a00b-11df-8abf-00234d21610a')

VALS = ['val1', 'val2', 'val3']
KEYS = ['key1', 'key2', 'key3']

class TestStandardCFs(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = QueuePool(pool_size=10, keyspace='Keyspace1', credentials=credentials)

        self.cf       = PooledColumnFamily(self.pool, 'Standard2')

        self.cf_long  = PooledColumnFamily(self.pool, 'StdLong')
        self.cf_int   = PooledColumnFamily(self.pool, 'StdInteger')
        self.cf_time  = PooledColumnFamily(self.pool, 'StdTimeUUID')
        self.cf_lex   = PooledColumnFamily(self.pool, 'StdLexicalUUID')
        self.cf_ascii = PooledColumnFamily(self.pool, 'StdAscii')
        self.cf_utf8  = PooledColumnFamily(self.pool, 'StdUTF8')
        self.cf_bytes = PooledColumnFamily(self.pool, 'StdBytes')

        self.cfs = [self.cf_long, self.cf_int, self.cf_time, self.cf_lex,
                    self.cf_ascii, self.cf_utf8, self.cf_bytes]

    def tearDown(self):
        for cf in self.cfs:
            cf.truncate()
        self.pool.dispose()

    def make_group(self, cf, cols):
        diction = { cols[0]: VALS[0],
                    cols[1]: VALS[1],
                    cols[2]: VALS[2]}
        return { 'cf': cf, 'cols': cols, 'dict': diction}

    def test_standard_column_family(self):

        # For each data type, create a group that includes its column family,
        # a set of column names, and a dictionary that maps from the column
        # names to values.
        type_groups = []

        long_cols = [1111111111111111L,
                     2222222222222222L,
                     3333333333333333L]
        type_groups.append(self.make_group(self.cf_long, long_cols))

        int_cols = [1,2,3]
        type_groups.append(self.make_group(self.cf_int, int_cols))

        time_cols = [TIME1, TIME2, TIME3]
        type_groups.append(self.make_group(self.cf_time, time_cols))

        lex_cols = [uuid.UUID(bytes='aaa aaa aaa aaaa'),
                    uuid.UUID(bytes='bbb bbb bbb bbbb'),
                    uuid.UUID(bytes='ccc ccc ccc cccc')]
        type_groups.append(self.make_group(self.cf_lex, lex_cols))

        ascii_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_group(self.cf_ascii, ascii_cols))

        utf8_cols = [u'a\u0020', u'b\u0020', u'c\u0020']
        type_groups.append(self.make_group(self.cf_utf8, utf8_cols))

        bytes_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_group(self.cf_bytes, bytes_cols))

        # Begin the actual inserting and getting
        for group in type_groups:
            group.get('cf').insert(KEYS[0], group.get('dict'))

            assert group.get('cf').get(KEYS[0]) == group.get('dict')

            # Check each column individually
            for i in range(3):
                assert group.get('cf').get(KEYS[0], columns=[group.get('cols')[i]]) == \
                        {group.get('cols')[i]: VALS[i]}

            # Check that if we list all columns, we get the full dict
            assert group.get('cf').get(KEYS[0], columns=group.get('cols')[:]) == group.get('dict')
            # The same thing with a start and end instead
            assert group.get('cf').get(KEYS[0], column_start=group.get('cols')[0], column_finish=group.get('cols')[2]) == group.get('dict')
            # A start and end that are the same
            assert group.get('cf').get(KEYS[0], column_start=group.get('cols')[0], column_finish=group.get('cols')[0]) == \
                    {group.get('cols')[0]: VALS[0]}

            assert group.get('cf').get_count(KEYS[0]) == 3

            # Test removing rows
            group.get('cf').remove(KEYS[0], columns=group.get('cols')[:1])
            assert group.get('cf').get_count(KEYS[0]) == 2

            group.get('cf').remove(KEYS[0], columns=group.get('cols')[1:])
            assert group.get('cf').get_count(KEYS[0]) == 0

            # Insert more than one row now
            group.get('cf').insert(KEYS[0], group.get('dict'))
            group.get('cf').insert(KEYS[1], group.get('dict'))
            group.get('cf').insert(KEYS[2], group.get('dict'))


            ### multiget() tests ###

            res = group.get('cf').multiget(KEYS[:])
            for i in range(3):
                assert res.get(KEYS[i]) == group.get('dict')

            res = group.get('cf').multiget(KEYS[2:])
            assert res.get(KEYS[2]) == group.get('dict')

            # Check each column individually
            for i in range(3):
                res = group.get('cf').multiget(KEYS[:], columns=[group.get('cols')[i]])
                for j in range(3):
                    assert res.get(KEYS[j]) == {group.get('cols')[i]: VALS[i]}

            # Check that if we list all columns, we get the full dict
            res = group.get('cf').multiget(KEYS[:], columns=group.get('cols')[:])
            for j in range(3):
                assert res.get(KEYS[j]) == group.get('dict')

            # The same thing with a start and end instead
            res = group.get('cf').multiget(KEYS[:], column_start=group.get('cols')[0], column_finish=group.get('cols')[2])
            for j in range(3):
                assert res.get(KEYS[j]) == group.get('dict')

            # A start and end that are the same
            res = group.get('cf').multiget(KEYS[:], column_start=group.get('cols')[0], column_finish=group.get('cols')[0])
            for j in range(3):
                assert res.get(KEYS[j]) == {group.get('cols')[0]: VALS[0]}


            ### get_range() tests ###

            res = group.get('cf').get_range(start=KEYS[0])
            for sub_res in res:
                assert sub_res[1] == group.get('dict')

            res = group.get('cf').get_range(start=KEYS[0], column_start=group.get('cols')[0], column_finish=group.get('cols')[2])
            for sub_res in res:
                assert sub_res[1] == group.get('dict')

            res = group.get('cf').get_range(start=KEYS[0], columns=group.get('cols')[:])
            for sub_res in res:
                assert sub_res[1] == group.get('dict')


class TestSuperCFs(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = QueuePool(pool_size=10, keyspace='Keyspace1', credentials=credentials)
        self.cf_suplong  = PooledColumnFamily(self.pool, 'SuperLong')
        self.cf_supint   = PooledColumnFamily(self.pool, 'SuperInt')
        self.cf_suptime  = PooledColumnFamily(self.pool, 'SuperTime')
        self.cf_suplex   = PooledColumnFamily(self.pool, 'SuperLex')
        self.cf_supascii = PooledColumnFamily(self.pool, 'SuperAscii')
        self.cf_suputf8  = PooledColumnFamily(self.pool, 'SuperUTF8')
        self.cf_supbytes = PooledColumnFamily(self.pool, 'SuperBytes')

        self.cfs = [self.cf_suplong, self.cf_supint, self.cf_suptime,
                    self.cf_suplex, self.cf_supascii, self.cf_suputf8,
                    self.cf_supbytes]

    def tearDown(self):
        for cf in self.cfs:
            cf.truncate()
        self.pool.dispose()

    def make_super_group(self, cf, cols):
        diction = { cols[0]: {'bytes': VALS[0]},
                    cols[1]: {'bytes': VALS[1]},
                    cols[2]: {'bytes': VALS[2]}}
        return { 'cf': cf, 'cols': cols, 'dict': diction}

    def test_super_column_families(self):

        # For each data type, create a group that includes its column family,
        # a set of column names, and a dictionary that maps from the column
        # names to values.
        type_groups = []

        long_cols = [1111111111111111L,
                     2222222222222222L,
                     3333333333333333L]
        type_groups.append(self.make_super_group(self.cf_suplong, long_cols))

        int_cols = [1,2,3]
        type_groups.append(self.make_super_group(self.cf_supint, int_cols))

        time_cols = [TIME1, TIME2, TIME3]
        type_groups.append(self.make_super_group(self.cf_suptime, time_cols))

        lex_cols = [uuid.UUID(bytes='aaa aaa aaa aaaa'),
                    uuid.UUID(bytes='bbb bbb bbb bbbb'),
                    uuid.UUID(bytes='ccc ccc ccc cccc')]
        type_groups.append(self.make_super_group(self.cf_suplex, lex_cols))

        ascii_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_super_group(self.cf_supascii, ascii_cols))

        utf8_cols = [u'a\u0020', u'b\u0020', u'c\u0020']
        type_groups.append(self.make_super_group(self.cf_suputf8, utf8_cols))

        bytes_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_super_group(self.cf_supbytes, bytes_cols))

        # Begin the actual inserting and getting
        for group in type_groups:
            group.get('cf').insert(KEYS[0], group.get('dict'))

            assert group.get('cf').get(KEYS[0]) == group.get('dict')

            # Check each supercolumn individually
            for i in range(3):
                res = group.get('cf').get(KEYS[0], columns=[group.get('cols')[i]])
                assert res == {group.get('cols')[i]: {'bytes': VALS[i]}}

            # Check that if we list all columns, we get the full dict
            assert group.get('cf').get(KEYS[0], columns=group.get('cols')[:]) == group.get('dict')
            # The same thing with a start and end instead
            assert group.get('cf').get(KEYS[0], column_start=group.get('cols')[0], column_finish=group.get('cols')[2]) == group.get('dict')
            # A start and end that are the same
            assert group.get('cf').get(KEYS[0], column_start=group.get('cols')[0], column_finish=group.get('cols')[0]) == \
                    {group.get('cols')[0]: {'bytes': VALS[0]}}

            assert group.get('cf').get_count(KEYS[0]) == 3

            # Test removing rows
            group.get('cf').remove(KEYS[0], columns=group.get('cols')[:1])
            assert group.get('cf').get_count(KEYS[0]) == 2

            group.get('cf').remove(KEYS[0], columns=group.get('cols')[1:])
            assert group.get('cf').get_count(KEYS[0]) == 0

            # Insert more than one row now
            group.get('cf').insert(KEYS[0], group.get('dict'))
            group.get('cf').insert(KEYS[1], group.get('dict'))
            group.get('cf').insert(KEYS[2], group.get('dict'))


            ### multiget() tests ###

            res = group.get('cf').multiget(KEYS[:])
            for i in range(3):
                assert res.get(KEYS[i]) == group.get('dict')

            res = group.get('cf').multiget(KEYS[2:])
            assert res.get(KEYS[2]) == group.get('dict')

            # Check each column individually
            for i in range(3):
                res = group.get('cf').multiget(KEYS[:], columns=[group.get('cols')[i]])
                for j in range(3):
                    assert res.get(KEYS[j]) == {group.get('cols')[i]: {'bytes': VALS[i]}}

            # Check that if we list all columns, we get the full dict
            res = group.get('cf').multiget(KEYS[:], columns=group.get('cols')[:])
            for j in range(3):
                assert res.get(KEYS[j]) == group.get('dict')

            # The same thing with a start and end instead
            res = group.get('cf').multiget(KEYS[:], column_start=group.get('cols')[0], column_finish=group.get('cols')[2])
            for j in range(3):
                assert res.get(KEYS[j]) == group.get('dict')

            # A start and end that are the same
            res = group.get('cf').multiget(KEYS[:], column_start=group.get('cols')[0], column_finish=group.get('cols')[0])
            for j in range(3):
                assert res.get(KEYS[j]) == {group.get('cols')[0]: {'bytes': VALS[0]}}


            ### get_range() tests ###

            res = group.get('cf').get_range(start=KEYS[0])
            for sub_res in res:
                assert sub_res[1] == group.get('dict')

            res = group.get('cf').get_range(start=KEYS[0], column_start=group.get('cols')[0], column_finish=group.get('cols')[2])
            for sub_res in res:
                assert sub_res[1] == group.get('dict')

            res = group.get('cf').get_range(start=KEYS[0], columns=group.get('cols')[:])
            for sub_res in res:
                assert sub_res[1] == group.get('dict')


class TestSuperSubCFs(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = QueuePool(pool_size=10, keyspace='Keyspace1', credentials=credentials)
        self.cf_suplong_sublong  = PooledColumnFamily(self.pool, 'SuperLongSubLong')
        self.cf_suplong_subint   = PooledColumnFamily(self.pool, 'SuperLongSubInt')
        self.cf_suplong_subtime  = PooledColumnFamily(self.pool, 'SuperLongSubTime')
        self.cf_suplong_sublex   = PooledColumnFamily(self.pool, 'SuperLongSubLex')
        self.cf_suplong_subascii = PooledColumnFamily(self.pool, 'SuperLongSubAscii')
        self.cf_suplong_subutf8  = PooledColumnFamily(self.pool, 'SuperLongSubUTF8')
        self.cf_suplong_subbytes = PooledColumnFamily(self.pool, 'SuperLongSubBytes')

        self.cfs = [self.cf_suplong_subint, self.cf_suplong_subint,
                    self.cf_suplong_subtime, self.cf_suplong_sublex,
                    self.cf_suplong_subascii, self.cf_suplong_subutf8,
                    self.cf_suplong_subbytes]

    def tearDown(self):
        for cf in self.cfs:
            cf.truncate()
        self.pool.dispose()

    def make_sub_group(self, cf, cols):
        diction = {123L: {cols[0]: VALS[0],
                          cols[1]: VALS[1],
                          cols[2]: VALS[2]}}
        return { 'cf': cf, 'cols': cols, 'dict': diction}

    def test_super_column_family_subs(self):

        # For each data type, create a group that includes its column family,
        # a set of column names, and a dictionary that maps from the column
        # names to values.
        type_groups = []

        long_cols = [1111111111111111L,
                     2222222222222222L,
                     3333333333333333L]
        type_groups.append(self.make_sub_group(self.cf_suplong_sublong, long_cols))

        int_cols = [1,2,3]
        type_groups.append(self.make_sub_group(self.cf_suplong_subint, int_cols))

        time_cols = [TIME1, TIME2, TIME3]
        type_groups.append(self.make_sub_group(self.cf_suplong_subtime, time_cols))

        lex_cols = [uuid.UUID(bytes='aaa aaa aaa aaaa'),
                    uuid.UUID(bytes='bbb bbb bbb bbbb'),
                    uuid.UUID(bytes='ccc ccc ccc cccc')]
        type_groups.append(self.make_sub_group(self.cf_suplong_sublex, lex_cols))

        ascii_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_sub_group(self.cf_suplong_subascii, ascii_cols))

        utf8_cols = [u'a\u0020', u'b\u0020', u'c\u0020']
        type_groups.append(self.make_sub_group(self.cf_suplong_subutf8, utf8_cols))

        bytes_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_sub_group(self.cf_suplong_subbytes, bytes_cols))

        # Begin the actual inserting and getting
        for group in type_groups:
            group.get('cf').insert(KEYS[0], group.get('dict'))

            assert group.get('cf').get(KEYS[0]) == group.get('dict')
            assert group.get('cf').get(KEYS[0], columns=[123L]) == group.get('dict')

            # A start and end that are the same
            assert group.get('cf').get(KEYS[0], column_start=123L, column_finish=123L) == \
                    group.get('dict')

            assert group.get('cf').get_count(KEYS[0]) == 1

            # Test removing rows
            group.get('cf').remove(KEYS[0], super_column=123L)
            assert group.get('cf').get_count(KEYS[0]) == 0

            # Insert more than one row now
            group.get('cf').insert(KEYS[0], group.get('dict'))
            group.get('cf').insert(KEYS[1], group.get('dict'))
            group.get('cf').insert(KEYS[2], group.get('dict'))


            ### multiget() tests ###

            res = group.get('cf').multiget(KEYS[:])
            for i in range(3):
                assert res.get(KEYS[i]) == group.get('dict')

            res = group.get('cf').multiget(KEYS[2:])
            assert res.get(KEYS[2]) == group.get('dict')

            res = group.get('cf').multiget(KEYS[:], columns=[123L])
            for i in range(3):
                assert res.get(KEYS[i]) == group.get('dict')

            res = group.get('cf').multiget(KEYS[:], super_column=123L)
            for i in range(3):
                assert res.get(KEYS[i]) == group.get('dict').get(123L)

            res = group.get('cf').multiget(KEYS[:], column_start=123L, column_finish=123L)
            for j in range(3):
                assert res.get(KEYS[j]) == group.get('dict')

            ### get_range() tests ###

            res = group.get('cf').get_range(start=KEYS[0])
            for sub_res in res:
                assert_equal(sub_res[1], group.get('dict'))

            res = group.get('cf').get_range(start=KEYS[0], column_start=123L, column_finish=123L)
            for sub_res in res:
                assert_equal(sub_res[1], group.get('dict'))

            res = group.get('cf').get_range(start=KEYS[0], columns=[123L])
            for sub_res in res:
                assert_equal(sub_res[1], group.get('dict'))

            res = group.get('cf').get_range(start=KEYS[0], super_column=123L)
            for sub_res in res:
                assert_equal(sub_res[1], group.get('dict').get(123L))


class TestValidators(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = QueuePool(pool_size=10, keyspace='Keyspace1', credentials=credentials)
        self.cf_valid_long = PooledColumnFamily(self.pool, 'ValidatorLong')
        self.cf_valid_int = PooledColumnFamily(self.pool, 'ValidatorInt')
        self.cf_valid_time = PooledColumnFamily(self.pool, 'ValidatorTime')
        self.cf_valid_lex = PooledColumnFamily(self.pool, 'ValidatorLex')
        self.cf_valid_ascii = PooledColumnFamily(self.pool, 'ValidatorAscii')
        self.cf_valid_utf8 = PooledColumnFamily(self.pool, 'ValidatorUTF8')
        self.cf_valid_bytes = PooledColumnFamily(self.pool, 'ValidatorBytes')

        self.cfs = [self.cf_valid_long, self.cf_valid_int, self.cf_valid_time,
                    self.cf_valid_lex, self.cf_valid_ascii, self.cf_valid_utf8,
                    self.cf_valid_bytes]

    def tearDown(self):
        for cf in self.cfs:
            cf.truncate()
        self.pool.dispose()

    def test_validated_columns(self):

        key = 'key1'

        col = {'subcol':1L}
        self.cf_valid_long.insert(key, col)
        assert self.cf_valid_long.get(key) == col

        col = {'subcol':1}
        self.cf_valid_int.insert(key, col)
        assert self.cf_valid_int.get(key) == col

        col = {'subcol':TIME1}
        self.cf_valid_time.insert(key, col)
        assert self.cf_valid_time.get(key) == col

        col = {'subcol':uuid.UUID(bytes='aaa aaa aaa aaaa')}
        self.cf_valid_lex.insert(key, col)
        assert self.cf_valid_lex.get(key) == col

        col = {'subcol':'aaa'}
        self.cf_valid_ascii.insert(key, col)
        assert self.cf_valid_ascii.get(key) == col

        col = {'subcol':u'a\u0020'}
        self.cf_valid_utf8.insert(key, col)
        assert self.cf_valid_utf8.get(key) == col

        col = {'subcol':'aaa'}
        self.cf_valid_bytes.insert(key, col)
        assert self.cf_valid_bytes.get(key) == col

class TestDefaultValidators(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = QueuePool(pool_size=5, keyspace='Keyspace1', credentials=credentials)
        self.cf_def_valid = PooledColumnFamily(self.pool, 'DefaultValidator')

    def tearDown(self):
        self.cf_def_valid.truncate()
        self.pool.dispose()

    def test_default_validated_columns(self):

        key = 'key1'

        col_cf  = {'aaaaaa':1L}
        col_cm  = {'subcol':TIME1}
        col_ncf = {'aaaaaa':TIME1}
        col_ncm = {'subcol':1L}

        # Both of these inserts work, as cf allows
        #  longs and cm for 'subcol' allows TIMEUUIDs.
        self.cf_def_valid.insert(key, col_cf)
        self.cf_def_valid.insert(key, col_cm)
        assert self.cf_def_valid.get(key) == {'aaaaaa':1L,'subcol':TIME1}
          
        assert_raises(TypeError, self.cf_def_valid.insert, key,col_ncf)
        assert_raises(TypeError, self.cf_def_valid.insert, key,col_ncm)

class TestTimeUUIDs(unittest.TestCase):

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pool = QueuePool(pool_size=5, keyspace='Keyspace1', credentials=credentials)
        self.cf_time = PooledColumnFamily(self.pool, 'StdTimeUUID')

    def tearDown(self):
        self.cf_time.truncate()
        self.pool.dispose()

    def test_datetime_to_uuid(self):

        key = 'key1'

        timeline = []

        timeline.append(datetime.now())
        time1 = uuid1()
        col1 = {time1:'0'}
        self.cf_time.insert(key, col1)
        time.sleep(1)

        timeline.append(datetime.now())
        time2 = uuid1()
        col2 = {time2:'1'}
        self.cf_time.insert(key, col2)
        time.sleep(1)

        timeline.append(datetime.now())

        cols = {time1:'0', time2:'1'}

        assert_equal(self.cf_time.get(key, column_start=timeline[0])                            , cols)
        assert_equal(self.cf_time.get(key,                           column_finish=timeline[2]) , cols)
        assert_equal(self.cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(self.cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(self.cf_time.get(key, column_start=timeline[0], column_finish=timeline[1]) , col1)
        assert_equal(self.cf_time.get(key, column_start=timeline[1], column_finish=timeline[2]) , col2)

    def test_time_to_uuid(self):

        key = 'key1'

        timeline = []

        timeline.append(time.time())
        time1 = uuid1()
        col1 = {time1:'0'}
        self.cf_time.insert(key, col1)
        time.sleep(0.1)

        timeline.append(time.time())
        time2 = uuid1()
        col2 = {time2:'1'}
        self.cf_time.insert(key, col2)
        time.sleep(0.1)

        timeline.append(time.time())

        cols = {time1:'0', time2:'1'}

        assert_equal(self.cf_time.get(key, column_start=timeline[0])                            , cols)
        assert_equal(self.cf_time.get(key,                           column_finish=timeline[2]) , cols)
        assert_equal(self.cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(self.cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(self.cf_time.get(key, column_start=timeline[0], column_finish=timeline[1]) , col1)
        assert_equal(self.cf_time.get(key, column_start=timeline[1], column_finish=timeline[2]) , col2)

    def test_auto_time_to_uuid1(self):

        key = 'key'

        t = time.time()
        col = {t: 'foo'}
        self.cf_time.insert(key, col)
        uuid_res = self.cf_time.get(key).keys()[0]
        timestamp = convert_uuid_to_time(uuid_res)
        assert_almost_equal(timestamp, t, places=3)
