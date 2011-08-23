from pycassa import NotFoundException
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.util import *
from pycassa.system_manager import *
from pycassa.index import *

from nose.tools import (assert_raises, assert_equal, assert_almost_equal,
                        assert_true)

from datetime import datetime
from uuid import uuid1
import uuid
import unittest
import time

TIME1 = uuid.UUID(hex='ddc6118e-a003-11df-8abf-00234d21610a')
TIME2 = uuid.UUID(hex='40ad6d4c-a004-11df-8abf-00234d21610a')
TIME3 = uuid.UUID(hex='dc3d5234-a00b-11df-8abf-00234d21610a')

VALS = ['val1', 'val2', 'val3']
KEYS = ['key1', 'key2', 'key3']

TEST_KS = 'PycassaTestKeyspace'

def setup_module():
    global pool
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(TEST_KS, pool_size=10, credentials=credentials)

def teardown_module():
    pool.dispose()

class TestCFs(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'StdLong', comparator_type=LongType())
        sys.create_column_family(TEST_KS, 'StdInteger', comparator_type=IntegerType())
        sys.create_column_family(TEST_KS, 'StdTimeUUID', comparator_type=TimeUUIDType())
        sys.create_column_family(TEST_KS, 'StdLexicalUUID', comparator_type=LexicalUUIDType())
        sys.create_column_family(TEST_KS, 'StdAscii', comparator_type=AsciiType())
        sys.create_column_family(TEST_KS, 'StdUTF8', comparator_type=UTF8Type())
        sys.create_column_family(TEST_KS, 'StdBytes', comparator_type=BytesType())
        sys.create_column_family(TEST_KS, 'StdComposite',
                                 comparator_type=CompositeType(LongType(), BytesType()))
        sys.close()

        cls.cf_long  = ColumnFamily(pool, 'StdLong')
        cls.cf_int   = ColumnFamily(pool, 'StdInteger')
        cls.cf_time  = ColumnFamily(pool, 'StdTimeUUID')
        cls.cf_lex   = ColumnFamily(pool, 'StdLexicalUUID')
        cls.cf_ascii = ColumnFamily(pool, 'StdAscii')
        cls.cf_utf8  = ColumnFamily(pool, 'StdUTF8')
        cls.cf_bytes = ColumnFamily(pool, 'StdBytes')
        cls.cf_composite = ColumnFamily(pool, 'StdComposite')

        cls.cfs = [cls.cf_long, cls.cf_int, cls.cf_time, cls.cf_lex,
                   cls.cf_ascii, cls.cf_utf8, cls.cf_bytes,
                   cls.cf_composite]

    def tearDown(self):
        for cf in TestCFs.cfs:
            for key, cols in cf.get_range():
                cf.remove(key)

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
        type_groups.append(self.make_group(TestCFs.cf_long, long_cols))

        int_cols = [1,2,3]
        type_groups.append(self.make_group(TestCFs.cf_int, int_cols))

        time_cols = [TIME1, TIME2, TIME3]
        type_groups.append(self.make_group(TestCFs.cf_time, time_cols))

        lex_cols = [uuid.UUID(bytes='aaa aaa aaa aaaa'),
                    uuid.UUID(bytes='bbb bbb bbb bbbb'),
                    uuid.UUID(bytes='ccc ccc ccc cccc')]
        type_groups.append(self.make_group(TestCFs.cf_lex, lex_cols))

        ascii_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_group(TestCFs.cf_ascii, ascii_cols))

        utf8_cols = [u'a\u0020', u'b\u0020', u'c\u0020']
        type_groups.append(self.make_group(TestCFs.cf_utf8, utf8_cols))

        bytes_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_group(TestCFs.cf_bytes, bytes_cols))

        composite_cols = [(1, 'foo'), (2, 'bar'), (3, 'baz')]
        type_groups.append(self.make_group(TestCFs.cf_composite, composite_cols))

        # Begin the actual inserting and getting
        for group in type_groups:
            cf = group.get('cf')
            gdict = group.get('dict')
            gcols = group.get('cols')

            cf.insert(KEYS[0], gdict)
            assert_equal(cf.get(KEYS[0]), gdict)

            # Check each column individually
            for i in range(3):
                assert_equal(cf.get(KEYS[0], columns=[gcols[i]]),
                             {gcols[i]: VALS[i]})

            # Check that if we list all columns, we get the full dict
            assert_equal(cf.get(KEYS[0], columns=gcols[:]), gdict)
            # The same thing with a start and end instead
            assert_equal(cf.get(KEYS[0], column_start=gcols[0], column_finish=gcols[2]),
                         gdict)
            # A start and end that are the same
            assert_equal(cf.get(KEYS[0], column_start=gcols[0], column_finish=gcols[0]),
                         {gcols[0]: VALS[0]})

            assert_equal(cf.get_count(KEYS[0]), 3)

            # Test removing rows
            cf.remove(KEYS[0], columns=gcols[:1])
            assert_equal(cf.get_count(KEYS[0]), 2)

            cf.remove(KEYS[0], columns=gcols[1:])
            assert_equal(cf.get_count(KEYS[0]), 0)

            # Insert more than one row now
            cf.insert(KEYS[0], gdict)
            cf.insert(KEYS[1], gdict)
            cf.insert(KEYS[2], gdict)


            ### multiget() tests ###

            res = cf.multiget(KEYS[:])
            for i in range(3):
                assert_equal(res.get(KEYS[i]), gdict)

            res = cf.multiget(KEYS[2:])
            assert_equal(res.get(KEYS[2]), gdict)

            # Check each column individually
            for i in range(3):
                res = cf.multiget(KEYS[:], columns=[gcols[i]])
                for j in range(3):
                    assert_equal(res.get(KEYS[j]), {gcols[i]: VALS[i]})

            # Check that if we list all columns, we get the full dict
            res = cf.multiget(KEYS[:], columns=gcols[:])
            for j in range(3):
                assert_equal(res.get(KEYS[j]), gdict)

            # The same thing with a start and end instead
            res = cf.multiget(KEYS[:], column_start=gcols[0], column_finish=gcols[2])
            for j in range(3):
                assert_equal(res.get(KEYS[j]), gdict)

            # A start and end that are the same
            res = cf.multiget(KEYS[:], column_start=gcols[0], column_finish=gcols[0])
            for j in range(3):
                assert_equal(res.get(KEYS[j]), {gcols[0]: VALS[0]})


            ### get_range() tests ###

            res = cf.get_range(start=KEYS[0])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)

            res = cf.get_range(start=KEYS[0], column_start=gcols[0], column_finish=gcols[2])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)

            res = cf.get_range(start=KEYS[0], columns=gcols[:])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)


class TestSuperCFs(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'SuperLong', super=True, comparator_type=LongType())
        sys.create_column_family(TEST_KS, 'SuperInt', super=True, comparator_type=IntegerType())
        sys.create_column_family(TEST_KS, 'SuperTime', super=True, comparator_type=TimeUUIDType())
        sys.create_column_family(TEST_KS, 'SuperLex', super=True, comparator_type=LexicalUUIDType())
        sys.create_column_family(TEST_KS, 'SuperAscii', super=True, comparator_type=AsciiType())
        sys.create_column_family(TEST_KS, 'SuperUTF8', super=True, comparator_type=UTF8Type())
        sys.create_column_family(TEST_KS, 'SuperBytes', super=True, comparator_type=BytesType())
        sys.close()

        cls.cf_suplong  = ColumnFamily(pool, 'SuperLong')
        cls.cf_supint   = ColumnFamily(pool, 'SuperInt')
        cls.cf_suptime  = ColumnFamily(pool, 'SuperTime')
        cls.cf_suplex   = ColumnFamily(pool, 'SuperLex')
        cls.cf_supascii = ColumnFamily(pool, 'SuperAscii')
        cls.cf_suputf8  = ColumnFamily(pool, 'SuperUTF8')
        cls.cf_supbytes = ColumnFamily(pool, 'SuperBytes')

        cls.cfs = [cls.cf_suplong, cls.cf_supint, cls.cf_suptime,
                   cls.cf_suplex, cls.cf_supascii, cls.cf_suputf8,
                   cls.cf_supbytes]

    def tearDown(self):
        for cf in TestSuperCFs.cfs:
            for key, cols in cf.get_range():
                cf.remove(key)

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
        type_groups.append(self.make_super_group(TestSuperCFs.cf_suplong, long_cols))

        int_cols = [1,2,3]
        type_groups.append(self.make_super_group(TestSuperCFs.cf_supint, int_cols))

        time_cols = [TIME1, TIME2, TIME3]
        type_groups.append(self.make_super_group(TestSuperCFs.cf_suptime, time_cols))

        lex_cols = [uuid.UUID(bytes='aaa aaa aaa aaaa'),
                    uuid.UUID(bytes='bbb bbb bbb bbbb'),
                    uuid.UUID(bytes='ccc ccc ccc cccc')]
        type_groups.append(self.make_super_group(TestSuperCFs.cf_suplex, lex_cols))

        ascii_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_super_group(TestSuperCFs.cf_supascii, ascii_cols))

        utf8_cols = [u'a\u0020', u'b\u0020', u'c\u0020']
        type_groups.append(self.make_super_group(TestSuperCFs.cf_suputf8, utf8_cols))

        bytes_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_super_group(TestSuperCFs.cf_supbytes, bytes_cols))

        # Begin the actual inserting and getting
        for group in type_groups:
            cf = group.get('cf')
            gdict = group.get('dict')
            gcols = group.get('cols')

            cf.insert(KEYS[0], gdict)
            assert_equal(cf.get(KEYS[0]), gdict)

            # Check each supercolumn individually
            for i in range(3):
                res = cf.get(KEYS[0], columns=[gcols[i]])
                assert_equal(res, {gcols[i]: {'bytes': VALS[i]}})

            # Check that if we list all columns, we get the full dict
            assert_equal(cf.get(KEYS[0], columns=gcols[:]), gdict)
            # The same thing with a start and end instead
            assert_equal(cf.get(KEYS[0], column_start=gcols[0], column_finish=gcols[2]), gdict)
            # A start and end that are the same
            assert_equal(cf.get(KEYS[0], column_start=gcols[0], column_finish=gcols[0]),
                         {gcols[0]: {'bytes': VALS[0]}})

            assert_equal(cf.get_count(KEYS[0]), 3)

            # Test removing rows
            cf.remove(KEYS[0], columns=gcols[:1])
            assert_equal(cf.get_count(KEYS[0]), 2)

            cf.remove(KEYS[0], columns=gcols[1:])
            assert_equal(cf.get_count(KEYS[0]), 0)

            # Insert more than one row now
            cf.insert(KEYS[0], gdict)
            cf.insert(KEYS[1], gdict)
            cf.insert(KEYS[2], gdict)


            ### multiget() tests ###

            res = cf.multiget(KEYS[:])
            for i in range(3):
                assert_equal(res.get(KEYS[i]), gdict)

            res = cf.multiget(KEYS[2:])
            assert_equal(res.get(KEYS[2]), gdict)

            # Check each column individually
            for i in range(3):
                res = cf.multiget(KEYS[:], columns=[gcols[i]])
                for j in range(3):
                    assert_equal(res.get(KEYS[j]), {gcols[i]: {'bytes': VALS[i]}})

            # Check that if we list all columns, we get the full dict
            res = cf.multiget(KEYS[:], columns=gcols[:])
            for j in range(3):
                assert_equal(res.get(KEYS[j]), gdict)

            # The same thing with a start and end instead
            res = cf.multiget(KEYS[:], column_start=gcols[0], column_finish=gcols[2])
            for j in range(3):
                assert_equal(res.get(KEYS[j]), gdict)

            # A start and end that are the same
            res = cf.multiget(KEYS[:], column_start=gcols[0], column_finish=gcols[0])
            for j in range(3):
                assert_equal(res.get(KEYS[j]), {gcols[0]: {'bytes': VALS[0]}})


            ### get_range() tests ###

            res = cf.get_range(start=KEYS[0])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)

            res = cf.get_range(start=KEYS[0], column_start=gcols[0], column_finish=gcols[2])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)

            res = cf.get_range(start=KEYS[0], columns=gcols[:])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)


class TestSuperSubCFs(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'SuperLongSubLong', super=True,
                                 comparator_type=LongType(), subcomparator_type=LongType())
        sys.create_column_family(TEST_KS, 'SuperLongSubInt', super=True,
                                 comparator_type=LongType(), subcomparator_type=IntegerType())
        sys.create_column_family(TEST_KS, 'SuperLongSubTime', super=True,
                                 comparator_type=LongType(), subcomparator_type=TimeUUIDType())
        sys.create_column_family(TEST_KS, 'SuperLongSubLex', super=True,
                                 comparator_type=LongType(), subcomparator_type=LexicalUUIDType())
        sys.create_column_family(TEST_KS, 'SuperLongSubAscii', super=True,
                                 comparator_type=LongType(), subcomparator_type=AsciiType())
        sys.create_column_family(TEST_KS, 'SuperLongSubUTF8', super=True,
                                 comparator_type=LongType(), subcomparator_type=UTF8Type())
        sys.create_column_family(TEST_KS, 'SuperLongSubBytes', super=True,
                                 comparator_type=LongType(), subcomparator_type=BytesType())
        sys.close()

        cls.cf_suplong_sublong  = ColumnFamily(pool, 'SuperLongSubLong')
        cls.cf_suplong_subint   = ColumnFamily(pool, 'SuperLongSubInt')
        cls.cf_suplong_subtime  = ColumnFamily(pool, 'SuperLongSubTime')
        cls.cf_suplong_sublex   = ColumnFamily(pool, 'SuperLongSubLex')
        cls.cf_suplong_subascii = ColumnFamily(pool, 'SuperLongSubAscii')
        cls.cf_suplong_subutf8  = ColumnFamily(pool, 'SuperLongSubUTF8')
        cls.cf_suplong_subbytes = ColumnFamily(pool, 'SuperLongSubBytes')

        cls.cfs = [cls.cf_suplong_subint, cls.cf_suplong_subint,
                   cls.cf_suplong_subtime, cls.cf_suplong_sublex,
                   cls.cf_suplong_subascii, cls.cf_suplong_subutf8,
                   cls.cf_suplong_subbytes]

    def tearDown(self):
        for cf in TestSuperSubCFs.cfs:
            for key, cols in cf.get_range():
                cf.remove(key)

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
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_sublong, long_cols))

        int_cols = [1,2,3]
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_subint, int_cols))

        time_cols = [TIME1, TIME2, TIME3]
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_subtime, time_cols))

        lex_cols = [uuid.UUID(bytes='aaa aaa aaa aaaa'),
                    uuid.UUID(bytes='bbb bbb bbb bbbb'),
                    uuid.UUID(bytes='ccc ccc ccc cccc')]
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_sublex, lex_cols))

        ascii_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_subascii, ascii_cols))

        utf8_cols = [u'a\u0020', u'b\u0020', u'c\u0020']
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_subutf8, utf8_cols))

        bytes_cols = ['aaaa', 'bbbb', 'cccc']
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_subbytes, bytes_cols))

        # Begin the actual inserting and getting
        for group in type_groups:
            cf = group.get('cf')
            gdict = group.get('dict')

            cf.insert(KEYS[0], gdict)

            assert_equal(cf.get(KEYS[0]), gdict)
            assert_equal(cf.get(KEYS[0], columns=[123L]), gdict)

            # A start and end that are the same
            assert_equal(cf.get(KEYS[0], column_start=123L, column_finish=123L), gdict)

            res = cf.get(KEYS[0], super_column=123L, column_start=group.get('cols')[0])
            assert_equal(res, gdict.get(123L))

            res = cf.get(KEYS[0], super_column=123L, column_finish=group.get('cols')[-1])
            assert_equal(res, gdict.get(123L))

            assert_equal(cf.get_count(KEYS[0]), 1)

            # Test removing rows
            cf.remove(KEYS[0], super_column=123L)
            assert_equal(cf.get_count(KEYS[0]), 0)

            # Insert more than one row now
            cf.insert(KEYS[0], gdict)
            cf.insert(KEYS[1], gdict)
            cf.insert(KEYS[2], gdict)


            ### multiget() tests ###

            res = cf.multiget(KEYS[:])
            for i in range(3):
                assert_equal(res.get(KEYS[i]), gdict)

            res = cf.multiget(KEYS[2:])
            assert_equal(res.get(KEYS[2]), gdict)

            res = cf.multiget(KEYS[:], columns=[123L])
            for i in range(3):
                assert_equal(res.get(KEYS[i]), gdict)

            res = cf.multiget(KEYS[:], super_column=123L)
            for i in range(3):
                assert_equal(res.get(KEYS[i]), gdict.get(123L))

            res = cf.multiget(KEYS[:], super_column=123L, column_start=group.get('cols')[0])
            for i in range(3):
                assert_equal(res.get(KEYS[i]), gdict.get(123L))

            res = cf.multiget(KEYS[:], column_start=123L, column_finish=123L)
            for j in range(3):
                assert_equal(res.get(KEYS[j]), gdict)

            ### get_range() tests ###

            res = cf.get_range(start=KEYS[0])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)

            res = cf.get_range(start=KEYS[0], column_start=123L, column_finish=123L)
            for sub_res in res:
                assert_equal(sub_res[1], gdict)

            res = cf.get_range(start=KEYS[0], columns=[123L])
            for sub_res in res:
                assert_equal(sub_res[1], gdict)

            res = cf.get_range(start=KEYS[0], super_column=123L)
            for sub_res in res:
                assert_equal(sub_res[1], gdict.get(123L))

            res = cf.get_range(start=KEYS[0], super_column=123L, column_start=group.get('cols')[0])
            for sub_res in res:
                assert_equal(sub_res[1], gdict.get(123L))

            res = cf.get_range(start=KEYS[0], super_column=123L, column_finish=group.get('cols')[-1])
            for sub_res in res:
                assert_equal(sub_res[1], gdict.get(123L))

class TestValidators(unittest.TestCase):

    def test_validated_columns(self):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'Validators',)
        sys.alter_column(TEST_KS, 'Validators', 'long', LongType())
        sys.alter_column(TEST_KS, 'Validators', 'int', IntegerType())
        sys.alter_column(TEST_KS, 'Validators', 'time', TimeUUIDType())
        sys.alter_column(TEST_KS, 'Validators', 'lex', LexicalUUIDType())
        sys.alter_column(TEST_KS, 'Validators', 'ascii', AsciiType())
        sys.alter_column(TEST_KS, 'Validators', 'utf8', UTF8Type())
        sys.alter_column(TEST_KS, 'Validators', 'bytes', BytesType())
        sys.close()

        cf = ColumnFamily(pool, 'Validators')
        key = 'key1'

        col = {'long':1L}
        cf.insert(key, col)
        assert_equal(cf.get(key)['long'], 1L)

        col = {'int':1}
        cf.insert(key, col)
        assert_equal(cf.get(key)['int'], 1)

        col = {'time':TIME1}
        cf.insert(key, col)
        assert_equal(cf.get(key)['time'], TIME1)

        col = {'lex':uuid.UUID(bytes='aaa aaa aaa aaaa')}
        cf.insert(key, col)
        assert_equal(cf.get(key)['lex'], uuid.UUID(bytes='aaa aaa aaa aaaa'))

        col = {'ascii':'aaa'}
        cf.insert(key, col)
        assert_equal(cf.get(key)['ascii'], 'aaa')

        col = {'utf8':u'a\u0020'}
        cf.insert(key, col)
        assert_equal(cf.get(key)['utf8'], u'a\u0020')

        col = {'bytes':'aaa'}
        cf.insert(key, col)
        assert_equal(cf.get(key)['bytes'], 'aaa')

        cf.remove(key)


class TestDefaultValidators(unittest.TestCase):

    def test_default_validated_columns(self):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'DefaultValidator', default_validation_class=LongType())
        sys.alter_column(TEST_KS, 'DefaultValidator', 'subcol', TimeUUIDType())
        sys.close()

        cf = ColumnFamily(pool, 'DefaultValidator')
        key = 'key1'

        col_cf  = {'aaaaaa': 1L}
        col_cm  = {'subcol': TIME1}
        col_ncf = {'aaaaaa': TIME1}
        col_ncm = {'subcol': 1L}

        # Both of these inserts work, as cf allows
        #  longs and cm for 'subcol' allows TIMEUUIDs.
        cf.insert(key, col_cf)
        cf.insert(key, col_cm)
        assert_equal(cf.get(key), {'aaaaaa': 1L, 'subcol': TIME1})

        assert_raises(TypeError, cf.insert, key, col_ncf)

        cf.remove(key)

class TestKeyValidators(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        have_key_validators = sys._conn.version != CASSANDRA_07
        if not have_key_validators:
            raise SkipTest("Cassandra 0.7 does not have key validators")

        sys.create_column_family(TEST_KS, 'KeyLong', key_validation_class=LongType())
        sys.create_column_family(TEST_KS, 'KeyInteger', key_validation_class=IntegerType())
        sys.create_column_family(TEST_KS, 'KeyTimeUUID', key_validation_class=TimeUUIDType())
        sys.create_column_family(TEST_KS, 'KeyLexicalUUID', key_validation_class=LexicalUUIDType())
        sys.create_column_family(TEST_KS, 'KeyAscii', key_validation_class=AsciiType())
        sys.create_column_family(TEST_KS, 'KeyUTF8', key_validation_class=UTF8Type())
        sys.create_column_family(TEST_KS, 'KeyBytes', key_validation_class=BytesType())
        sys.close()

        cls.cf_long  = ColumnFamily(pool, 'KeyLong')
        cls.cf_int   = ColumnFamily(pool, 'KeyInteger')
        cls.cf_time  = ColumnFamily(pool, 'KeyTimeUUID')
        cls.cf_lex   = ColumnFamily(pool, 'KeyLexicalUUID')
        cls.cf_ascii = ColumnFamily(pool, 'KeyAscii')
        cls.cf_utf8  = ColumnFamily(pool, 'KeyUTF8')
        cls.cf_bytes = ColumnFamily(pool, 'KeyBytes')

        cls.cfs = [cls.cf_long, cls.cf_int, cls.cf_time, cls.cf_lex,
                    cls.cf_ascii, cls.cf_utf8, cls.cf_bytes]

    def tearDown(self):
        for cf in TestKeyValidators.cfs:
            for key, cols in cf.get_range():
                cf.remove(key)

    def setUp(self):
        self.type_groups = []

        long_keys = [1111111111111111L,
                     2222222222222222L,
                     3333333333333333L]
        self.type_groups.append((TestKeyValidators.cf_long, long_keys))

        int_keys = [1,2,3]
        self.type_groups.append((TestKeyValidators.cf_int, int_keys))

        time_keys = [TIME1, TIME2, TIME3]
        self.type_groups.append((TestKeyValidators.cf_time, time_keys))

        lex_keys = [uuid.UUID(bytes='aaa aaa aaa aaaa'),
                    uuid.UUID(bytes='bbb bbb bbb bbbb'),
                    uuid.UUID(bytes='ccc ccc ccc cccc')]
        self.type_groups.append((TestKeyValidators.cf_lex, lex_keys))

        ascii_keys = ['aaaa', 'bbbb', 'cccc']
        self.type_groups.append((TestKeyValidators.cf_ascii, ascii_keys))

        utf8_keys = [u'a\u0020', u'b\u0020', u'c\u0020']
        self.type_groups.append((TestKeyValidators.cf_utf8, utf8_keys))

        bytes_keys = ['aaaa', 'bbbb', 'cccc']
        self.type_groups.append((TestKeyValidators.cf_bytes, bytes_keys))

    def test_inserts(self):
        for cf, keys in self.type_groups:
            for key in keys:
                cf.insert(key, {str(key): 'val'})
                results = cf.get(key)
                assert_equal(results, {str(key): 'val'})

                col1 = str(key) + "1"
                col2 = str(key) + "2"
                cols = {col1: "val1", col2: "val2"}
                cf.insert(key, cols)
                results = cf.get(key)
                cols.update({str(key): 'val'})
                assert_equal(results, cols)

    def test_batch_insert(self):
        for cf, keys in self.type_groups:
            rows = dict([(key, {str(key): 'val'}) for key in keys])
            cf.batch_insert(rows)
            for key in keys:
                results = cf.get(key)
                assert_equal(results, {str(key): 'val'})

    def test_multiget(self):
        for cf, keys in self.type_groups:
            for key in keys:
                cf.insert(key, {str(key): 'val'})
            results = cf.multiget(keys)
            for key in keys:
                assert_true(key in results)
                assert_equal(results[key], {str(key): 'val'})

    def test_get_count(self):
        for cf, keys in self.type_groups:
            for key in keys:
                cf.insert(key, {str(key): 'val'})
                results = cf.get_count(key)
                assert_equal(results, 1)

    def test_multiget_count(self):
        for cf, keys in self.type_groups:
            for key in keys:
                cf.insert(key, {str(key): 'val'})
            results = cf.multiget_count(keys)
            for key in keys:
                assert_true(key in results, "%s should be in %r" % (key, results))
                assert_equal(results[key], 1)

    def test_get_range(self):
        for cf, keys in self.type_groups:
            for key in keys:
                cf.insert(key, {str(key): 'val'})

            rows = list(cf.get_range())
            assert_equal(len(rows), len(keys))
            for k, c in rows:
                assert_true(k in keys)
                assert_equal(c, {str(k): 'val'})

    def test_get_indexed_slices(self):
        sys = SystemManager()
        for cf, keys in self.type_groups:
            sys.create_index(TEST_KS, cf.column_family, 'birthdate', LongType())
            cf = ColumnFamily(pool, cf.column_family)
            for key in keys:
                cf.insert(key, {'birthdate': 1})
            expr = create_index_expression('birthdate', 1)
            clause = create_index_clause([expr])
            rows = list(cf.get_indexed_slices(clause))
            assert_equal(len(rows), len(keys))
            for k, c in rows:
                assert_true(k in keys)
                assert_equal(c, {'birthdate': 1})

    def test_remove(self):
        for cf, keys in self.type_groups:
            for key in keys:
                cf.insert(key, {str(key): 'val'})
                assert_equal(cf.get(key), {str(key):'val'})
                cf.remove(key)
                assert_raises(NotFoundException, cf.get, key)

    def test_add_remove_counter(self):
        sys = SystemManager()
        if sys._conn.version == CASSANDRA_07:
            raise SkipTest("Cassandra 0.7 does not have key validators")

        sys.create_column_family(TEST_KS, 'KeyLongCounter', key_validation_class=LongType(),
                                 default_validation_class=COUNTER_COLUMN_TYPE)
        sys.close()
        cf_long  = ColumnFamily(pool, 'KeyLongCounter')

        key = 1111111111111111L

        cf_long.add(key, 'col')
        assert_equal(cf_long.get(key), {'col': 1})
        cf_long.remove_counter(key, 'col')
        time.sleep(0.1)
        assert_raises(NotFoundException, cf_long.get, key)

class TestComposites(unittest.TestCase):

    def test_static_composite(cls):
        sys = SystemManager()
        have_composites = sys._conn.version != CASSANDRA_07
        if not have_composites:
            raise SkipTest("Cassandra < 0.8 does not composite types")

        sys.create_column_family(TEST_KS, 'StaticComposite',
                                 comparator_type=CompositeType(LongType(),
                                                               IntegerType(),
                                                               TimeUUIDType(reversed=True),
                                                               LexicalUUIDType(reversed=False),
                                                               AsciiType(),
                                                               UTF8Type(),
                                                               BytesType()))

        cf = ColumnFamily(pool, 'StaticComposite')
        colname = (127312831239123123, 1, uuid.uuid1(), uuid.uuid4(), 'foo', u'ba\u0254r', 'baz')
        cf.insert('key', {colname: 'val'})
        assert_equal(cf.get('key'), {colname: 'val'})

        sys.drop_column_family(TEST_KS, 'StaticComposite')

class TestBigInt(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'StdInteger', comparator_type=IntegerType())

    @classmethod
    def teardown_class(cls):
        sys = SystemManager()
        sys.drop_column_family(TEST_KS, 'StdInteger')

    def setUp(self):
        self.key = 'TestBigInt'
        self.cf = ColumnFamily(pool, 'StdInteger')

    def tearDown(self):
        self.cf.remove(self.key)

    def test_negative_integers(self):
        self.cf.insert(self.key, {-1: '-1'})
        self.cf.insert(self.key, {-12342390: '-12342390'})
        self.cf.insert(self.key, {-255: '-255'})
        self.cf.insert(self.key, {-256: '-256'})
        self.cf.insert(self.key, {-257: '-257'})
        for key, cols in self.cf.get_range():
            self.assertEquals(str(cols.keys()[0]), cols.values()[0])

class TestTimeUUIDs(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'TestTimeUUIDs', comparator_type=TimeUUIDType())
        sys.close()
        cls.cf_time = ColumnFamily(pool, 'TestTimeUUIDs')

    def tearDown(self):
        TestTimeUUIDs.cf_time.truncate()

    def test_datetime_to_uuid(self):
        cf_time = TestTimeUUIDs.cf_time
        key = 'key1'
        timeline = []

        timeline.append(datetime.now())
        time1 = uuid1()
        col1 = {time1:'0'}
        cf_time.insert(key, col1)
        time.sleep(1)

        timeline.append(datetime.now())
        time2 = uuid1()
        col2 = {time2:'1'}
        cf_time.insert(key, col2)
        time.sleep(1)

        timeline.append(datetime.now())

        cols = {time1:'0', time2:'1'}

        assert_equal(cf_time.get(key, column_start=timeline[0])                            , cols)
        assert_equal(cf_time.get(key,                           column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[1]) , col1)
        assert_equal(cf_time.get(key, column_start=timeline[1], column_finish=timeline[2]) , col2)

    def test_time_to_uuid(self):
        cf_time = TestTimeUUIDs.cf_time
        key = 'key1'
        timeline = []

        timeline.append(time.time())
        time1 = uuid1()
        col1 = {time1:'0'}
        cf_time.insert(key, col1)
        time.sleep(0.1)

        timeline.append(time.time())
        time2 = uuid1()
        col2 = {time2:'1'}
        cf_time.insert(key, col2)
        time.sleep(0.1)

        timeline.append(time.time())

        cols = {time1:'0', time2:'1'}

        assert_equal(cf_time.get(key, column_start=timeline[0])                            , cols)
        assert_equal(cf_time.get(key,                           column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[1]) , col1)
        assert_equal(cf_time.get(key, column_start=timeline[1], column_finish=timeline[2]) , col2)

    def test_auto_time_to_uuid1(self):
        cf_time = TestTimeUUIDs.cf_time
        key = 'key1'
        t = time.time()
        col = {t: 'foo'}
        cf_time.insert(key, col)
        uuid_res = cf_time.get(key).keys()[0]
        timestamp = convert_uuid_to_time(uuid_res)
        assert_almost_equal(timestamp, t, places=3)

class TestTypeErrors(unittest.TestCase):

    def test_packing_enabled(self):
        self.cf = ColumnFamily(pool, 'Standard1')
        self.cf.insert('key', {'col': 'val'})
        assert_raises(TypeError, self.cf.insert, args=('key', {123: 'val'}))
        assert_raises(TypeError, self.cf.insert, args=('key', {'col': 123}))
        assert_raises(TypeError, self.cf.insert, args=('key', {123: 123}))
        self.cf.remove('key')

    def test_packing_disabled(self):
        self.cf = ColumnFamily(pool, 'Standard1', autopack_names=False, autopack_values=False)
        self.cf.insert('key', {'col': 'val'})
        assert_raises(TypeError, self.cf.insert, args=('key', {123: 'val'}))
        assert_raises(TypeError, self.cf.insert, args=('key', {'col': 123}))
        assert_raises(TypeError, self.cf.insert, args=('key', {123: 123}))
        self.cf.remove('key')
