from pycassa import NotFoundException
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.util import OrderedDict, convert_uuid_to_time
from pycassa.system_manager import SystemManager
from pycassa.types import (LongType, IntegerType, TimeUUIDType, LexicalUUIDType,
                           AsciiType, UTF8Type, BytesType, CompositeType,
                           OldPycassaDateType, IntermediateDateType, DateType,
                           BooleanType, CassandraType, DecimalType,
                           FloatType, Int32Type, UUIDType, DoubleType, DynamicCompositeType)
from pycassa.index import create_index_expression, create_index_clause
import pycassa.marshal as marshal

from nose import SkipTest
from nose.tools import (assert_raises, assert_equal, assert_almost_equal,
                        assert_true)

from datetime import date, datetime
from uuid import uuid1
from decimal import Decimal
import uuid
import unittest
import time
from collections import namedtuple

TIME1 = uuid.UUID(hex='ddc6118e-a003-11df-8abf-00234d21610a')
TIME2 = uuid.UUID(hex='40ad6d4c-a004-11df-8abf-00234d21610a')
TIME3 = uuid.UUID(hex='dc3d5234-a00b-11df-8abf-00234d21610a')

VALS = ['val1', 'val2', 'val3']
KEYS = ['key1', 'key2', 'key3']

pool = None
TEST_KS = 'PycassaTestKeyspace'

def setup_module():
    global pool
    credentials = {'username': 'jsmith', 'password': 'havebadpass'}
    pool = ConnectionPool(TEST_KS, pool_size=10, credentials=credentials, timeout=1.0)

def teardown_module():
    pool.dispose()

class TestCFs(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'StdLong', comparator_type=LongType())
        sys.create_column_family(TEST_KS, 'StdInteger', comparator_type=IntegerType())
        sys.create_column_family(TEST_KS, 'StdBigInteger', comparator_type=IntegerType())
        sys.create_column_family(TEST_KS, 'StdDecimal', comparator_type=DecimalType())
        sys.create_column_family(TEST_KS, 'StdTimeUUID', comparator_type=TimeUUIDType())
        sys.create_column_family(TEST_KS, 'StdLexicalUUID', comparator_type=LexicalUUIDType())
        sys.create_column_family(TEST_KS, 'StdAscii', comparator_type=AsciiType())
        sys.create_column_family(TEST_KS, 'StdUTF8', comparator_type=UTF8Type())
        sys.create_column_family(TEST_KS, 'StdBytes', comparator_type=BytesType())
        sys.create_column_family(TEST_KS, 'StdComposite',
                                 comparator_type=CompositeType(LongType(), BytesType()))
        sys.create_column_family(TEST_KS, 'StdDynamicComposite',
                                 comparator_type=DynamicCompositeType({'a': AsciiType(),
                                 'b': BytesType(), 'c': DecimalType(), 'd': DateType(),
                                 'f': FloatType(), 'i': IntegerType(), 'l': LongType(),
                                 'n': Int32Type(), 's': UTF8Type(), 't': TimeUUIDType(),
                                 'u': UUIDType(), 'w': DoubleType(), 'x': LexicalUUIDType(),
                                 'y': BooleanType()}))
        sys.close()

        cls.cf_long = ColumnFamily(pool, 'StdLong')
        cls.cf_int = ColumnFamily(pool, 'StdInteger')
        cls.cf_big_int = ColumnFamily(pool, 'StdBigInteger')
        cls.cf_decimal = ColumnFamily(pool, 'StdDecimal')
        cls.cf_time = ColumnFamily(pool, 'StdTimeUUID')
        cls.cf_lex = ColumnFamily(pool, 'StdLexicalUUID')
        cls.cf_ascii = ColumnFamily(pool, 'StdAscii')
        cls.cf_utf8 = ColumnFamily(pool, 'StdUTF8')
        cls.cf_bytes = ColumnFamily(pool, 'StdBytes')
        cls.cf_composite = ColumnFamily(pool, 'StdComposite')
        cls.cf_dynamic_composite = ColumnFamily(pool, 'StdDynamicComposite')

        cls.cfs = [cls.cf_long, cls.cf_int, cls.cf_time, cls.cf_lex,
                   cls.cf_ascii, cls.cf_utf8, cls.cf_bytes, cls.cf_composite, 
                   cls.cf_dynamic_composite]

    def tearDown(self):
        for cf in TestCFs.cfs:
            for key, cols in cf.get_range():
                cf.remove(key)

    def make_group(self, cf, cols):
        diction = OrderedDict([(cols[0], VALS[0]),
                               (cols[1], VALS[1]),
                               (cols[2], VALS[2])])
        return {'cf': cf, 'cols': cols, 'dict': diction}

    def test_standard_column_family(self):

        # For each data type, create a group that includes its column family,
        # a set of column names, and a dictionary that maps from the column
        # names to values.
        type_groups = []

        long_cols = [1111111111111111L,
                     2222222222222222L,
                     3333333333333333L]
        type_groups.append(self.make_group(TestCFs.cf_long, long_cols))

        int_cols = [1, 2, 3]
        type_groups.append(self.make_group(TestCFs.cf_int, int_cols))

        big_int_cols = [1 + int(time.time() * 10 ** 6),
                        2 + int(time.time() * 10 ** 6),
                        3 + int(time.time() * 10 ** 6)]
        type_groups.append(self.make_group(TestCFs.cf_big_int, big_int_cols))

        decimal_cols = [Decimal('1.123456789123456789'),
                        Decimal('2.123456789123456789'),
                        Decimal('3.123456789123456789')]
        type_groups.append(self.make_group(TestCFs.cf_decimal, decimal_cols))

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

        dynamic_composite_cols = [(('LongType', 1), ('BytesType', 'foo')), 
                                  (('LongType', 2), ('BytesType', 'bar')), 
                                  (('LongType', 3), ('BytesType', 'baz'))]
        type_groups.append(self.make_group(TestCFs.cf_dynamic_composite, dynamic_composite_cols))

        dynamic_composite_alias_cols = [(('l', 1), ('b', 'foo')), 
                                        (('l', 2), ('b', 'bar')), 
                                        (('l', 3), ('b', 'baz'))]
        type_groups.append(self.make_group(TestCFs.cf_dynamic_composite, dynamic_composite_alias_cols))

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

            # Test xget paging
            assert_equal(list(cf.xget(KEYS[0], buffer_size=2)), gdict.items())
            assert_equal(list(cf.xget(KEYS[0], column_reversed=True, buffer_size=2)),
                         list(reversed(gdict.items())))
            assert_equal(list(cf.xget(KEYS[0], column_start=gcols[0], buffer_size=2)),
                         gdict.items())
            assert_equal(list(cf.xget(KEYS[0], column_finish=gcols[2], buffer_size=2)),
                         gdict.items())
            assert_equal(list(cf.xget(KEYS[0], column_start=gcols[2], column_finish=gcols[0],
                                      column_reversed=True, buffer_size=2)),
                         list(reversed(gdict.items())))
            assert_equal(list(cf.xget(KEYS[0], column_start=gcols[1], column_finish=gcols[1],
                                      column_reversed=True, buffer_size=2)),
                         [(gcols[1], VALS[1])])

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
        sys.create_column_family(TEST_KS, 'SuperBigInt', super=True, comparator_type=IntegerType())
        sys.create_column_family(TEST_KS, 'SuperTime', super=True, comparator_type=TimeUUIDType())
        sys.create_column_family(TEST_KS, 'SuperLex', super=True, comparator_type=LexicalUUIDType())
        sys.create_column_family(TEST_KS, 'SuperAscii', super=True, comparator_type=AsciiType())
        sys.create_column_family(TEST_KS, 'SuperUTF8', super=True, comparator_type=UTF8Type())
        sys.create_column_family(TEST_KS, 'SuperBytes', super=True, comparator_type=BytesType())
        sys.close()

        cls.cf_suplong = ColumnFamily(pool, 'SuperLong')
        cls.cf_supint = ColumnFamily(pool, 'SuperInt')
        cls.cf_supbigint = ColumnFamily(pool, 'SuperBigInt')
        cls.cf_suptime = ColumnFamily(pool, 'SuperTime')
        cls.cf_suplex = ColumnFamily(pool, 'SuperLex')
        cls.cf_supascii = ColumnFamily(pool, 'SuperAscii')
        cls.cf_suputf8 = ColumnFamily(pool, 'SuperUTF8')
        cls.cf_supbytes = ColumnFamily(pool, 'SuperBytes')

        cls.cfs = [cls.cf_suplong, cls.cf_supint, cls.cf_suptime,
                   cls.cf_suplex, cls.cf_supascii, cls.cf_suputf8,
                   cls.cf_supbytes]

    def tearDown(self):
        for cf in TestSuperCFs.cfs:
            for key, cols in cf.get_range():
                cf.remove(key)

    def make_super_group(self, cf, cols):
        diction = OrderedDict([(cols[0], {'bytes': VALS[0]}),
                               (cols[1], {'bytes': VALS[1]}),
                               (cols[2], {'bytes': VALS[2]})])
        return {'cf': cf, 'cols': cols, 'dict': diction}

    def test_super_column_families(self):

        # For each data type, create a group that includes its column family,
        # a set of column names, and a dictionary that maps from the column
        # names to values.
        type_groups = []

        long_cols = [1111111111111111L,
                     2222222222222222L,
                     3333333333333333L]
        type_groups.append(self.make_super_group(TestSuperCFs.cf_suplong, long_cols))

        int_cols = [1, 2, 3]
        type_groups.append(self.make_super_group(TestSuperCFs.cf_supint, int_cols))

        big_int_cols = [1 + int(time.time() * 10 ** 6),
                        2 + int(time.time() * 10 ** 6),
                        3 + int(time.time() * 10 ** 6)]
        type_groups.append(self.make_super_group(TestSuperCFs.cf_supbigint, big_int_cols))

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

            # test xget paging
            assert_equal(list(cf.xget(KEYS[0], buffer_size=2)), gdict.items())

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
        sys.create_column_family(TEST_KS, 'SuperLongSubBigInt', super=True,
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

        cls.cf_suplong_sublong = ColumnFamily(pool, 'SuperLongSubLong')
        cls.cf_suplong_subint = ColumnFamily(pool, 'SuperLongSubInt')
        cls.cf_suplong_subbigint = ColumnFamily(pool, 'SuperLongSubBigInt')
        cls.cf_suplong_subtime = ColumnFamily(pool, 'SuperLongSubTime')
        cls.cf_suplong_sublex = ColumnFamily(pool, 'SuperLongSubLex')
        cls.cf_suplong_subascii = ColumnFamily(pool, 'SuperLongSubAscii')
        cls.cf_suplong_subutf8 = ColumnFamily(pool, 'SuperLongSubUTF8')
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
        return {'cf': cf, 'cols': cols, 'dict': diction}

    def test_super_column_family_subs(self):

        # For each data type, create a group that includes its column family,
        # a set of column names, and a dictionary that maps from the column
        # names to values.
        type_groups = []

        long_cols = [1111111111111111L,
                     2222222222222222L,
                     3333333333333333L]
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_sublong, long_cols))

        int_cols = [1, 2, 3]
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_subint, int_cols))

        big_int_cols = [1 + int(time.time() * 10 ** 6),
                        2 + int(time.time() * 10 ** 6),
                        3 + int(time.time() * 10 ** 6)]
        type_groups.append(self.make_sub_group(TestSuperSubCFs.cf_suplong_subbigint, big_int_cols))

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

    def test_validation_with_packed_names(self):
        """
        Make sure that validated columns are packed correctly when the
        column names themselves must be packed
        """
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'Validators2',
                comparator_type=LongType(), default_validation_class=LongType())
        sys.alter_column(TEST_KS, 'Validators2', 1, TimeUUIDType())
        sys.close()

        my_uuid = uuid.uuid1()
        cf = ColumnFamily(pool, 'Validators2')

        cf.insert('key', {0: 0})
        assert_equal(cf.get('key'), {0: 0})

        cf.insert('key', {1: my_uuid})
        assert_equal(cf.get('key'), {0: 0, 1: my_uuid})

        cf.insert('key', {0: 0, 1: my_uuid})
        assert_equal(cf.get('key'), {0: 0, 1: my_uuid})

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

        col = {'long': 1L}
        cf.insert(key, col)
        assert_equal(cf.get(key)['long'], 1L)

        col = {'int': 1}
        cf.insert(key, col)
        assert_equal(cf.get(key)['int'], 1)

        col = {'time': TIME1}
        cf.insert(key, col)
        assert_equal(cf.get(key)['time'], TIME1)

        col = {'lex': uuid.UUID(bytes='aaa aaa aaa aaaa')}
        cf.insert(key, col)
        assert_equal(cf.get(key)['lex'], uuid.UUID(bytes='aaa aaa aaa aaaa'))

        col = {'ascii': 'aaa'}
        cf.insert(key, col)
        assert_equal(cf.get(key)['ascii'], 'aaa')

        col = {'utf8': u'a\u0020'}
        cf.insert(key, col)
        assert_equal(cf.get(key)['utf8'], u'a\u0020')

        col = {'bytes': 'aaa'}
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

        col_cf = {'aaaaaa': 1L}
        col_cm = {'subcol': TIME1}
        col_ncf = {'aaaaaa': TIME1}

        # Both of these inserts work, as cf allows
        #  longs and cm for 'subcol' allows TIMEUUIDs.
        cf.insert(key, col_cf)
        cf.insert(key, col_cm)
        assert_equal(cf.get(key), {'aaaaaa': 1L, 'subcol': TIME1})

        # Insert multiple columns at once
        col_cf.update(col_cm)
        cf.insert(key, col_cf)
        assert_equal(cf.get(key), {'aaaaaa': 1L, 'subcol': TIME1})

        assert_raises(TypeError, cf.insert, key, col_ncf)

        cf.remove(key)

class TestKeyValidators(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()

        sys.create_column_family(TEST_KS, 'KeyLong', key_validation_class=LongType())
        sys.create_column_family(TEST_KS, 'KeyInteger', key_validation_class=IntegerType())
        sys.create_column_family(TEST_KS, 'KeyTimeUUID', key_validation_class=TimeUUIDType())
        sys.create_column_family(TEST_KS, 'KeyLexicalUUID', key_validation_class=LexicalUUIDType())
        sys.create_column_family(TEST_KS, 'KeyAscii', key_validation_class=AsciiType())
        sys.create_column_family(TEST_KS, 'KeyUTF8', key_validation_class=UTF8Type())
        sys.create_column_family(TEST_KS, 'KeyBytes', key_validation_class=BytesType())
        sys.close()

        cls.cf_long = ColumnFamily(pool, 'KeyLong')
        cls.cf_int = ColumnFamily(pool, 'KeyInteger')
        cls.cf_time = ColumnFamily(pool, 'KeyTimeUUID')
        cls.cf_lex = ColumnFamily(pool, 'KeyLexicalUUID')
        cls.cf_ascii = ColumnFamily(pool, 'KeyAscii')
        cls.cf_utf8 = ColumnFamily(pool, 'KeyUTF8')
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

        int_keys = [1, 2, 3]
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
                assert_equal(cf.get(key), {str(key): 'val'})
                cf.remove(key)
                assert_raises(NotFoundException, cf.get, key)

    def test_add_remove_counter(self):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'KeyLongCounter', key_validation_class=LongType(),
                                 default_validation_class='CounterColumnType')
        sys.close()
        cf_long = ColumnFamily(pool, 'KeyLongCounter')

        key = 1111111111111111L

        cf_long.add(key, 'col')
        assert_equal(cf_long.get(key), {'col': 1})
        cf_long.remove_counter(key, 'col')
        time.sleep(0.1)
        assert_raises(NotFoundException, cf_long.get, key)

class TestComposites(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'StaticComposite',
                                 comparator_type=CompositeType(LongType(),
                                                               IntegerType(),
                                                               TimeUUIDType(reversed=True),
                                                               LexicalUUIDType(reversed=False),
                                                               AsciiType(),
                                                               UTF8Type(),
                                                               BytesType()))

    @classmethod
    def teardown_class(cls):
        sys = SystemManager()
        sys.drop_column_family(TEST_KS, 'StaticComposite')

    def test_static_composite_basic(self):
        cf = ColumnFamily(pool, 'StaticComposite')
        colname = (127312831239123123, 1, uuid.uuid1(), uuid.uuid4(), 'foo', u'ba\u0254r', 'baz')
        cf.insert('key', {colname: 'val'})
        assert_equal(cf.get('key'), {colname: 'val'})

    def test_static_composite_slicing(self):
        cf = ColumnFamily(pool, 'StaticComposite')
        u1 = uuid.uuid1()
        u4 = uuid.uuid4()
        col0 = (0, 1, u1, u4, '', '', '')
        col1 = (1, 1, u1, u4, '', '', '')
        col2 = (1, 2, u1, u4, '', '', '')
        col3 = (1, 3, u1, u4, '', '', '')
        col4 = (2, 1, u1, u4, '', '', '')
        cf.insert('key2', {col0: '', col1: '', col2: '', col3: '', col4: ''})

        result = cf.get('key2', column_start=((1, True),), column_finish=((1, True),))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(1,), column_finish=((2, False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=((1, True),), column_finish=((2, False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(1, ), column_finish=((2, False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=((0, False), ), column_finish=((2, False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(1, 1), column_finish=(1, 3))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(1, 1), column_finish=(1, (3, True)))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(1, (1, True)), column_finish=((2, False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

    def test_static_composite_get_partial_composite(self):
        cf = ColumnFamily(pool, 'StaticComposite')
        cf.insert('key3', {(123123, 1): 'val'})
        assert_equal(cf.get('key3'), {(123123, 1): 'val'})

    def test_uuid_composites(self):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'UUIDComposite',
                comparator_type=CompositeType(IntegerType(reversed=True), TimeUUIDType()),
                key_validation_class=TimeUUIDType(),
                default_validation_class=UTF8Type())

        key, u1, u2 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
        cf = ColumnFamily(pool, 'UUIDComposite')
        cf.insert(key, {(123123, u1): 'foo'})
        cf.insert(key, {(123123, u1): 'foo', (-1, u2): 'bar', (-123123123, u1): 'baz'})
        assert_equal(cf.get(key), {(123123, u1): 'foo', (-1, u2): 'bar', (-123123123, u1): 'baz'})

    def test_single_component_composite(self):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'SingleComposite',
                comparator_type=CompositeType(IntegerType()))

        cf = ColumnFamily(pool, 'SingleComposite')
        cf.insert('key', {(123456,): 'val'})
        assert_equal(cf.get('key'), {(123456,): 'val'})

class TestDynamicComposites(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'StaticDynamicComposite',
                                 comparator_type=DynamicCompositeType({'l': LongType(),
                                                                       'i': IntegerType(),
                                                                       'T': TimeUUIDType(reversed=True),
                                                                       'x': LexicalUUIDType(reversed=False),
                                                                       'a': AsciiType(),
                                                                       's': UTF8Type(),
                                                                       'b': BytesType()}))

    @classmethod
    def teardown_class(cls):
        sys = SystemManager()
        sys.drop_column_family(TEST_KS, 'StaticDynamicComposite')

    def setUp(self):
        global a, b, i, I, x, l, t, T, s

        component = namedtuple('DynamicComponent', ['type','value'])
        ascii_alias = component('a', None)
        bytes_alias = component('b', None)
        integer_alias = component('i', None)
        integer_rev_alias = component('I', None)
        lexicaluuid_alias = component('x', None)
        long_alias = component('l', None)
        timeuuid_alias = component('t', None)
        timeuuid_rev_alias = component('T', None)
        utf8_alias = component('s', None)

        _r = lambda t, v: t._replace(value=v) 
        a = lambda v: _r(ascii_alias, v)
        b = lambda v: _r(bytes_alias, v)
        i = lambda v: _r(integer_alias, v)
        I = lambda v: _r(integer_rev_alias, v)
        x = lambda v: _r(lexicaluuid_alias, v)
        l = lambda v: _r(long_alias, v)
        t = lambda v: _r(timeuuid_alias, v)
        T = lambda v: _r(timeuuid_rev_alias, v)
        s = lambda v: _r(utf8_alias, v)

    def test_static_composite_basic(self):
        cf = ColumnFamily(pool, 'StaticDynamicComposite')
        colname = (l(127312831239123123), i(1), T(uuid.uuid1()), x(uuid.uuid4()), a('foo'), s(u'ba\u0254r'), b('baz'))
        cf.insert('key', {colname: 'val'})
        assert_equal(cf.get('key'), {colname: 'val'})

    def test_static_composite_slicing(self):
        cf = ColumnFamily(pool, 'StaticDynamicComposite')
        u1 = uuid.uuid1()
        u4 = uuid.uuid4()
        col0 = (l(0), i(1), T(u1), x(u4), a(''), s(''), b(''))
        col1 = (l(1), i(1), T(u1), x(u4), a(''), s(''), b(''))
        col2 = (l(1), i(2), T(u1), x(u4), a(''), s(''), b(''))
        col3 = (l(1), i(3), T(u1), x(u4), a(''), s(''), b(''))
        col4 = (l(2), i(1), T(u1), x(u4), a(''), s(''), b(''))
        cf.insert('key2', {col0: '', col1: '', col2: '', col3: '', col4: ''})

        result = cf.get('key2', column_start=((l(1), True),), column_finish=((l(1), True),))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(l(1),), column_finish=((l(2), False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=((l(1), True),), column_finish=((l(2), False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(l(1), ), column_finish=((l(2), False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=((l(0), False), ), column_finish=((l(2), False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(l(1), i(1)), column_finish=(l(1), i(3)))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(l(1), i(1)), column_finish=(l(1), (i(3), True)))
        assert_equal(result, {col1: '', col2: '', col3: ''})

        result = cf.get('key2', column_start=(l(1), (i(1), True)), column_finish=((l(2), False), ))
        assert_equal(result, {col1: '', col2: '', col3: ''})

    def test_static_composite_get_partial_composite(self):
        cf = ColumnFamily(pool, 'StaticDynamicComposite')
        cf.insert('key3', {(l(123123), i(1)): 'val'})
        assert_equal(cf.get('key3'), {(l(123123), i(1)): 'val'})

    def test_uuid_composites(self):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'UUIDDynamicComposite',
                comparator_type=DynamicCompositeType({'I': IntegerType(reversed=True), 't': TimeUUIDType()}),
                key_validation_class=TimeUUIDType(),
                default_validation_class=UTF8Type())

        key, u1, u2 = uuid.uuid1(), uuid.uuid1(), uuid.uuid1()
        cf = ColumnFamily(pool, 'UUIDDynamicComposite')
        cf.insert(key, {(I(123123), t(u1)): 'foo'})
        cf.insert(key, {(I(123123), t(u1)): 'foo', (I(-1), t(u2)): 'bar', (I(-123123123), t(u1)): 'baz'})
        assert_equal(cf.get(key), {(I(123123), t(u1)): 'foo', (I(-1), t(u2)): 'bar', (I(-123123123), t(u1)): 'baz'})

    def test_single_component_composite(self):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'SingleDynamicComposite',
                comparator_type=DynamicCompositeType({'i': IntegerType()}))

        cf = ColumnFamily(pool, 'SingleDynamicComposite')
        cf.insert('key', {(i(123456),): 'val'})
        assert_equal(cf.get('key'), {(i(123456),): 'val'})

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

    def test_datetime_to_uuid(self):
        cf_time = TestTimeUUIDs.cf_time
        key = 'key1'
        timeline = []

        timeline.append(datetime.utcnow())
        time1 = uuid1()
        col1 = {time1: '0'}
        cf_time.insert(key, col1)
        time.sleep(1)

        timeline.append(datetime.utcnow())
        time2 = uuid1()
        col2 = {time2: '1'}
        cf_time.insert(key, col2)
        time.sleep(1)

        timeline.append(datetime.utcnow())

        cols = {time1: '0', time2: '1'}

        assert_equal(cf_time.get(key, column_start=timeline[0])                            , cols)
        assert_equal(cf_time.get(key,                           column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[1]) , col1)
        assert_equal(cf_time.get(key, column_start=timeline[1], column_finish=timeline[2]) , col2)
        cf_time.remove(key)

    def test_time_to_uuid(self):
        cf_time = TestTimeUUIDs.cf_time
        key = 'key1'
        timeline = []

        timeline.append(time.time())
        time1 = uuid1()
        col1 = {time1: '0'}
        cf_time.insert(key, col1)
        time.sleep(0.1)

        timeline.append(time.time())
        time2 = uuid1()
        col2 = {time2: '1'}
        cf_time.insert(key, col2)
        time.sleep(0.1)

        timeline.append(time.time())

        cols = {time1:'0', time2: '1'}

        assert_equal(cf_time.get(key, column_start=timeline[0])                            , cols)
        assert_equal(cf_time.get(key,                           column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[2]) , cols)
        assert_equal(cf_time.get(key, column_start=timeline[0], column_finish=timeline[1]) , col1)
        assert_equal(cf_time.get(key, column_start=timeline[1], column_finish=timeline[2]) , col2)
        cf_time.remove(key)

    def test_auto_time_to_uuid1(self):
        cf_time = TestTimeUUIDs.cf_time
        key = 'key1'
        t = time.time()
        col = {t: 'foo'}
        cf_time.insert(key, col)
        uuid_res = cf_time.get(key).keys()[0]
        timestamp = convert_uuid_to_time(uuid_res)
        assert_almost_equal(timestamp, t, places=3)
        cf_time.remove(key)

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

class TestDateTypes(unittest.TestCase):

    def _compare_dates(self, d1, d2):
        self.assertEquals(d1.timetuple(), d2.timetuple())
        self.assertEquals(int(d1.microsecond/1e3), int(d2.microsecond/1e3))

    def test_compatibility(self):
        self.cf = ColumnFamily(pool, 'Standard1')
        self.cf.column_validators['date'] = OldPycassaDateType()

        d = datetime.utcnow()
        self.cf.insert('key1', {'date': d})
        self._compare_dates(self.cf.get('key1')['date'], d)

        self.cf.column_validators['date'] = IntermediateDateType()
        self._compare_dates(self.cf.get('key1')['date'], d)
        self.cf.insert('key1', {'date': d})
        self._compare_dates(self.cf.get('key1')['date'], d)

        self.cf.column_validators['date'] = DateType()
        self._compare_dates(self.cf.get('key1')['date'], d)
        self.cf.insert('key1', {'date': d})
        self._compare_dates(self.cf.get('key1')['date'], d)
        self.cf.remove('key1')

class TestPackerOverride(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(TEST_KS, 'CompositeOverrideCF',
                comparator_type=CompositeType(AsciiType(), AsciiType()),
                default_validation_class=AsciiType())

    @classmethod
    def teardown_class(cls):
        sys = SystemManager()
        sys.drop_column_family(TEST_KS, 'CompositeOverrideCF')

    def test_column_validator(self):
        cf = ColumnFamily(pool, 'CompositeOverrideCF')
        cf.column_validators[('a', 'b')] = BooleanType()
        cf.insert('key', {('a', 'a'): 'foo', ('a', 'b'): True})
        assert_equal(cf.get('key'), {('a', 'a'): 'foo', ('a', 'b'): True})

        assert_equal(cf.column_validators[('a', 'b')].__class__, BooleanType)

        keys = cf.column_validators.keys()
        assert_equal(keys, [('a', 'b')])

        del cf.column_validators[('a', 'b')]
        assert_raises(KeyError, cf.column_validators.__getitem__, ('a', 'b'))

class TestCustomTypes(unittest.TestCase):

    class IntString(CassandraType):

        @staticmethod
        def pack(intval):
            return str(intval)

        @staticmethod
        def unpack(strval):
            return int(strval)

    class IntString2(CassandraType):

        def __init__(self, *args, **kwargs):
            self.pack = lambda val: str(val)
            self.unpack = lambda val: int(val)

    def test_staticmethod_funcs(self):
        self.cf = ColumnFamily(pool, 'Standard1')
        self.cf.key_validation_class = TestCustomTypes.IntString()
        self.cf.insert(1234, {'col': 'val'})
        assert_equal(self.cf.get(1234), {'col': 'val'})

    def test_constructor_lambdas(self):
        self.cf = ColumnFamily(pool, 'Standard1')
        self.cf.key_validation_class = TestCustomTypes.IntString2()
        self.cf.insert(1234, {'col': 'val'})
        assert_equal(self.cf.get(1234), {'col': 'val'})

class TestCustomComposite(unittest.TestCase):
    """
    Test CompositeTypes with custom inner types.
    """

    # Some contrived scenarios
    class IntDateType(CassandraType):
        """
        Represent a date as an integer. E.g.: March 05, 2012 = 20120305
        """
        @staticmethod
        def pack(v, *args, **kwargs):
            assert type(v) in (datetime, date), "Invalid arg"
            str_date = v.strftime("%Y%m%d")
            return marshal.encode_int(int(str_date))

        @staticmethod
        def unpack(v, *args, **kwargs):
            int_date = marshal.decode_int(v)
            return date(*time.strptime(str(int_date), "%Y%m%d")[0:3])

    class IntString(CassandraType):

        @staticmethod
        def pack(intval):
            return str(intval)

        @staticmethod
        def unpack(strval):
            return int(strval)

    class IntString2(CassandraType):

        def __init__(self, *args, **kwargs):
            self.pack = lambda val: str(val)
            self.unpack = lambda val: int(val)

    @classmethod
    def setup_class(cls):
        sys = SystemManager()
        sys.create_column_family(
            TEST_KS,
            'CustomComposite1',
            comparator_type=CompositeType(
                IntegerType(),
                UTF8Type()))

    @classmethod
    def teardown_class(cls):
        sys = SystemManager()
        sys.drop_column_family(TEST_KS, 'CustomComposite1')

    def test_static_composite_basic(self):
        cf = ColumnFamily(pool, 'CustomComposite1')
        colname = (20120305, '12345')
        cf.insert('key', {colname: 'val1'})
        assert_equal(cf.get('key'), {colname: 'val1'})

    def test_insert_with_custom_composite(self):
        cf_std = ColumnFamily(pool, 'CustomComposite1')
        cf_cust = ColumnFamily(pool, 'CustomComposite1')
        cf_cust.column_name_class = CompositeType(
                TestCustomComposite.IntDateType(),
                TestCustomComposite.IntString())

        std_col = (20120311, '321')
        cust_col = (date(2012, 3, 11), 321)
        cf_cust.insert('cust_insert_key_1', {cust_col: 'cust_insert_val_1'})
        assert_equal(cf_std.get('cust_insert_key_1'),
                {std_col: 'cust_insert_val_1'})

    def test_retrieve_with_custom_composite(self):
        cf_std = ColumnFamily(pool, 'CustomComposite1')
        cf_cust = ColumnFamily(pool, 'CustomComposite1')
        cf_cust.column_name_class = CompositeType(
                TestCustomComposite.IntDateType(),
                TestCustomComposite.IntString())

        std_col = (20120312, '321')
        cust_col = (date(2012, 3, 12), 321)
        cf_std.insert('cust_insert_key_2', {std_col: 'cust_insert_val_2'})
        assert_equal(cf_cust.get('cust_insert_key_2'),
                {cust_col: 'cust_insert_val_2'})

    def test_composite_slicing(self):
        cf_std = ColumnFamily(pool, 'CustomComposite1')
        cf_cust = ColumnFamily(pool, 'CustomComposite1')
        cf_cust.column_name_class = CompositeType(
                TestCustomComposite.IntDateType(),
                TestCustomComposite.IntString2())

        col0 = (20120101, '123')
        col1 = (20120102, '123')
        col2 = (20120102, '456')
        col3 = (20120102, '789')
        col4 = (20120103, '123')

        dt0 = date(2012, 1, 1)
        dt1 = date(2012, 1, 2)
        dt2 = date(2012, 1, 3)

        col1_cust = (dt1, 123)
        col2_cust = (dt1, 456)
        col3_cust = (dt1, 789)

        cf_std.insert('key2', {col0: '', col1: '', col2: '', col3: '', col4: ''})

        def check(column_start, column_finish, col_reversed=False):
            result = cf_cust.get('key2', column_start=column_start,
                    column_finish=column_finish, column_reversed=col_reversed)

            assert_equal(result, {col1_cust: '', col2_cust: '', col3_cust: ''})

        # Defaults should be inclusive on both ends
        check((dt1,), (dt1,))
        check((dt1,), (dt1,), True)

        check(((dt1, True),), ((dt1, True),))
        check((dt1,), ((dt2, False),))
        check(((dt1, True),), ((dt2, False),))
        check(((dt0, False),), ((dt2, False),))

        check((dt1, 123), (dt1, 789))
        check((dt1, 123), (dt1, (789, True)))
        check((dt1, (123, True)), ((dt2, False),))

        # Test inclusive ends for reversed
        check(((dt1, True),), ((dt1, True),), True)
        check( (dt1,),        ((dt1, True),), True)
        check(((dt1, True),),  (dt1,),        True)

        # Test exclusive ends for reversed
        check(((dt2, False),), ((dt0, False),), True)
        check(((dt2, False),),  (dt1,),         True)
        check((dt1,),          ((dt0, False),), True)

