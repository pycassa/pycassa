import uuid

from pycassa import connect, connect_thread_local, ColumnFamily, NotFoundException

from nose.tools import assert_raises

TIME1 = uuid.UUID(hex='ddc6118e-a003-11df-8abf-00234d21610a')
TIME2 = uuid.UUID(hex='40ad6d4c-a004-11df-8abf-00234d21610a')
TIME3 = uuid.UUID(hex='dc3d5234-a00b-11df-8abf-00234d21610a')

VALS = ['val1', 'val2', 'val3']
KEYS = ['key1', 'key2', 'key3']

class TestAutoPacking:

    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.client = connect_thread_local('Keyspace1', credentials=credentials)

        self.cf       = ColumnFamily(self.client, 'Standard2')

        self.cf_long  = ColumnFamily(self.client, 'StdLong')
        self.cf_int   = ColumnFamily(self.client, 'StdInteger')
        self.cf_time  = ColumnFamily(self.client, 'StdTimeUUID')
        self.cf_lex   = ColumnFamily(self.client, 'StdLexicalUUID')
        self.cf_ascii = ColumnFamily(self.client, 'StdAscii')
        self.cf_utf8  = ColumnFamily(self.client, 'StdUTF8')
        self.cf_bytes = ColumnFamily(self.client, 'StdBytes')

        self.cf_suplong  = ColumnFamily(self.client, 'SuperLong', super=True)
        self.cf_supint   = ColumnFamily(self.client, 'SuperInt', super=True)
        self.cf_suptime  = ColumnFamily(self.client, 'SuperTime', super=True)
        self.cf_suplex   = ColumnFamily(self.client, 'SuperLex', super=True)
        self.cf_supascii = ColumnFamily(self.client, 'SuperAscii', super=True)
        self.cf_suputf8  = ColumnFamily(self.client, 'SuperUTF8', super=True)
        self.cf_supbytes = ColumnFamily(self.client, 'SuperBytes', super=True)
 
        self.cf_suplong_sublong  = ColumnFamily(self.client, 'SuperLongSubLong', super=True)
        self.cf_suplong_subint   = ColumnFamily(self.client, 'SuperLongSubInt', super=True)
        self.cf_suplong_subtime  = ColumnFamily(self.client, 'SuperLongSubTime', super=True)
        self.cf_suplong_sublex   = ColumnFamily(self.client, 'SuperLongSubLex', super=True)
        self.cf_suplong_subascii = ColumnFamily(self.client, 'SuperLongSubAscii', super=True)
        self.cf_suplong_subutf8  = ColumnFamily(self.client, 'SuperLongSubUTF8', super=True)
        self.cf_suplong_subbytes = ColumnFamily(self.client, 'SuperLongSubBytes', super=True)
        
        self.cf_valid_long = ColumnFamily(self.client, 'ValidatorLong')
        self.cf_valid_int = ColumnFamily(self.client, 'ValidatorInt')
        self.cf_valid_time = ColumnFamily(self.client, 'ValidatorTime')
        self.cf_valid_lex = ColumnFamily(self.client, 'ValidatorLex')
        self.cf_valid_ascii = ColumnFamily(self.client, 'ValidatorAscii')
        self.cf_valid_utf8 = ColumnFamily(self.client, 'ValidatorUTF8')
        self.cf_valid_bytes = ColumnFamily(self.client, 'ValidatorBytes')

        self.cfs = [self.cf_long, self.cf_int, self.cf_time, self.cf_lex,
                    self.cf_ascii, self.cf_utf8, self.cf_bytes,
                    self.cf_suplong, self.cf_supint, self.cf_suptime,
                    self.cf_suplex, self.cf_supascii, self.cf_suputf8,
                    self.cf_supbytes,
                    self.cf_suplong_subint, self.cf_suplong_subint,
                    self.cf_suplong_subtime, self.cf_suplong_sublex,
                    self.cf_suplong_subascii, self.cf_suplong_subutf8,
                    self.cf_suplong_subbytes,
                    self.cf_valid_long, self.cf_valid_int, self.cf_valid_time,
                    self.cf_valid_lex, self.cf_valid_ascii, self.cf_valid_utf8,
                    self.cf_valid_bytes]

        try:
            self.timestamp_n = int(self.cf.get('meta')['timestamp'])
        except NotFoundException:
            self.timestamp_n = 0
        self.clear()

    def tearDown(self):
        self.cf.insert('meta', {'timestamp': str(self.timestamp_n)})

    def timestamp(self):
        self.timestamp_n += 1
        return self.timestamp_n

    def clear(self):
        for key, columns in self.cf.get_range(include_timestamp=True):
            for value, timestamp in columns.itervalues():
                self.timestamp_n = max(self.timestamp_n, timestamp)
            self.cf.remove(key)
        
        for cf in self.cfs:
            for key, columns in cf.get_range():
                cf.remove(key)

    def test_basic_inserts(self):

        long_col = {1111111111111111L: VALS[0]}
        int_col = {1: VALS[0]}
        time_col = {TIME1: VALS[0]}
        lex_col = {uuid.UUID(bytes='abc abc abc abcd'): VALS[0]}
        ascii_col = {'foo': VALS[0]}
        utf8_col = {u'\u0020'.encode('utf8'): VALS[0]}
        bytes_col = {'bytes': VALS[0]}

        self.cf_long.insert(KEYS[0], long_col)
        self.cf_int.insert(KEYS[0], int_col)
        self.cf_time.insert(KEYS[0], time_col)
        self.cf_lex.insert(KEYS[0], lex_col)
        self.cf_ascii.insert(KEYS[0], ascii_col)
        self.cf_utf8.insert(KEYS[0], utf8_col)
        self.cf_bytes.insert(KEYS[0], bytes_col)

        assert self.cf_long.get(KEYS[0]) == long_col
        assert self.cf_int.get(KEYS[0]) == int_col
        assert self.cf_time.get(KEYS[0]) == time_col
        assert self.cf_lex.get(KEYS[0]) == lex_col
        assert self.cf_ascii.get(KEYS[0]) == ascii_col
        assert self.cf_utf8.get(KEYS[0]) == utf8_col
        assert self.cf_bytes.get(KEYS[0]) == bytes_col

        self.cf_suplong.insert(KEYS[0],  {123L: bytes_col})
        self.cf_supint.insert(KEYS[0],   {111123: bytes_col})
        self.cf_suptime.insert(KEYS[0],  {TIME1: bytes_col})
        self.cf_suplex.insert(KEYS[0],   {uuid.UUID(bytes='aaa aaa aaa aaaa'): bytes_col})
        self.cf_supascii.insert(KEYS[0], {'aaaa': bytes_col})
        self.cf_suputf8.insert(KEYS[0],  {u'a\u0020'.encode('utf8'): bytes_col})
        self.cf_supbytes.insert(KEYS[0], {'aaaa': bytes_col})

        self.cf_suplong_sublong.insert(KEYS[0], {123L: long_col})
        self.cf_suplong_subint.insert(KEYS[0], {123L: int_col})
        self.cf_suplong_subtime.insert(KEYS[0], {123L: time_col})
        self.cf_suplong_sublex.insert(KEYS[0], {123L: lex_col})
        self.cf_suplong_subascii.insert(KEYS[0], {123L: ascii_col})
        self.cf_suplong_subutf8.insert(KEYS[0], {123L: utf8_col})
        self.cf_suplong_subbytes.insert(KEYS[0], {123L: bytes_col})

    def make_group(self, cf, cols):
        diction = { cols[0]: VALS[0],
                    cols[1]: VALS[1],
                    cols[2]: VALS[2]}
        return { 'cf': cf, 'cols': cols, 'dict': diction}  

    def test_standard_column_family(self):
        self.clear()

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

        utf8_cols = [u'a\u0020'.encode('utf8'), u'b\u0020'.encode('utf8'), u'c\u0020'.encode('utf8')]
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

    def make_super_group(self, cf, cols):
        diction = { cols[0]: {'bytes': VALS[0]},
                    cols[1]: {'bytes': VALS[1]},
                    cols[2]: {'bytes': VALS[2]}}
        return { 'cf': cf, 'cols': cols, 'dict': diction}  

    def test_super_column_families(self):
        self.clear()
      
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

        utf8_cols = [u'a\u0020'.encode('utf8'), u'b\u0020'.encode('utf8'), u'c\u0020'.encode('utf8')]
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


    def make_sub_group(self, cf, cols):
        diction = {123L: {cols[0]: VALS[0],
                          cols[1]: VALS[1],
                          cols[2]: VALS[2]}}
        return { 'cf': cf, 'cols': cols, 'dict': diction}  

    def test_super_column_family_subs(self):
        self.clear()
      
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

        utf8_cols = [u'a\u0020'.encode('utf8'), u'b\u0020'.encode('utf8'), u'c\u0020'.encode('utf8')]
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
                assert sub_res[1] == group.get('dict')
            
            res = group.get('cf').get_range(start=KEYS[0], column_start=123L, column_finish=123L)
            for sub_res in res:
                assert sub_res[1] == group.get('dict')

            res = group.get('cf').get_range(start=KEYS[0], columns=[123L])
            for sub_res in res:
                assert sub_res[1] == group.get('dict')

            res = group.get('cf').get_range(start=KEYS[0], super_column=123L)
            for sub_res in res:
                assert sub_res[1] == group.get('dict').get(123L)

    def test_validated_columns(self):
        self.clear()

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

        col = {'subcol':u'a\u0020'.encode('utf8')}
        self.cf_valid_utf8.insert(key, col)
        assert self.cf_valid_utf8.get(key) == col

        col = {'subcol':'aaa'}
        self.cf_valid_bytes.insert(key, col)
        assert self.cf_valid_bytes.get(key) == col

