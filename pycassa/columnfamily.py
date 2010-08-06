from cassandra.ttypes import Column, ColumnOrSuperColumn, ColumnParent, \
    ColumnPath, ConsistencyLevel, NotFoundException, SlicePredicate, \
    SliceRange, SuperColumn, Mutation, Deletion, Clock, KeyRange

import time
import sys
import uuid
import struct

__all__ = ['gm_timestamp', 'ColumnFamily']
_TYPES = ['BytesType', 'LongType', 'IntegerType', 'UTF8Type', 'AsciiType',
         'LexicalUUIDType', 'TimeUUIDType']

def gm_timestamp():
    """
    Returns
    -------
    int : UNIX epoch time in GMT
    """
    return int(time.time() * 1e6)

def create_SlicePredicate(columns, column_start, column_finish, column_reversed, column_count):
    if columns is not None:
        return SlicePredicate(column_names=columns)
    sr = SliceRange(start=column_start, finish=column_finish,
                    reversed=column_reversed, count=column_count)
    return SlicePredicate(slice_range=sr)

class ColumnFamily(object):
    def __init__(self, client, column_family, buffer_size=1024,
                 read_consistency_level=ConsistencyLevel.ONE,
                 write_consistency_level=ConsistencyLevel.ONE,
                 timestamp=gm_timestamp, super=False,
                 dict_class=dict):
        """
        Construct a ColumnFamily

        Parameters
        ----------
        client   : cassandra.Cassandra.Client
            Cassandra client with thrift API
        column_family : str
            The name of this ColumnFamily
        buffer_size : int
            When calling get_range(), the intermediate results need to be
            buffered if we are fetching many rows, otherwise the Cassandra
            server will overallocate memory and fail.  This is the size of
            that buffer.
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation
        write_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any write operation
        timestamp : function
            The default timestamp function returns:
            int(time.mktime(time.gmtime()))
            Or the number of seconds since Unix epoch in GMT.
            Set timestamp to replace the default timestamp function with your
            own.
        super : bool
            Whether this ColumnFamily has SuperColumns
        dict_class : class (must act like the dict type)
            The default dict_class is dict.
            If the order of columns matter to you, pass your own dictionary
            class, or python 2.7's new collections.OrderedDict. All returned
            rows and subcolumns are instances of this.
        """
        self.client = client
        self.column_family = column_family
        self.buffer_size = buffer_size
        self.read_consistency_level = read_consistency_level
        self.write_consistency_level = write_consistency_level
        self.timestamp = timestamp
        self.super = super
        self.dict_class = dict_class

        # Determine the ColumnFamily type to allow for auto conversion
        # so that packing/unpacking doesn't need to be done manually
        self.col_name_data_type = None
        self.supercol_name_data_type = None
        description = client.describe_keyspace(client._keyspace)
        col_fam = description.get(self.column_family)
        if col_fam is not None:
            if not self.super:
                self.col_name_data_type = col_fam.get('CompareWith')
            else:
                self.supercol_name_data_type = col_fam.get('CompareWith')
                self.col_name_data_type = col_fam.get('CompareSubcolumnsWith')
                self.supercol_name_data_type = self._extract_type_name(self.supercol_name_data_type)

            index = self.col_name_data_type = self._extract_type_name(self.col_name_data_type)

    def _extract_type_name(self, string):

        if string is None: return 'BytesType'

        index = string.rfind('.')
        if index == -1:
            string = 'BytesType'
        else:
            string = string[index + 1: ]
            if string not in _TYPES:
                string = 'BytesType'
        return string

    def _convert_Column_to_base(self, column, include_timestamp):
        if include_timestamp:
            return (column.value, column.clock.timestamp)
        return column.value

    def _convert_SuperColumn_to_base(self, super_column, include_timestamp):
        ret = self.dict_class()
        for column in super_column.columns:
            ret[self._unpack(column.name)] = self._convert_Column_to_base(column, include_timestamp)
        return ret

    def _convert_ColumnOrSuperColumns_to_dict_class(self, list_col_or_super, include_timestamp):
        ret = self.dict_class()
        for col_or_super in list_col_or_super:
            if col_or_super.super_column is not None:
                col = col_or_super.super_column
                ret[self._unpack(col.name, is_supercol_name=True)] = self._convert_SuperColumn_to_base(col, include_timestamp)
            else:
                col = col_or_super.column
                ret[self._unpack(col.name)] = self._convert_Column_to_base(col, include_timestamp)
        return ret

    def _convert_KeySlice_list_to_dict_class(self, keyslice_list, include_timestamp):
        ret = self.dict_class()
        for keyslice in keyslice_list:
            ret[keyslice.key] = self._convert_ColumnOrSuperColumns_to_dict_class(keyslice.columns, include_timestamp)
        return ret

    def _rcl(self, alternative):
        """Helper function that returns self.read_consistency_level if
        alternative is None, otherwise returns alternative"""
        if alternative is None:
            return self.read_consistency_level
        return alternative

    def _wcl(self, alternative):
        """Helper function that returns self.write_consistency_level
        if alternative is None, otherwise returns alternative"""
        if alternative is None:
            return self.write_consistency_level
        return alternative

    def _pack(self, value, is_supercol_name=False):
        """
        Packs a value into the expected sequence of bytes that Cassandra expects.
        """
        
        if value is None: return

        if is_supercol_name:
            d_type = self.supercol_name_data_type
        else:
            d_type = self.col_name_data_type

        if d_type == 'LongType':
            return struct.pack('>q', long(value))  # q is 'long long'
        elif d_type == 'IntegerType':
            return struct.pack('>i', int(value))
        elif d_type == 'AsciiType' or d_type == 'UTF8Type':
            return struct.pack(">%ds" % len(value), value)
        elif d_type == 'TimeUUIDType' or d_type == 'LexicalUUIDType':
            return struct.pack('>16s', value.bytes)
        else: 
            return value

    def _unpack(self, b, is_supercol_name=False):
        """
        Unpacks Cassandra's byte-representation of values into their Python
        equivalents.
        """

        if b is None: return

        if is_supercol_name:
            d_type = self.supercol_name_data_type
        else:
            d_type = self.col_name_data_type

        if d_type == 'LongType':
            return struct.unpack('>q', b)[0]
        elif d_type == 'IntegerType':
            return struct.unpack('>i', b)[0]
        elif d_type == 'AsciiType' or d_type == 'UTF8Type':
            return struct.unpack('>%ds' % len(b), b)[0]
        elif d_type == 'LexicalUUIDType' or d_type == 'TimeUUIDType':
            temp_bytes = struct.unpack('>16s', b)[0]
            return uuid.UUID(bytes=temp_bytes)
        else: # BytesType
            return b

    def get(self, key, columns=None, column_start="", column_finish="",
            column_reversed=False, column_count=100, include_timestamp=False,
            super_column=None, read_consistency_level = None):
        """
        Fetch a key from a Cassandra server
        
        Parameters
        ----------
        key : str
            The key to fetch
        columns : [str]
            Limit the columns or super_columns fetched to the specified list
        column_start : str
            Only fetch when a column or super_column is >= column_start
        column_finish : str
            Only fetch when a column or super_column is <= column_finish
        column_reversed : bool
            Fetch the columns or super_columns in reverse order. This will do
            nothing unless you passed a dict_class to the constructor.
        column_count : int
            Limit the number of columns or super_columns fetched per key
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column
        super_column : str
            Return columns only in this super_column
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation

        Returns
        -------
        if include_timestamp == True: {'column': ('value', timestamp)}
        else: {'column': 'value'}
        """

        if super_column != '': super_column = self._pack(super_column, is_supercol_name=True)
        if column_start != '': column_start = self._pack(column_start, is_supercol_name=self.super)
        if column_finish != '': column_finish = self._pack(column_finish, is_supercol_name=self.super) 

        packed_cols = None
        if columns is not None:
            packed_cols = []
            for col in columns:
                packed_cols.append(self._pack(col, is_supercol_name=self.super))

        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = create_SlicePredicate(packed_cols, column_start, column_finish,
                                   column_reversed, column_count)

        list_col_or_super = self.client.get_slice(key, cp, sp,
                                                  self._rcl(read_consistency_level))

        if len(list_col_or_super) == 0:
            raise NotFoundException()
        return self._convert_ColumnOrSuperColumns_to_dict_class(list_col_or_super, include_timestamp)

    def get_indexed_slices(self, index_clause, columns=None, column_start="", column_finish="",
                          column_reversed=False, column_count=100, include_timestamp=False,
                          super_column=None, read_consistency_level=None):
        """
        Fetches a list of KeySlices from a Cassandra server based on an index clause
        
        Parameters
        ----------
        index_clause : IndexClause
            Limits the keys that are returned based on expressions that compare
            the value of a column to a given value.  At least one of the
            expressions in the IndexClause must be on an indexed column.
            See index_clause.create_index_clause() and create_index_expression().
        columns : [str]
            Limit the columns or super_columns fetched to the specified list
        column_start : str
            Only fetch when a column or super_column is >= column_start
        column_finish : str
            Only fetch when a column or super_column is <= column_finish
        column_reversed : bool
            Fetch the columns or super_columns in reverse order. This will do
            nothing unless you passed a dict_class to the constructor.
        column_count : int
            Limit the number of columns or super_columns fetched per key
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column
        super_column : str
            Return columns only in this super_column
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation

        Returns
        -------
        if include_timestamp == True: {key : {column : (value, timestamp)}}
        else: {key : {column : value}}
        """

        if super_column != '': super_column = self._pack(super_column, is_supercol_name=True)
        if column_start != '': column_start = self._pack(column_start, is_supercol_name=self.super)
        if column_finish != '': column_finish = self._pack(column_finish, is_supercol_name=self.super) 

        packed_cols = None
        if columns is not None:
            packed_cols = []
            for col in columns:
                packed_cols.append(self._pack(col, is_supercol_name=self.super))

        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = create_SlicePredicate(packed_cols, column_start, column_finish,
                                   column_reversed, column_count)

        keyslice_list = self.client.get_indexed_slices(cp, index_clause, sp,
                                                  self._rcl(read_consistency_level))

        if len(keyslice_list) == 0:
            raise NotFoundException()
        return self._convert_KeySlice_list_to_dict_class(keyslice_list, include_timestamp)

    def multiget(self, keys, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100, include_timestamp=False,
                 super_column=None, read_consistency_level = None):
        """
        Fetch multiple key from a Cassandra server
        
        Parameters
        ----------
        keys : [str]
            A list of keys to fetch
        columns : [str]
            Limit the columns or super_columns fetched to the specified list
        column_start : str
            Only fetch when a column or super_column is >= column_start
        column_finish : str
            Only fetch when a column or super_column is <= column_finish
        column_reversed : bool
            Fetch the columns or super_columns in reverse order. This will do
            nothing unless you passed a dict_class to the constructor.
        column_count : int
            Limit the number of columns or super_columns fetched per key
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column
        super_column : str
            Return columns only in this super_column
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation

        Returns
        -------
        if include_timestamp == True: {'key': {'column': ('value', timestamp)}}
        else: {'key': {'column': 'value'}}
        """

        if super_column != '': super_column = self._pack(super_column, is_supercol_name=True)
        if column_start != '': column_start = self._pack(column_start, is_supercol_name=self.super)
        if column_finish != '': column_finish = self._pack(column_finish, is_supercol_name=self.super) 

        packed_cols = None
        if columns is not None:
            packed_cols = []
            for col in columns:
                packed_cols.append(self._pack(col, is_supercol_name=self.super))

        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = create_SlicePredicate(packed_cols, column_start, column_finish,
                                   column_reversed, column_count)

        keymap = self.client.multiget_slice(keys, cp, sp,
                                            self._rcl(read_consistency_level))

        ret = dict()
        for key, columns in keymap.iteritems():
            if len(columns) > 0:
                ret[key] = self._convert_ColumnOrSuperColumns_to_dict_class(columns, include_timestamp)
        return ret

    MAX_COUNT = 2**31-1
    def get_count(self, key, super_column=None, read_consistency_level = None):
        """
        Count the number of columns for a key

        Parameters
        ----------
        key : str
            The key with which to count columns
        super_column : str
            Count the columns only in this super_column
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation

        Returns
        -------
        int Count of columns
        """

        if super_column != '':
            super_column = self._pack(super_column, is_supercol_name=True)

        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = SlicePredicate(slice_range=SliceRange(start='',
                                                   finish='',
                                                   count=self.MAX_COUNT))
        return self.client.get_count(key, cp, sp,
                                     self._rcl(read_consistency_level))

    def get_range(self, start="", finish="", columns=None, column_start="",
                  column_finish="", column_reversed=False, column_count=100,
                  row_count=None, include_timestamp=False,
                  super_column=None, read_consistency_level = None):
        """
        Get an iterator over keys in a specified range
        
        Parameters
        ----------
        start : str
            Start from this key (inclusive)
        finish : str
            End at this key (inclusive)
        columns : [str]
            Limit the columns or super_columns fetched to the specified list
        column_start : str
            Only fetch when a column or super_column is >= column_start
        column_finish : str
            Only fetch when a column or super_column is <= column_finish
        column_reversed : bool
            Fetch the columns or super_columns in reverse order. This will do
            nothing unless you passed a dict_class to the constructor.
        column_count : int
            Limit the number of columns or super_columns fetched per key
        row_count : int
            Limit the number of rows fetched
        include_timestamp : bool
            If true, return a (value, timestamp) tuple for each column
        super_column : string
            Return columns only in this super_column
        read_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any read operation

        Returns
        -------
        iterator over ('key', {'column': 'value'})
        """

        if super_column != '':
            super_column = self._pack(super_column, is_supercol_name=True)
        if column_start != '':
            column_start = self._pack(column_start, is_supercol_name=self.super)
        if column_finish != '':
            column_finish = self._pack(column_finish, is_supercol_name=self.super)
        
        packed_cols = None
        if columns is not None:
            packed_cols = []
            for col in columns:
                packed_cols.append(self._pack(col, is_supercol_name=self.super))

        cp = ColumnParent(column_family=self.column_family, super_column=super_column)
        sp = create_SlicePredicate(packed_cols, column_start, column_finish,
                                   column_reversed, column_count)

        count = 0
        i = 0
        last_key = start
        
        buffer_size = self.buffer_size
        if row_count is not None:
            buffer_size = min(row_count, self.buffer_size)
        while True:
            key_range = KeyRange(start_key=last_key, end_key=finish, count=buffer_size)
            key_slices = self.client.get_range_slices(cp, sp, key_range,
                                                     self._rcl(read_consistency_level))
            # This may happen if nothing was ever inserted
            if key_slices is None:
                return
            for j, key_slice in enumerate(key_slices):
                # Ignore the first element after the first iteration
                # because it will be a duplicate.
                if j == 0 and i != 0:
                    continue
                yield (key_slice.key,
                       self._convert_ColumnOrSuperColumns_to_dict_class(key_slice.columns, include_timestamp))
                count += 1
                if row_count is not None and count >= row_count:
                    return

            if len(key_slices) != self.buffer_size:
                return
            last_key = key_slices[-1].key
            i += 1

    def insert(self, key, columns, write_consistency_level = None):
        """
        Insert or update columns for a key

        Parameters
        ----------
        key : str
            The key to insert or update the columns at
        columns : dict
            Column: {'column': 'value'}
            SuperColumn: {'column': {'subcolumn': 'value'}}
            The columns or supercolumns to insert or update
        write_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any write operation

        Returns
        -------
        int timestamp
        """

        clock = Clock(timestamp=self.timestamp())

        cols = []
        for c, v in columns.iteritems():
            if self.super:
                subc = [Column(name=self._pack(subname), value=subvalue, clock=clock) \
                        for subname, subvalue in v.iteritems()]
                column = SuperColumn(name=self._pack(c, is_supercol_name=True), columns=subc)
                cols.append(Mutation(column_or_supercolumn=ColumnOrSuperColumn(super_column=column)))
            else:
                column = Column(name=self._pack(c), value=v, clock=clock)
                cols.append(Mutation(column_or_supercolumn=ColumnOrSuperColumn(column=column)))
        self.client.batch_mutate({key: {self.column_family: cols}},
                                 self._wcl(write_consistency_level))
        return clock.timestamp

    def remove(self, key, columns=None, super_column=None, write_consistency_level = None):
        """
        Remove a specified key or columns

        Parameters
        ----------
        key : str
            The key to remove. If columns is not set, remove all columns
        columns : list
            Delete the columns or super_columns in this list
        super_column : str
            Delete the columns from this super_column
        write_consistency_level : ConsistencyLevel
            Affects the guaranteed replication factor before returning from
            any write operation

        Returns
        -------
        int timestamp
        """

        packed_cols = None
        if columns is not None:
            packed_cols = []
            for col in columns:
                packed_cols.append(self._pack(col, is_supercol_name = self.super))

        if super_column != '':
            super_column = self._pack(super_column, is_supercol_name=True)

        clock = Clock(timestamp=self.timestamp())
        if packed_cols is not None:
            # Deletion doesn't support SliceRange predicates as of Cassandra 0.6.0,
            # so we can't add column_start, column_finish, etc... yet
            sp = SlicePredicate(column_names=packed_cols)
            deletion = Deletion(clock=clock, super_column=super_column, predicate=sp)
            mutation = Mutation(deletion=deletion)
            self.client.batch_mutate({key: {self.column_family: [mutation]}},
                                     self._wcl(write_consistency_level))
        else:
            cp = ColumnPath(column_family=self.column_family, super_column=super_column)
            self.client.remove(key, cp, clock,
                               self._wcl(write_consistency_level))
        return clock.timestamp
