"""
Provides an abstraction of Cassandra's data model to allow for easy
manipulation of data inside Cassandra.

.. seealso:: :mod:`pycassa.columnfamilymap`
"""

from pycassa.cassandra.ttypes import Column, ColumnOrSuperColumn,\
    ColumnParent, ColumnPath, ConsistencyLevel, NotFoundException,\
    SlicePredicate, SliceRange, SuperColumn, KeyRange,\
    IndexExpression, IndexClause
from pycassa.util import *

import time
import sys
import uuid
import struct

from batch import CfMutator

if hasattr(struct, 'Struct'): # new in Python 2.5
   _have_struct = True
   _long_packer = struct.Struct('>q')
   _int_packer = struct.Struct('>i')
   _uuid_packer = struct.Struct('>16s')
else:
    _have_struct = False

__all__ = ['gm_timestamp', 'ColumnFamily']

_TYPES = ['BytesType', 'LongType', 'IntegerType', 'UTF8Type', 'AsciiType',
         'LexicalUUIDType', 'TimeUUIDType']

_NON_SLICE = 0
_SLICE_START = 1
_SLICE_FINISH = 2

def gm_timestamp():
    """
    Gets the current GMT timestamp

    :Returns:
        integer UNIX epoch time in GMT

    """
    return int(time.time() * 1e6)

class ColumnFamily(object):
    """ An abstraction of a Cassandra column family or super column family. """

    def __init__(self, client, column_family, buffer_size=1024,
                 read_consistency_level=ConsistencyLevel.ONE,
                 write_consistency_level=ConsistencyLevel.ONE,
                 timestamp=gm_timestamp, super=False,
                 dict_class=dict, autopack_names=True,
                 autopack_values=True):
        """
        Constructs an abstraction of a Cassandra column family or super column family.

        Operations on this, such as :meth:`get` or :meth:`insert` will get data from or
        insert data into the corresponding Cassandra column family.

        :param client: Connection to a Cassandra node
        :type client: :class:`~pycassa.connection.Connection`

        :param column_family: The name of the column family
        :type column_family: string

        :param buffer_size: When calling :meth:`get_range()`, the
          intermediate results need to be buffered if we are fetching
          many rows, otherwise the Cassandra server will overallocate
          memory and fail.  This is the size of that buffer in number
          of rows.
        :type buffer_size: int

        :param read_consistency_level: Affects the guaranteed replication factor
          before returning from any read operation
        :type read_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :param write_consistency_level: Affects the guaranteed replication
          factor before returning from any write operation
        :type write_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :param timestamp:
          The default timestamp function returns
          ``int(time.mktime(time.gmtime()))``,
          the number of seconds since Unix epoch in GMT.
          Set this to replace the default timestamp function with your own.
        :type timestamp: function

        :param dict_class: The default dict_class is :class:`dict`.
          If the order of columns matter to you, pass your own dictionary
          class, or python 2.7's new :class:`collections.OrderedDict`. All returned
          rows and subcolumns are instances of this.
        :type dict_class: class

        :param autopack_names: Whether column and supercolumn names should
          be packed automatically based on the comparator and subcomparator
          for the column family.  This does not typically work when used with
          :class:`~pycassa.columnfamilymap.ColumnFamilyMap`.
        :type autopack_names: bool

        :param autopack_values: Whether column values should be packed
          automatically based on the validator_class for a given column.
          This should probably be set to ``False`` when used with a
          :class:`~pycassa.columnfamilymap.ColumnFamilyMap`.
        :type autopack_values: bool

        :param super: Whether this column family has super columns. This
          is detected automatically since 0.5.1.

          .. deprecated:: 0.5.1

        :type super: bool

        """

        self.client = client
        self.column_family = column_family
        self.buffer_size = buffer_size
        self.read_consistency_level = read_consistency_level
        self.write_consistency_level = write_consistency_level
        self.timestamp = timestamp
        self.dict_class = dict_class
        self.autopack_names = autopack_names
        self.autopack_values = autopack_values

        # Determine the ColumnFamily type to allow for auto conversion
        # so that packing/unpacking doesn't need to be done manually
        self.cf_data_type = None
        self.col_name_data_type = None
        self.supercol_name_data_type = None
        self.col_type_dict = dict()

        col_fam = None
        try:
            col_fam = client.get_keyspace_description()[self.column_family]
        except KeyError:
            raise NotFoundException('Column family %s not found.' % self.column_family)

        if col_fam is not None:
            self.super = col_fam.column_type == 'Super'
            if self.autopack_names:
                if not self.super:
                    self.col_name_data_type = col_fam.comparator_type
                else:
                    self.col_name_data_type = col_fam.subcomparator_type
                    self.supercol_name_data_type = self._extract_type_name(col_fam.comparator_type)

                index = self.col_name_data_type = self._extract_type_name(self.col_name_data_type)
            if self.autopack_values:
                self.cf_data_type = self._extract_type_name(col_fam.default_validation_class)
                for name, cdef in col_fam.column_metadata.items():
                    self.col_type_dict[name] = self._extract_type_name(cdef.validation_class)

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
        value = self._unpack_value(column.value, column.name)
        if include_timestamp:
            return (value, column.timestamp)
        return value

    def _convert_SuperColumn_to_base(self, super_column, include_timestamp):
        ret = self.dict_class()
        for column in super_column.columns:
            ret[self._unpack_name(column.name)] = self._convert_Column_to_base(column, include_timestamp)
        return ret

    def _convert_ColumnOrSuperColumns_to_dict_class(self, list_col_or_super, include_timestamp):
        ret = self.dict_class()
        for col_or_super in list_col_or_super:
            if col_or_super.super_column is not None:
                col = col_or_super.super_column
                ret[self._unpack_name(col.name, is_supercol_name=True)] = self._convert_SuperColumn_to_base(col, include_timestamp)
            else:
                col = col_or_super.column
                ret[self._unpack_name(col.name)] = self._convert_Column_to_base(col, include_timestamp)
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

    def _create_column_parent(self, super_column=None):
        return ColumnParent(column_family=self.column_family,
                            super_column=self._pack_name(super_column, is_supercol_name=True))

    def _create_slice_predicate(self, columns, column_start, column_finish,
                                      column_reversed, column_count):
        if columns is not None:
            packed_cols = []
            for col in columns:
                packed_cols.append(self._pack_name(col, is_supercol_name=self.super))
            return SlicePredicate(column_names=packed_cols)
        else:
            if column_start != '':
                column_start = self._pack_name(column_start,
                                               is_supercol_name=self.super,
                                               slice_end=_SLICE_START)
            if column_finish != '':
                column_finish = self._pack_name(column_finish,
                                                is_supercol_name=self.super,
                                                slice_end=_SLICE_FINISH)
            sr = SliceRange(start=column_start, finish=column_finish,
                            reversed=column_reversed, count=column_count)
            return SlicePredicate(slice_range=sr)

    def _pack_name(self, value, is_supercol_name=False,
            slice_end=_NON_SLICE):
        if not self.autopack_names:
            return value
        if value is None: return

        if is_supercol_name:
            d_type = self.supercol_name_data_type
        else:
            d_type = self.col_name_data_type

        if d_type == 'TimeUUIDType':
            if slice_end:
                value = convert_time_to_uuid(value,
                        lowest_val=(slice_end == _SLICE_START),
                        randomize=False)
            else:
                value = convert_time_to_uuid(value,
                        randomize=True)

        return self._pack(value, d_type)

    def _unpack_name(self, b, is_supercol_name=False):
        if not self.autopack_names:
            return b
        if b is None: return

        if is_supercol_name:
            d_type = self.supercol_name_data_type
        else:
            d_type = self.col_name_data_type

        return self._unpack(b, d_type)

    def _get_data_type_for_col(self, col_name):
        if col_name not in self.col_type_dict.keys(): 
            return self.cf_data_type
        return self.col_type_dict[col_name]

    def _pack_value(self, value, col_name):
        if not self.autopack_values:
            return value
        return self._pack(value, self._get_data_type_for_col(col_name))

    def _unpack_value(self, value, col_name):
        if not self.autopack_values:
            return value
        return self._unpack(value, self._get_data_type_for_col(col_name))

    def _pack(self, value, data_type):
        """
        Packs a value into the expected sequence of bytes that Cassandra expects.
        """
        if data_type == 'LongType':
            if _have_struct:
                return _long_packer.pack(long(value))
            else:
                return struct.pack('>q', long(value))  # q is 'long long'
        elif data_type == 'IntegerType':
            if _have_struct:
                return _int_packer.pack(int(value))
            else:
                return struct.pack('>i', int(value))
        elif data_type == 'AsciiType':
            return struct.pack(">%ds" % len(value), value)
        elif data_type == 'UTF8Type':
            try:
                st = value.encode('utf-8')
            except UnicodeDecodeError:
                # value is already utf-8 encoded
                st = value
            return struct.pack(">%ds" % len(st), st)
        elif data_type == 'TimeUUIDType' or data_type == 'LexicalUUIDType':
            if not hasattr(value, 'bytes'):
                raise TypeError("%s not valid for %s" % (value, data_type))
            if _have_struct:
                return _uuid_packer.pack(value.bytes)
            else:
                return struct.pack('>16s', value.bytes)
        else:
            return value

    def _unpack(self, b, data_type):
        """
        Unpacks Cassandra's byte-representation of values into their Python
        equivalents.
        """

        if data_type == 'LongType':
            if _have_struct:
                return _long_packer.unpack(b)[0]
            else:
                return struct.unpack('>q', b)[0]
        elif data_type == 'IntegerType':
            if _have_struct:
                return _int_packer.unpack(b)[0]
            else:
                return struct.unpack('>i', b)[0]
        elif data_type == 'AsciiType':
            return struct.unpack('>%ds' % len(b), b)[0]
        elif data_type == 'UTF8Type':
            unic = struct.unpack('>%ds' % len(b), b)[0]
            return unic.decode('utf-8')
        elif data_type == 'LexicalUUIDType' or data_type == 'TimeUUIDType':
            if _have_struct:
                temp_bytes = _uuid_packer.unpack(b)[0]
            else:
                temp_bytes = struct.unpack('>16s', b)[0]
            return uuid.UUID(bytes=temp_bytes)
        else: # BytesType
            return b

    def get(self, key, columns=None, column_start="", column_finish="",
            column_reversed=False, column_count=100, include_timestamp=False,
            super_column=None, read_consistency_level = None):
        """
        Fetches a row in this column family.

        :param key: Fetch the row with this key
        :type key: str

        :param columns: Limit the columns or super columns fetched to the specified list
        :type columns: list

        :param column_start: Only fetch columns or super columns ``>= column_start``

        :param column_finish: Only fetch columns or super columns ``<= column_finish``

        :param column_reversed:
          Fetch the columns or super_columns in reverse order. If `column_count` is
          used with this, columns will be drawn from the end. The returned dictionary
          of columns may not be in reversed order if an ordered ``dict_class`` is not
          passed to the constructor.
        :type column_reversed: bool

        :param column_count: Limit the number of columns or super columns fetched per row
        :type column_count: int

        :param include_timestamp: If True, return a ``(value, timestamp)`` tuple for each column
        :type include_timestamp: bool

        :param super_column: Return columns only in this super column

        :param read_consistency_level: Affects the guaranteed replication factor before
          returning from any read operation
        :type read_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: ``{column_name: column_value}``

        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                    column_reversed, column_count)

        list_col_or_super = self.client.get_slice(key, cp, sp,
                                                  self._rcl(read_consistency_level))

        if len(list_col_or_super) == 0:
            raise NotFoundException()
        return self._convert_ColumnOrSuperColumns_to_dict_class(list_col_or_super, include_timestamp)

    def get_indexed_slices(self, index_clause, columns=None, column_start="", column_finish="",
                           column_reversed=False, column_count=100, include_timestamp=False,
                           super_column=None, read_consistency_level=None,
                           buffer_size=None):
        """
        Fetches a set of rows from this column family based on an index clause.

        :param index_clause: Limits the keys that are returned based on expressions
          that compare the value of a column to a given value.  At least one of the
          expressions in the :class:`.IndexClause` must be on an indexed column.

            .. seealso:: :meth:`~pycassa.index.create_index_clause()` and
                         :meth:`~pycassa.index.create_index_expression()`

        :type index_clause: :class:`~pycassa.cassandra.ttypes.IndexClause`

        :param columns: Limit the columns or super columns fetched to the specified list
        :type columns: list

        :param column_start: Only fetch columns or super columns ``>= column_start``

        :param column_finish: Only fetch columns or super columns ``<= column_finish``

        :param column_reversed:
          Fetch the columns or super_columns in reverse order. If `column_count` is
          used with this, columns will be drawn from the end. The returned dictionary
          of columns may not be in reversed order if an ordered ``dict_class`` is not
          passed to the constructor.
        :type column_reversed: bool

        :param column_count: Limit the number of columns or super columns fetched per row

        :param include_timestamp: If True, return a ``(value, timestamp)`` tuple for each column
        :type include_timestamp: bool

        :param super_column: Return columns only in this super column

        :param read_consistency_level: Affects the guaranteed replication factor before
          returning from any read operation
        :type read_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :param buffer_size: When calling `get_indexed_slices()`, the intermediate
          results need to be buffered if we are fetching many rows, otherwise the Cassandra
          server will overallocate memory and fail.  This is the size of
          that buffer in number of rows. If left as ``None``,
          the :class:`~pycassa.cassandra.ColumnFamily`'s default
          `buffer_size` will be used.
        :type buffer_size: int

        :rtype: Generator that iterates over:
                ``{key : {column_name : column_value}}``
        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                    column_reversed, column_count)

        new_exprs = []
        # Pack the values in the index clause expressions
        for expr in index_clause.expressions:
            new_exprs.append(IndexExpression(self._pack_name(expr.column_name),
                                             expr.op,
                                             self._pack_value(expr.value, expr.column_name)))

        clause = IndexClause(new_exprs, index_clause.start_key, index_clause.count)

        # Figure out how we will chunk the request
        if buffer_size is None:
            buffer_size = self.buffer_size
        row_count = clause.count

        count = 0
        i = 0
        last_key = clause.start_key
        while True:
            if row_count is not None:
                buffer_size = min(row_count - count + 1, buffer_size)
            clause.count = buffer_size
            clause.start_key = last_key
            key_slices = self.client.get_indexed_slices(cp, clause, sp,
                                                        self._rcl(read_consistency_level))

            if key_slices is None:
                return
            for j, key_slice in enumerate(key_slices):
                # Ignore the first element after the first iteration
                # because it will be a duplicate.
                if j == 0 and i != 0:
                    continue
                yield (key_slice.key, self._convert_ColumnOrSuperColumns_to_dict_class(
                        key_slice.columns, include_timestamp))

                count += 1
                if row_count is not None and count >= row_count:
                    return

            if len(key_slices) != buffer_size:
                return
            last_key = key_slices[-1].key
            i += 1

    def multiget(self, keys, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100, include_timestamp=False,
                 super_column=None, read_consistency_level = None):
        """
        Fetch multiple rows from a Cassandra server.

        :param keys: Fetch the row with this key
        :type keys: list of str

        :param columns: Limit the columns or super columns fetched to the specified list
        :type columns: list

        :param column_start: Only fetch columns or super columns ``>= column_start``

        :param column_finish: Only fetch columns or super columns ``<= column_finish``

        :param column_reversed:
          Fetch the columns or super_columns in reverse order. If `column_count` is
          used with this, columns will be drawn from the end. The returned dictionary
          of columns may not be in reversed order if an ordered ``dict_class`` is not
          passed to the constructor.
        :type column_reversed: bool

        :param column_count: Limit the number of columns or super columns fetched per row
        :type column_count: int

        :param include_timestamp: If True, return a ``(value, timestamp)`` tuple for each column
        :type include_timestamp: bool

        :param super_column: Return columns only in this super column

        :param read_consistency_level: Affects the guaranteed replication factor before
          returning from any read operation
        :type read_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: ``{key: {column_name: column_value}}``

        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                          column_reversed, column_count)

        keymap = self.client.multiget_slice(keys, cp, sp,
                                            self._rcl(read_consistency_level))
        
        ret = self.dict_class()
        
        # Keep the order of keys
        for key in keys:
            ret[key] = None

        non_empty_keys = []
        for key, columns in keymap.iteritems():
            if len(columns) > 0:
                non_empty_keys.append(key)
                ret[key] = self._convert_ColumnOrSuperColumns_to_dict_class(columns, include_timestamp) 

        for key in keys:
            if key not in non_empty_keys:
                del ret[key]

        return ret

    MAX_COUNT = 2**31-1
    def get_count(self, key, super_column=None, read_consistency_level=None,
                  columns=None, column_start="", column_finish=""):
        """
        Count the number of columns in a row.

        :param key: Count the columns in the row with this key

        :param columns: Limit the columns or super columns counted to this list
        :type columns: list

        :param column_start: Only count columns or super columns ``>= column_start``

        :param column_finish: Only count columns or super columns ``>= column_finish``

        :param super_column: Only count the columns in this super column

        :param read_consistency_level: Affects the guaranteed replication factor before
          returning from any read operation
        :type read_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: int

        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                          False, self.MAX_COUNT)

        return self.client.get_count(key, cp, sp,
                                     self._rcl(read_consistency_level))

    def multiget_count(self, keys, super_column=None,
                       read_consistency_level=None,
                       columns=None, column_start="",
                       column_finish="", ):
        """
        Perform a get_count in parallel on a set of rows.

        :param keys: Count the columns in the rows with these keys
        :type keys: list

        :param columns: Limit the columns or super columns counted to this list
        :type columns: list

        :param column_start: Only count columns or super columns ``>= column_start``

        :param column_finish: Only count columns or super columns ``>= column_finish``

        :param super_column: Only count the columns in this super column

        :param read_consistency_level: Affects the guaranteed replication factor before
          returning from any read operation
        :type read_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: ``{key: int}``
        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                          False, self.MAX_COUNT)

        return self.client.multiget_count(keys, cp, sp,
                                     self._rcl(read_consistency_level))


    def get_range(self, start="", finish="", columns=None, column_start="",
                  column_finish="", column_reversed=False, column_count=100,
                  row_count=None, include_timestamp=False,
                  super_column=None, read_consistency_level=None,
                  buffer_size=None):
        """
        Get an iterator over rows in a specified key range.

        :param start: Start from this key (inclusive)
        :type start: str

        :param finish: End at this key (inclusive)
        :type finish: str

        :param columns: Limit the columns or super columns fetched to the specified list
        :type columns: list

        :param column_start: Only fetch columns or super columns ``>= column_start``

        :param column_finish: Only fetch columns or super columns ``<= column_finish``

        :param column_reversed:
          Fetch the columns or super_columns in reverse order. If `column_count` is
          used with this, columns will be drawn from the end. The returned dictionary
          of columns may not be in reversed order if an ordered ``dict_class`` is not
          passed to the constructor.
        :type column_reversed: bool

        :param column_count: Limit the number of columns or super columns fetched per row
        :type column_count: int

        :param include_timestamp: If True, return a ``(value, timestamp)`` tuple for each column
        :type include_timestamp: bool

        :param super_column: Return columns only in this super column

        :param read_consistency_level: Affects the guaranteed replication factor before
          returning from any read operation
        :type read_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :param buffer_size: When calling `get_range()`, the intermediate results need to be
          buffered if we are fetching many rows, otherwise the Cassandra
          server will overallocate memory and fail.  This is the size of
          that buffer in number of rows. If left as None, the ColumnFamily's default
          `buffer_size` will be used.
        :type buffer_size: int

        :rtype: Generator over ``(key, {column_name: column_value})``

        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                          column_reversed, column_count)

        count = 0
        i = 0
        last_key = start

        if buffer_size is None:
            buffer_size = self.buffer_size
        while True:
            if row_count is not None:
                buffer_size = min(row_count - count + 1, buffer_size)
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

            if len(key_slices) != buffer_size:
                return
            last_key = key_slices[-1].key
            i += 1

    def insert(self, key, columns, timestamp=None, ttl=None,
               write_consistency_level=None):
        """
        Insert or update columns in a row.

        :param key: Insert or update the columns in the row with this key
        :type key: str

        :type columns: The columns or supercolumns to insert or update
          Column: ``{column_name: column_value}``
          SuperColumn: ``{super_column_name: {sub_column_name: value}}``
        :type columns: dict

        :param write_consistency_level: Affects the guaranteed replication factor
          before returning from any write operation
        :type write_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: int timestamp

        """
        return self.batch_insert({key: columns}, timestamp=timestamp, ttl=ttl,
                                 write_consistency_level=write_consistency_level)

    def batch_insert(self, rows, timestamp=None, ttl=None, write_consistency_level = None):
        """
        Insert or update columns for multiple rows.

        :param rows: 
           Standard Column Family: ``{row_key: {column_name: column_value}}``
           Super Column Family: ``{row_key: {super_column_name: {sub_column_name: value}}}``
        :type rows: :class:`dict`

        :param write_consistency_level: Affects the guaranteed replication factor
          before returning from any write operation
        :type write_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: int timestamp

        """

        if timestamp == None:
            timestamp = self.timestamp()
        batch = self.batch(write_consistency_level=write_consistency_level)
        for key, columns in rows.iteritems():
            batch.insert(key, columns, timestamp=timestamp, ttl=ttl)
        batch.send()
        return timestamp

    def remove(self, key, columns=None, super_column=None, write_consistency_level=None):
        """
        Remove a specified row or a set of columns within a row.

        :param key: Remove the row with this key. If ``columns`` is
          ``None``, remove all columns
        :type key: str

        :param columns: Delete only the columns or super columns in this list
        :type columns: list

        :param super_column: Delete the columns from this super column

        :param write_consistency_level: Affects the guaranteed replication factor
          before returning from any write operation
        :type write_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: int timestamp

        """

        timestamp = self.timestamp()
        batch = self.batch(write_consistency_level=write_consistency_level)
        batch.remove(key, columns, super_column, timestamp)
        batch.send()
        return timestamp

    def batch(self, queue_size=100, write_consistency_level=None):
        """
        Create batch mutator for doing multiple insert, update, and remove
        operations using as few roundtrips as possible.

        :param queue_size: Max number of mutations per request
        :type queue_size: int

        :param write_consistency_level: Affects the guaranteed replication factor
          before returning from any write operation
        :type write_consistency_level: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`

        :rtype: :class:`~pycassa.batch.CfMutator`

        """

        return CfMutator(self, queue_size, self._wcl(write_consistency_level))

    def truncate(self):
        """
        Marks the entire ColumnFamily as deleted.

        From the user's perspective a successful call to truncate will result
        complete data deletion from cfname. Internally, however, disk space
        will not be immediatily released, as with all deletes in cassandra,
        this one only marks the data as deleted.

        The operation succeeds only if all hosts in the cluster at available
        and will throw an :exc:`.UnavailableException` if some hosts are
        down.

        """
        self.client.truncate(self.column_family)
