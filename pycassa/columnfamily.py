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

try:
    from functools import wraps
except ImportError:
    from py25_functools import wraps

import time
import sys
import uuid
import struct
import threading

from batch import CfMutator

if hasattr(struct, 'Struct'): # new in Python 2.5
   _have_struct = True
   _long_packer = struct.Struct('>q')
   _int_packer = struct.Struct('>i')
   _uuid_packer = struct.Struct('>16s')
else:
    _have_struct = False

__all__ = ['gm_timestamp', 'ColumnFamily', 'PooledColumnFamily']

_TYPES = ['BytesType', 'LongType', 'IntegerType', 'UTF8Type', 'AsciiType',
         'LexicalUUIDType', 'TimeUUIDType']

_NON_SLICE = 0
_SLICE_START = 1
_SLICE_FINISH = 2

def gm_timestamp():
    """ Gets the current GMT timestamp as ``int(time.time() * 1e6``. """
    return int(time.time() * 1e6)


class ColumnFamily(object):
    """ An abstraction of a Cassandra column family or super column family. """

    def __init__(self, pool, column_family, buffer_size=1024,
                 read_consistency_level=ConsistencyLevel.ONE,
                 write_consistency_level=ConsistencyLevel.ONE,
                 timestamp=gm_timestamp, super=False,
                 dict_class=OrderedDict, autopack_names=True,
                 autopack_values=True):
        """
        An abstraction of a Cassandra column family or super column family.
        Operations on this, such as :meth:`get` or :meth:`insert` will get data from or
        insert data into the corresponding Cassandra column family with
        name `column_family`.

        `pool` is a :class:`~pycassa.pool.ConnectionPool` that the column
        family will use for all operations.  A connection is drawn from the
        pool before each operations and is returned afterwards.  Note that
        the keyspace to be used is determined by the pool.

        When calling :meth:`get_range()` or :meth:`get_indexed_slices()`,
        the intermediate results need to be buffered if we are fetching many
        rows, otherwise the Cassandra server will overallocate memory and fail.
        `buffer_size` is the size of that buffer in number of rows.  The default
        is 1024.

        `read_consistency_level` and `write_consistency_level` set the default
        consistency levels for every operation; these may be overridden
        per-operation. These should be instances of
        :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`.  These default
        to level ``ONE``.

        Each :meth:`insert()` or :meth:`remove` sends a timestamp with every
        column. The `timestamp` parameter is a function that is used to get
        this timestamp when needed.  The default function is :meth:`gm_timestamp()`.

        Results are returned as dictionaries. :class:`~pycassa.util.OrderedDict` is
        used by default so that order is maintained. A different class, such as
        :class:`dict` may be used instead by passing `dict_class`.

        By default, column family definitions will be examined to determine
        what data type Cassandra expects for column names and values. When
        columns are retrieved or inserted, their names and values will be
        packed or unpacked if necessary to convert them to or from their
        binary representation. Automatic packing of names and values can
        be individually enabled or disabled with `autopack_names` and
        `autopack_values`.  When using :class:`~pycassa.columnfamilymap.ColumnFamilyMap`,
        these should both be set to ``False``.

        """

        self.pool = pool
        self._tlocal = threading.local()
        self._tlocal.client = None
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
            try:
                self._obtain_connection()
                col_fam = self._tlocal.client.get_keyspace_description(use_dict_for_col_metadata=True)[self.column_family]
            except KeyError:
                nfe = NotFoundException()
                nfe.why = 'Column family %s not found.' % self.column_family
                raise nfe
        finally:
            self._release_connection()

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

    def _create_column_path(self, super_column=None, column=None):
        return ColumnPath(self.column_family,
                          self._pack_name(super_column, is_supercol_name=True),
                          self._pack_name(column, False))

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
            if value is not None and not (isinstance(value, str) or isinstance(value, unicode)):
                raise TypeError("A str or unicode column name was expected, but %s was received instead (%s)"
                        % (value.__class__.__name__, str(value)))
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
        elif d_type == 'BytesType' and not (isinstance(value, str) or isinstance(value, unicode)):
            raise TypeError("A str or unicode column name was expected, but %s was received instead (%s)"
                    % (value.__class__.__name__, str(value)))

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
            if value is not None and not (isinstance(value, str) or isinstance(value, unicode)):
                raise TypeError("A str or unicode column value was expected for column '%s', but %s was received instead (%s)"
                        % (str(col_name), value.__class__.__name__, str(value)))
            return value

        d_type = self._get_data_type_for_col(col_name)
        if d_type == 'BytesType' and not (isinstance(value, str) or isinstance(value, unicode)):
            raise TypeError("A str or unicode column value was expected for column '%s', but %s was received instead (%s)"
                    % (str(col_name), value.__class__.__name__, str(value)))

        return self._pack(value, d_type)

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

    def _obtain_connection(self):
        self._tlocal.client = self.pool.get()

    def _release_connection(self):
        if hasattr(self._tlocal, 'client'):
            if self._tlocal.client:
                self._tlocal.client.return_to_pool()
                self._tlocal.client = None

    def get(self, key, columns=None, column_start="", column_finish="",
            column_reversed=False, column_count=100, include_timestamp=False,
            super_column=None, read_consistency_level = None):
        """
        Fetches all or part of the row with key `key`.

        The columns fetched may be limited to a specified list of column names
        using `columns`.

        Alternatively, you may fetch a slice of columns or super columns from a row
        using `column_start`, `column_finish`, and `column_count`.
        Setting these will cause columns or super columns to be fetched starting with
        `column_start`, continuing until `column_count` columns or super columns have
        been fetched or `column_finish` is reached.  If `column_start` is left as the
        empty string, the slice will begin with the start of the row; leaving
        `column_finish` blank will cause the slice to extend to the end of the row.
        Note that `column_count` defaults to 100, so rows over this size will not be
        completely fetched by default.

        If `column_reversed` is ``True``, columns are fetched in reverse sorted order,
        beginning with `column_start`.  In this case, if `column_start` is the empty
        string, the slice will begin with the end of the row.

        You may fetch all or part of only a single super column by setting `super_column`.
        If this is set, `column_start`, `column_finish`, `column_count`, and `column_reversed`
        will apply to the subcolumns of `super_column`.

        To include every column's timestamp in the result set, set `include_timestamp` to
        ``True``.  Results will include a ``(value, timestamp)`` tuple for each column.

        If this is a standard column family, the return type is of the form
        ``{column_name: column_value}``.  If this is a super column family and `super_column`
        is not specified, the results are of the form
        ``{super_column_name: {column_name, column_value}}``.  If `super_column` is set,
        the super column name will be excluded and the results are of the form
        ``{column_name: column_value}``.

        """

        single_column = columns is not None and len(columns) == 1
        if (not self.super and single_column) or \
           (self.super and super_column is not None and single_column):
            super_col_orig = super_column is not None
            column = None
            if self.super and super_column is None:
                super_column = columns[0]
            else:
                column = columns[0]
            cp = self._create_column_path(super_column, column)
            try:
                self._obtain_connection()
                col_or_super = self._tlocal.client.get(key, cp, self._rcl(read_consistency_level))
            finally:
                self._release_connection()
            return self._convert_ColumnOrSuperColumns_to_dict_class([col_or_super], include_timestamp)
        else:
            cp = self._create_column_parent(super_column)
            sp = self._create_slice_predicate(columns, column_start, column_finish,
                                        column_reversed, column_count)

            try:
                self._obtain_connection()
                list_col_or_super = self._tlocal.client.get_slice(key, cp, sp,
                                                              self._rcl(read_consistency_level))
            finally:
                self._release_connection()

            if len(list_col_or_super) == 0:
                raise NotFoundException()
            return self._convert_ColumnOrSuperColumns_to_dict_class(list_col_or_super, include_timestamp)

    def get_indexed_slices(self, index_clause, columns=None, column_start="", column_finish="",
                           column_reversed=False, column_count=100, include_timestamp=False,
                           super_column=None, read_consistency_level=None,
                           buffer_size=None):
        """
        Similar to :meth:`get_range()`, but an :class:`~pycassa.cassandra.ttypes.IndexClause`
        is used instead of a key range.

        `index_clause` limits the keys that are returned based on expressions
        that compare the value of a column to a given value.  At least one of the
        expressions in the :class:`.IndexClause` must be on an indexed column.

            .. seealso:: :meth:`~pycassa.index.create_index_clause()` and
                         :meth:`~pycassa.index.create_index_expression()`

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
            try:
                self._obtain_connection()
                key_slices = self._tlocal.client.get_indexed_slices(cp, clause, sp,
                                                            self._rcl(read_consistency_level))
            finally:
                self._release_connection()

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

        All parameters are the same as :meth:`get()`, except that a list of keys may
        be passed in. Results will be returned in the form:
        ``{key: {column_name: column_value}}``

        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                          column_reversed, column_count)

        try:
            self._obtain_connection()
            keymap = self._tlocal.client.multiget_slice(keys, cp, sp,
                                                self._rcl(read_consistency_level))
        finally:
            self._release_connection()

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
        Count the number of columns in the row with key `key`.

        You may limit the columns or super columns counted to those in `columns`.
        Additionally, you may limit the columns or super columns counted to
        only those between `column_start` and `column_finish`.

        You may also count only the number of subcolumns in a single super column
        using `super_column`.  If this is set, `columns`, `column_start`, and
        `column_finish` only apply to the subcolumns of `super_column`.

        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                          False, self.MAX_COUNT)

        try:
            self._obtain_connection()
            ret = self._tlocal.client.get_count(key, cp, sp,
                                         self._rcl(read_consistency_level))
        finally:
            self._release_connection()
        return ret

    def multiget_count(self, keys, super_column=None,
                       read_consistency_level=None,
                       columns=None, column_start="",
                       column_finish="", ):
        """
        Perform a column count in parallel on a set of rows.

        The parameters are the same as for :meth:`get()`, except that a list
        of keys may be used. A dictionary of the form ``{key: int}`` is
        returned.

        """

        cp = self._create_column_parent(super_column)
        sp = self._create_slice_predicate(columns, column_start, column_finish,
                                          False, self.MAX_COUNT)

        try:
            self._obtain_connection()
            ret = self._tlocal.client.multiget_count(keys, cp, sp,
                                         self._rcl(read_consistency_level))
        finally:
            self._release_connection()
        return ret

    def get_range(self, start="", finish="", columns=None, column_start="",
                  column_finish="", column_reversed=False, column_count=100,
                  row_count=None, include_timestamp=False,
                  super_column=None, read_consistency_level=None,
                  buffer_size=None):
        """
        Get an iterator over rows in a specified key range.

        The key range begins with `start` and ends with `finish`. If left
        as empty strings, these extend to the beginning and end, respectively.
        Note that if RandomPartitioner is used, rows are stored in the
        order of the MD5 hash of their keys, so getting a lexicographical range
        of keys is not feasible.

        The `row_count` parameter limits the total number of rows that may be
        returned. If left as ``None``, the number of rows that may be returned
        is unlimted (this is the default).

        When calling `get_range()`, the intermediate results need to be
        buffered if we are fetching many rows, otherwise the Cassandra
        server will overallocate memory and fail.  `buffer_size` is the size of
        that buffer in number of rows. If left as ``None``, the
        ColumnFamily's `buffer_size` attribute will be used.

        All other parameters are the same as those of :meth:`get()`.

        A generator over ``(key, {column_name: column_value})`` is returned.
        To convert this to a list, use ``list()`` on the result.

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
            try:
                self._obtain_connection()
                key_slices = self._tlocal.client.get_range_slices(cp, sp, key_range,
                                                         self._rcl(read_consistency_level))
            finally:
                self._release_connection()
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
        Insert or update columns in the row with key `key`.

        `columns` should be a dictionary of columns or super columns to insert
        or update.  If this is a standard column family, `columns` should
        look like ``{column_name: column_value}``.  If this is a super
        column family, `columns` should look like
        ``{super_column_name: {sub_column_name: value}}``

        A timestamp may be supplied for all inserted columns with `timestamp`.

        `ttl` sets the "time to live" in number of seconds for the inserted
        columns. After this many seconds, Cassandra will mark the columns as
        deleted.

        The timestamp Cassandra reports as being used for insert is returned.

        """
        if ((not self.super) and len(columns) == 1) or \
           (self.super and len(columns) == 1 and len(columns.values()[0]) == 1):

            if timestamp is None:
                timestamp = self.timestamp()

            if self.super:
                super_col = columns.keys()[0]
                cp = self._create_column_path(super_col)
                columns = columns.values()[0]
            else:
                cp = self._create_column_path()

            colname = columns.keys()[0]
            colval = self._pack_value(columns.values()[0], colname)
            colname = self._pack_name(colname, False)
            column = Column(colname, colval, timestamp, ttl)
            try:
                self._obtain_connection()
                res = self._tlocal.client.insert(key, cp, column, self._wcl(write_consistency_level))
            finally:
                self._release_connection()
            return res
        else:
            return self.batch_insert({key: columns}, timestamp=timestamp, ttl=ttl,
                                     write_consistency_level=write_consistency_level)

    def batch_insert(self, rows, timestamp=None, ttl=None, write_consistency_level = None):
        """
        Like :meth:`insert()`, but multiple rows may be inserted at once.

        The `rows` parameter should be of the form ``{key: {column_name: column_value}}``
        if this is a standard column family or ``{key: {column_name: column_value}}`` if
        this is a super column family.

        """

        if timestamp == None:
            timestamp = self.timestamp()
        batch = self.batch(write_consistency_level=write_consistency_level)
        for key, columns in rows.iteritems():
            batch.insert(key, columns, timestamp=timestamp, ttl=ttl)
        batch.send()
        return timestamp

    def remove(self, key, columns=None, super_column=None,
               write_consistency_level=None, timestamp=None):
        """
        Remove a specified row or a set of columns within the row with key `key`.

        A set of columns or super columns to delete may be specified using
        `columns`. If `columns` is ``None``, the entire row is removed.

        A single super column may be deleted by setting `super_column`. If
        `super_column` is specified, `columns` will apply to the subcolumns
        of `super_column`.

        If `columns` and `super_column` are both ``None``, the entire row is
        removed.

        The timestamp used for remove is returned.

        """

        if timestamp is None:
            timestamp = self.timestamp()
        batch = self.batch(write_consistency_level=write_consistency_level)
        batch.remove(key, columns, super_column, timestamp)
        batch.send()
        return timestamp

    def batch(self, queue_size=100, write_consistency_level=None):
        """
        Create batch mutator for doing multiple insert, update, and remove
        operations using as few roundtrips as possible.

        The `queue_size` parameter sets the max number of mutations per request.

        A :class:`~pycassa.batch.CfMutator` is returned.

        """

        return CfMutator(self, queue_size, self._wcl(write_consistency_level))

    def truncate(self):
        """
        Marks the entire ColumnFamily as deleted.

        From the user's perspective, a successful call to ``truncate`` will
        result complete data deletion from this column family. Internally,
        however, disk space will not be immediatily released, as with all
        deletes in Cassandra, this one only marks the data as deleted.

        The operation succeeds only if all hosts in the cluster at available
        and will throw an :exc:`.UnavailableException` if some hosts are
        down.

        """
        try:
            self._obtain_connection()
            self._tlocal.client.truncate(self.column_family)
        finally:
            self._release_connection()

PooledColumnFamily = ColumnFamily
