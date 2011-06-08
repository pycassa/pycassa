"""
Provides an abstraction of Cassandra's data model to allow for easy
manipulation of data inside Cassandra.

.. seealso:: :mod:`pycassa.columnfamilymap`
"""

from pycassa.cassandra.ttypes import Column, ColumnOrSuperColumn,\
    ColumnParent, ColumnPath, ConsistencyLevel, NotFoundException,\
    SlicePredicate, SliceRange, SuperColumn, KeyRange,\
    IndexExpression, IndexClause, CounterColumn
import pycassa.util as util

import time
import sys
import uuid
import threading

from batch import CfMutator

__all__ = ['gm_timestamp', 'ColumnFamily', 'PooledColumnFamily']

_NON_SLICE = 0
_SLICE_START = 1
_SLICE_FINISH = 2

def gm_timestamp():
    """ Gets the current GMT timestamp in microseconds. """
    return int(time.time() * 1e6)


class ColumnFamily(object):
    """ An abstraction of a Cassandra column family or super column family. """

    def __init__(self, pool, column_family, buffer_size=1024,
                 read_consistency_level=ConsistencyLevel.ONE,
                 write_consistency_level=ConsistencyLevel.ONE,
                 timestamp=gm_timestamp, super=False,
                 dict_class=util.OrderedDict, autopack_names=True,
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

        # Determine the ColumnFamily type to allow for auto conversion
        # so that packing/unpacking doesn't need to be done manually
        self.cf_data_type = None
        self.col_name_data_type = None
        self.supercol_name_data_type = None
        self.key_type = None
        self.col_type_dict = dict()

        self.cfdef = None
        try:
            try:
                self._obtain_connection()
                ksdef = self._tlocal.client.get_keyspace_description(use_dict_for_col_metadata=True)
                self.cfdef = ksdef[self.column_family]
            except KeyError:
                nfe = NotFoundException()
                nfe.why = 'Column family %s not found.' % self.column_family
                raise nfe
        finally:
            self._release_connection()

        self.super = self.cfdef.column_type == 'Super'
        self._set_autopack_names(autopack_names)
        self._set_autopack_values(autopack_values)
        self._set_autopack_keys(True)

    def _set_autopack_names(self, autopack):
        if autopack:
            self._autopack_names = True
            if not self.super:
                self.col_name_data_type = util.extract_type_name(self.cfdef.comparator_type)
            else:
                self.col_name_data_type = util.extract_type_name(self.cfdef.subcomparator_type)
                self.supercol_name_data_type = util.extract_type_name(self.cfdef.comparator_type)
        else:
            self._autopack_names = False

    def _get_autopack_names(self):
        return self._autopack_names

    autopack_names = property(_get_autopack_names, _set_autopack_names)

    def _set_autopack_values(self, autopack):
        if autopack:
            self._autopack_values = True
            self.cf_data_type = util.extract_type_name(self.cfdef.default_validation_class)
            for name, coldef in self.cfdef.column_metadata.items():
                self.col_type_dict[name] = util.extract_type_name(coldef.validation_class)
        else:
            self._autopack_values = False

    def _get_autopack_values(self):
        return self._autopack_values

    autopack_values = property(_get_autopack_values, _set_autopack_values)

    def _set_autopack_keys(self, autopack):
        if autopack:
            self._autopack_keys = True
            if hasattr(self.cfdef, "key_validation_class"):
                self.key_type = util.extract_type_name(self.cfdef.key_validation_class)
        else:
            self._autopack_keys = False

    def _get_autopack_keys(self):
        return self._autopack_keys

    autopack_keys = property(_get_autopack_keys, _set_autopack_keys)

    def _col_to_dict(self, column, include_timestamp):
        value = self._unpack_value(column.value, column.name)
        if include_timestamp:
            return (value, column.timestamp)
        return value

    def _scol_to_dict(self, super_column, include_timestamp):
        ret = self.dict_class()
        for column in super_column.columns:
            ret[self._unpack_name(column.name)] = self._col_to_dict(column, include_timestamp)
        return ret

    def _scounter_to_dict(self, counter_super_column):
        ret = self.dict_class()
        for counter in counter_super_column.columns:
            ret[self._unpack_name(counter.name)] = counter.value
        return ret

    def _cosc_to_dict(self, list_col_or_super, include_timestamp):
        ret = self.dict_class()
        for cosc in list_col_or_super:
            if cosc.column:
                col = cosc.column
                ret[self._unpack_name(col.name)] = self._col_to_dict(col, include_timestamp)
            elif cosc.counter_column:
                counter = cosc.counter_column
                ret[self._unpack_name(counter.name)] = counter.value
            elif cosc.super_column:
                scol = cosc.super_column
                ret[self._unpack_name(scol.name, True)] = self._scol_to_dict(scol, include_timestamp)
            else:
                scounter = cosc.counter_super_column
                ret[self._unpack_name(scounter.name, True)] = self._scounter_to_dict(scol, False)
        return ret

    def _column_path(self, super_column=None, column=None):
        return ColumnPath(self.column_family,
                          self._pack_name(super_column, is_supercol_name=True),
                          self._pack_name(column, False))

    def _column_parent(self, super_column=None):
        return ColumnParent(column_family=self.column_family,
                            super_column=self._pack_name(super_column, is_supercol_name=True))

    def _slice_predicate(self, columns, column_start, column_finish,
                         column_reversed, column_count, super_column=None):
        is_supercol_name = self.super and super_column is None
        if columns is not None:
            packed_cols = []
            for col in columns:
                packed_cols.append(self._pack_name(col, is_supercol_name=is_supercol_name))
            return SlicePredicate(column_names=packed_cols)
        else:
            if column_start != '':
                column_start = self._pack_name(column_start,
                                               is_supercol_name=is_supercol_name,
                                               slice_end=_SLICE_START)
            if column_finish != '':
                column_finish = self._pack_name(column_finish,
                                                is_supercol_name=is_supercol_name,
                                                slice_end=_SLICE_FINISH)

            sr = SliceRange(start=column_start, finish=column_finish,
                            reversed=column_reversed, count=column_count)
            return SlicePredicate(slice_range=sr)

    def _pack_name(self, value, is_supercol_name=False, slice_end=_NON_SLICE):
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
                value = util.convert_time_to_uuid(value,
                        lowest_val=(slice_end == _SLICE_START),
                        randomize=False)
            else:
                value = util.convert_time_to_uuid(value,
                        randomize=True)
        elif d_type == 'BytesType' and not (isinstance(value, str) or isinstance(value, unicode)):
            raise TypeError("A str or unicode column name was expected, but %s was received instead (%s)"
                    % (value.__class__.__name__, str(value)))

        return util.pack(value, d_type)

    def _unpack_name(self, b, is_supercol_name=False):
        if not self.autopack_names:
            return b
        if b is None: return

        if is_supercol_name:
            d_type = self.supercol_name_data_type
        else:
            d_type = self.col_name_data_type

        return util.unpack(b, d_type)

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

        return util.pack(value, d_type)

    def _unpack_value(self, value, col_name):
        if not self.autopack_values:
            return value
        return util.unpack(value, self._get_data_type_for_col(col_name))

    def _pack_key(self, key):
        if not self._autopack_keys or not key:
            return key
        return util.pack(key, self.key_type)

    def _unpack_key(self, b):
        if not self._autopack_keys:
            return b
        return util.unpack(b, self.key_type)

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

        packed_key = self._pack_key(key)
        single_column = columns is not None and len(columns) == 1
        if (not self.super and single_column) or \
           (self.super and super_column is not None and single_column):
            super_col_orig = super_column is not None
            column = None
            if self.super and super_column is None:
                super_column = columns[0]
            else:
                column = columns[0]
            cp = self._column_path(super_column, column)
            try:
                self._obtain_connection()
                col_or_super = self._tlocal.client.get(
                    packed_key, cp,
                    read_consistency_level or self.read_consistency_level)
            finally:
                self._release_connection()
            return self._cosc_to_dict([col_or_super], include_timestamp)
        else:
            cp = self._column_parent(super_column)
            sp = self._slice_predicate(columns, column_start, column_finish,
                                       column_reversed, column_count, super_column)

            try:
                self._obtain_connection()
                list_col_or_super = self._tlocal.client.get_slice(
                    packed_key, cp, sp,
                    read_consistency_level or self.read_consistency_level)
            finally:
                self._release_connection()

            if len(list_col_or_super) == 0:
                raise NotFoundException()
            return self._cosc_to_dict(list_col_or_super, include_timestamp)

    def get_indexed_slices(self, index_clause, columns=None, column_start="", column_finish="",
                           column_reversed=False, column_count=100, include_timestamp=False,
                           read_consistency_level=None, buffer_size=None):
        """
        Similar to :meth:`get_range()`, but an :class:`~pycassa.cassandra.ttypes.IndexClause`
        is used instead of a key range.

        `index_clause` limits the keys that are returned based on expressions
        that compare the value of a column to a given value.  At least one of the
        expressions in the :class:`.IndexClause` must be on an indexed column.

        Note that Cassandra does not support secondary indexes or get_indexed_slices()
        for super column families.

            .. seealso:: :meth:`~pycassa.index.create_index_clause()` and
                         :meth:`~pycassa.index.create_index_expression()`

        """

        assert not self.super, "get_indexed_slices() is not " \
                "supported by super column families"

        cp = self._column_parent()
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        new_exprs = []
        # Pack the values in the index clause expressions
        for expr in index_clause.expressions:
            name = self._pack_name(expr.column_name)
            value = self._pack_value(expr.value, name)
            new_exprs.append(IndexExpression(name, expr.op, value))

        packed_start_key = self._pack_key(index_clause.start_key)
        clause = IndexClause(new_exprs, packed_start_key, index_clause.count)

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
                key_slices = self._tlocal.client.get_indexed_slices(
                    cp, clause, sp, read_consistency_level or self.read_consistency_level)
            finally:
                self._release_connection()

            if key_slices is None:
                return
            for j, key_slice in enumerate(key_slices):
                # Ignore the first element after the first iteration
                # because it will be a duplicate.
                if j == 0 and i != 0:
                    continue
                unpacked_key = self._unpack_key(key_slice.key)
                yield (unpacked_key,
                       self._cosc_to_dict(key_slice.columns, include_timestamp))

                count += 1
                if row_count is not None and count >= row_count:
                    return

            if len(key_slices) != buffer_size:
                return
            last_key = key_slices[-1].key
            i += 1

    def multiget(self, keys, columns=None, column_start="", column_finish="",
                 column_reversed=False, column_count=100, include_timestamp=False,
                 super_column=None, read_consistency_level = None, buffer_size=None):
        """
        Fetch multiple rows from a Cassandra server.

        `keys` should be a list of keys to fetch.

        `buffer_size` is the number of rows from the total list to fetch at a time.
        If left as ``None``, the ColumnFamily's `buffer_size` will be used.

        All other parameters are the same as :meth:`get()`, except that a list of keys may
        be passed in.

        Results will be returned in the form: ``{key: {column_name: column_value}}``. If
        an OrderedDict is used, the rows will have the same order as `keys`.

        """

        packed_keys = map(self._pack_key, keys)
        cp = self._column_parent(super_column)
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   column_reversed, column_count, super_column)
        consistency = read_consistency_level or self.read_consistency_level

        buffer_size = buffer_size or self.buffer_size
        offset = 0
        keymap = {}
        while offset < len(packed_keys):
            try:
                self._obtain_connection()
                new_keymap = self._tlocal.client.multiget_slice(
                    packed_keys[offset:offset+buffer_size], cp, sp, consistency)
            finally:
                self._release_connection()
            keymap.update(new_keymap)
            offset += buffer_size

        ret = self.dict_class()

        # Keep the order of keys
        for key in keys:
            ret[key] = None

        non_empty_keys = []
        for packed_key, columns in keymap.iteritems():
            if len(columns) > 0:
                unpacked_key = self._unpack_key(packed_key)
                non_empty_keys.append(unpacked_key)
                ret[unpacked_key] = self._cosc_to_dict(columns, include_timestamp)

        for key in keys:
            if key not in non_empty_keys:
                try:
                    del ret[key]
                except KeyError:
                    pass

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

        packed_key = self._pack_key(key)
        cp = self._column_parent(super_column)
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   False, self.MAX_COUNT, super_column)

        try:
            self._obtain_connection()
            ret = self._tlocal.client.get_count(
                packed_key, cp, sp,
                read_consistency_level or self.read_consistency_level)
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

        packed_keys = map(self._pack_key, keys)
        cp = self._column_parent(super_column)
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   False, self.MAX_COUNT, super_column)

        try:
            self._obtain_connection()
            ret = self._tlocal.client.multiget_count(
                packed_keys, cp, sp,
                read_consistency_level or self.read_consistency_level)
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

        cp = self._column_parent(super_column)
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   column_reversed, column_count, super_column)

        count = 0
        i = 0
        last_key = self._pack_key(start)
        finish = self._pack_key(finish)

        if buffer_size is None:
            buffer_size = self.buffer_size
        while True:
            if row_count is not None:
                buffer_size = min(row_count - count + 1, buffer_size)
            key_range = KeyRange(start_key=last_key, end_key=finish, count=buffer_size)
            try:
                self._obtain_connection()
                key_slices = self._tlocal.client.get_range_slices(
                    cp, sp, key_range, read_consistency_level or self.read_consistency_level)
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
                yield (self._unpack_key(key_slice.key),
                       self._cosc_to_dict(key_slice.columns, include_timestamp))
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
        packed_key = self._pack_key(key)
        if ((not self.super) and len(columns) == 1) or \
           (self.super and len(columns) == 1 and len(columns.values()[0]) == 1):

            if timestamp is None:
                timestamp = self.timestamp()

            if self.super:
                super_col = columns.keys()[0]
                cp = self._column_path(super_col)
                columns = columns.values()[0]
            else:
                cp = self._column_path()

            colname = self._pack_name(columns.keys()[0], False)
            colval = self._pack_value(columns.values()[0], colname)
            column = Column(colname, colval, timestamp, ttl)
            try:
                self._obtain_connection()
                self._tlocal.client.insert(packed_key, cp, column,
                    write_consistency_level or self.write_consistency_level)
            finally:
                self._release_connection()
            return timestamp
        else:
            return self.batch_insert({packed_key: columns}, timestamp=timestamp, ttl=ttl,
                                     write_consistency_level=write_consistency_level)

    def batch_insert(self, rows, timestamp=None, ttl=None, write_consistency_level = None):
        """
        Like :meth:`insert()`, but multiple rows may be inserted at once.

        The `rows` parameter should be of the form ``{key: {column_name: column_value}}``
        if this is a standard column family or
        ``{key: {super_column_name: {column_name: column_value}}}`` if this is a super
        column family.

        """

        if timestamp == None:
            timestamp = self.timestamp()
        batch = self.batch(write_consistency_level=write_consistency_level)
        for key, columns in rows.iteritems():
            batch.insert(key, columns, timestamp=timestamp, ttl=ttl)
        batch.send()
        return timestamp

    def add(self, key, column, value=1, super_column=None, write_consistency_level=None):
        """
        Increment or decrement a counter.

        `value` should be an integer, either positive or negative, to be added
        to a counter column. By default, `value` is 1.

        .. note:: This method is not idempotent. Retrying a failed add may result
                  in a double count. You should consider using a separate
                  ConnectionPool with retries disabled for column families
                  with counters.

        .. versionadded:: 1.1.0
            Available in Cassandra 0.8.0 and later.

        """
        packed_key = self._pack_key(key)
        cp = self._column_parent(super_column)
        column = self._pack_name(column)
        try:
            self._obtain_connection()
            self._tlocal.client.add(packed_key, cp, CounterColumn(column, value),
                                    write_consistency_level or self.write_consistency_level)
        finally:
            self._release_connection()


    def remove(self, key, columns=None, super_column=None,
               write_consistency_level=None, timestamp=None, counter=None):
        """
        Remove a specified row or a set of columns within the row with key `key`.

        A set of columns or super columns to delete may be specified using
        `columns`.

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

    def remove_counter(self, key, column, super_column=None, write_consistency_level=None):
        """
        Remove a counter at the specified location.

        Note that counters have limited support for deletes: if you remove a
        counter, you must wait to issue any following update until the delete
        has reached all the nodes and all of them have been fully compacted.

        .. versionadded:: 1.1.0
            Available in Cassandra 0.8.0 and later.

        """
        packed_key = self._pack_key(key)
        cp = self._column_path(super_column, column)
        consistency = write_consistency_level or self.write_consistency_level
        try:
            self._obtain_connection()
            self._tlocal.client.remove_counter(packed_key, cp, consistency)
        finally:
            self._release_connection()

    def batch(self, queue_size=100, write_consistency_level=None):
        """
        Create batch mutator for doing multiple insert, update, and remove
        operations using as few roundtrips as possible.

        The `queue_size` parameter sets the max number of mutations per request.

        A :class:`~pycassa.batch.CfMutator` is returned.

        """

        return CfMutator(self, queue_size,
                         write_consistency_level or self.write_consistency_level)

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
