"""
Provides an abstraction of Cassandra's data model to allow for easy
manipulation of data inside Cassandra.

.. seealso:: :mod:`pycassa.columnfamilymap`
"""

import time
import struct
from UserDict import DictMixin

from pycassa.cassandra.ttypes import Column, ColumnOrSuperColumn,\
    ColumnParent, ColumnPath, ConsistencyLevel, NotFoundException,\
    SlicePredicate, SliceRange, SuperColumn, KeyRange,\
    IndexExpression, IndexClause, CounterColumn, Mutation
import pycassa.marshal as marshal
import pycassa.types as types
from pycassa.batch import CfMutator
try:
    from collections import OrderedDict
except ImportError:
    from pycassa.util import OrderedDict

__all__ = ['gm_timestamp', 'ColumnFamily', 'PooledColumnFamily']

class ColumnValidatorDict(DictMixin):

    def __init__(self, other_dict={}):
        self.type_map = {}
        self.packers = {}
        self.unpackers = {}
        for item, value in other_dict.items():
            self[item] = value

    def __getitem__(self, item):
        return self.type_map[item]

    def __setitem__(self, item, value):
        if isinstance(value, types.CassandraType):
            self.type_map[item] = value
            self.packers[item] = value.pack
            self.unpackers[item] = value.unpack
        else:
            self.type_map[item] = marshal.extract_type_name(value)
            self.packers[item] = marshal.packer_for(value)
            self.unpackers[item] = marshal.unpacker_for(value)

    def __delitem__(self, item):
        del self.type_map[item]
        del self.packers[item]
        del self.unpackers[item]

    def keys(self):
        return self.type_map.keys()

def gm_timestamp():
    """ Gets the current GMT timestamp in microseconds. """
    return int(time.time() * 1e6)

class ColumnFamily(object):
    """ An abstraction of a Cassandra column family or super column family. """

    buffer_size = 1024
    """ When calling :meth:`get_range()` or :meth:`get_indexed_slices()`,
    the intermediate results need to be buffered if we are fetching many
    rows, otherwise performance may suffer and the Cassandra server may
    overallocate memory and fail. This is the size of that buffer in number
    of rows. The default is 1024. """

    read_consistency_level = ConsistencyLevel.ONE
    """ The default consistency level for every read operation, such as
    :meth:`get` or :meth:`get_range`. This may be overridden per-operation. This should be
    an instance of :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`.
    The default level is ``ONE``. """

    write_consistency_level = ConsistencyLevel.ONE
    """ The default consistency level for every write operation, such as
    :meth:`insert` or :meth:`remove`. This may be overridden per-operation. This should be
    an instance of :class:`.~pycassa.cassandra.ttypes.ConsistencyLevel`.
    The default level is ``ONE``. """

    timestamp = gm_timestamp
    """ Each :meth:`insert()` or :meth:`remove` sends a timestamp with every
    column. This attribute is a function that is used to get
    this timestamp when needed.  The default function is :meth:`gm_timestamp()`."""

    dict_class = OrderedDict
    """ Results are returned as dictionaries. By default, python 2.7's
    :class:`collections.OrderedDict` is used if available, otherwise
    :class:`~pycassa.util.OrderedDict` is used so that order is maintained.
    A different class, such as :class:`dict`, may be instead by used setting
    this. """

    autopack_names = True
    """ Controls whether column names are automatically converted to or from
    their natural type to the binary string format that Cassandra uses.
    The data type used is controlled by :attr:`column_name_class` for
    column names and :attr:`super_column_name_class` for super column names.
    By default, this is :const:`True`. """

    autopack_values = True
    """ Whether column values are automatically converted to or from
    their natural type to the binary string format that Cassandra uses.
    The data type used is controlled by :attr:`default_validation_class`
    and :attr:`column_validators`.
    By default, this is :const:`True`. """

    autopack_keys = True
    """ Whether row keys are automatically converted to or from
    their natural type to the binary string format that Cassandra uses.
    The data type used is controlled by :attr:`key_validation_class`.
    By default, this is :const:`True`.
    """

    def _set_column_name_class(self, t):
        if isinstance(t, types.CassandraType):
            self._column_name_class = t
            self._name_packer = t.pack
            self._name_unpacker = t.unpack
        else:
            self._column_name_class = marshal.extract_type_name(t)
            self._name_packer = marshal.packer_for(t)
            self._name_unpacker = marshal.unpacker_for(t)

    def _get_column_name_class(self):
        return self._column_name_class

    column_name_class = property(_get_column_name_class, _set_column_name_class)
    """ The data type of column names, which pycassa will use
    to determine how to pack and unpack them.

    This is set automatically by inspecting the column family's
    ``comparator_type``, but it may also be set manually if you want
    autopacking behavior without setting a ``comparator_type``. Options
    include an instance of any class in :mod:`pycassa.types`, such as ``LongType()``.
    """

    def _set_super_column_name_class(self, t):
        if isinstance(t, types.CassandraType):
            self._super_column_name_class = t
            self._super_name_packer = t.pack
            self._super_name_unpacker = t.unpack
        else:
            self._super_column_name_class = marshal.extract_type_name(t)
            self._super_name_packer = marshal.packer_for(t)
            self._super_name_unpacker = marshal.unpacker_for(t)

    def _get_super_column_name_class(self):
        return self._super_column_name_class

    super_column_name_class = property(_get_super_column_name_class,
                                       _set_super_column_name_class)
    """ Like :attr:`column_name_class`, but for
    super column names. """

    def _set_default_validation_class(self, t):
        if isinstance(t, types.CassandraType):
            self._default_validation_class = t
            self._default_value_packer = t.pack
            self._default_value_unpacker = t.unpack
            have_counters = isinstance(t, types.CounterColumnType)
        else:
            self._default_validation_class = marshal.extract_type_name(t)
            self._default_value_packer = marshal.packer_for(t)
            self._default_value_unpacker = marshal.unpacker_for(t)
            have_counters = self._default_validation_class == "CounterColumnType"

        if not self.super:
            if have_counters:
                def _make_cosc(name, value, timestamp, ttl):
                    return ColumnOrSuperColumn(counter_column=CounterColumn(name, value))
            else:
                def _make_cosc(name, value, timestamp, ttl):
                    return ColumnOrSuperColumn(Column(name, value, timestamp, ttl))
            self._make_cosc = _make_cosc
        else:
            if have_counters:
                def _make_column(name, value, timestamp, ttl):
                    return CounterColumn(name, value)
                self._make_column = _make_column

                def _make_cosc(scol_name, subcols):
                    return ColumnOrSuperColumn(counter_super_column=(SuperColumn(scol_name, subcols)))
            else:
                self._make_column = Column
                def _make_cosc(scol_name, subcols):
                    return ColumnOrSuperColumn(super_column=(SuperColumn(scol_name, subcols)))
            self._make_cosc = _make_cosc

    def _get_default_validation_class(self):
        return self._default_validation_class

    default_validation_class = property(_get_default_validation_class,
                                        _set_default_validation_class)
    """ The default data type of column values, which pycassa
    will use to determine how to pack and unpack them.

    This is set automatically by inspecting the column family's
    ``default_validation_class``, but it may also be set manually if you want
    autopacking behavior without setting a ``default_validation_class``. Options
    include an instance of any class in :mod:`pycassa.types`, such as ``LongType()``.
    """

    def _set_column_validators(self, other_dict):
        self._column_validators = ColumnValidatorDict(other_dict)

    def _get_column_validators(self):
        return self._column_validators

    column_validators = property(_get_column_validators, _set_column_validators)
    """ Like :attr:`default_validation_class`, but is a
    :class:`dict` mapping individual columns to types. """

    def _set_key_validation_class(self, t):
        if isinstance(t, types.CassandraType):
            self._key_validation_class = t
            self._key_packer = t.pack
            self._key_unpacker = t.unpack
        else:
            self._key_validation_class = marshal.extract_type_name(t)
            self._key_packer = marshal.packer_for(t)
            self._key_unpacker = marshal.unpacker_for(t)

    def _get_key_validation_class(self):
        return self._key_validation_class

    key_validation_class = property(_get_key_validation_class,
                                    _set_key_validation_class)
    """ The data type of row keys, which pycassa will use
    to determine how to pack and unpack them.

    This is set automatically by inspecting the column family's
    ``key_validation_class`` (which only exists in Cassandra 0.8 or greater),
    but may be set manually if you want the autopacking behavior without
    setting a ``key_validation_class`` or if you are using Cassandra 0.7.
    Options include an instance of any class in :mod:`pycassa.types`,
    such as ``LongType()``.
    """

    def __init__(self, pool, column_family, **kwargs):
        """
        An abstraction of a Cassandra column family or super column family.
        Operations on this, such as :meth:`get` or :meth:`insert` will get data from or
        insert data into the corresponding Cassandra column family with
        name `column_family`.

        `pool` is a :class:`~pycassa.pool.ConnectionPool` that the column
        family will use for all operations.  A connection is drawn from the
        pool before each operations and is returned afterwards.  Note that
        the keyspace to be used is determined by the pool.
        """

        self.pool = pool
        self.column_family = column_family
        self.timestamp = gm_timestamp
        self.load_schema()

        recognized_kwargs = ["buffer_size", "read_consistency_level",
                             "write_consistency_level", "timestamp",
                             "dict_class", "buffer_size", "autopack_names",
                             "autopack_values", "autopack_keys"]
        for kw in recognized_kwargs:
            if kw in kwargs:
                setattr(self, kw, kwargs[kw])

    def load_schema(self):
        """
        Loads the schema definition for this column family from
        Cassandra and updates comparator and validation classes if
        neccessary.
        """
        ksdef = self.pool.execute('get_keyspace_description',
                                  use_dict_for_col_metadata=True)
        try:
            self._cfdef = ksdef[self.column_family]
        except KeyError:
            nfe = NotFoundException()
            nfe.why = 'Column family %s not found.' % self.column_family
            raise nfe

        self.super = self._cfdef.column_type == 'Super'
        self._load_comparator_classes()
        self._load_validation_classes()
        self._load_key_class()

    def _load_comparator_classes(self):
        if not self.super:
            self.column_name_class = self._cfdef.comparator_type
            self.super_column_name_class = None
        else:
            self.column_name_class = self._cfdef.subcomparator_type
            self.super_column_name_class = self._cfdef.comparator_type

    def _load_validation_classes(self):
        self.default_validation_class = self._cfdef.default_validation_class
        self.column_validators = {}
        for name, coldef in self._cfdef.column_metadata.items():
            self.column_validators[name] = coldef.validation_class

    def _load_key_class(self):
        if hasattr(self._cfdef, "key_validation_class"):
            self.key_validation_class = self._cfdef.key_validation_class
        else:
            self.key_validation_class = 'BytesType'

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
                ret[self._unpack_name(scounter.name, True)] = self._scounter_to_dict(scounter)
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
                                               slice_start=True)
            if column_finish != '':
                column_finish = self._pack_name(column_finish,
                                                is_supercol_name=is_supercol_name,
                                                slice_start=False)

            sr = SliceRange(start=column_start, finish=column_finish,
                            reversed=column_reversed, count=column_count)
            return SlicePredicate(slice_range=sr)

    def _pack_name(self, value, is_supercol_name=False, slice_start=None):
        if value is None:
            return

        if not self.autopack_names:
            if not isinstance(value, basestring):
                raise TypeError("A str or unicode column name was expected, " +
                                "but %s was received instead (%s)"
                                % (value.__class__.__name__, str(value)))
            return value

        try:
            if is_supercol_name:
                return self._super_name_packer(value, slice_start)
            else:
                return self._name_packer(value, slice_start)
        except struct.error:
            if is_supercol_name:
                d_type = self.super_column_name_class
            else:
                d_type = self.column_name_class

            raise TypeError("%s is not a compatible type for %s" %
                            (value.__class__.__name__, d_type))

    def _unpack_name(self, b, is_supercol_name=False):
        if not self.autopack_names:
            return b

        try:
            if is_supercol_name:
                return self._super_name_unpacker(b)
            else:
                return self._name_unpacker(b)
        except struct.error:
            if is_supercol_name:
                d_type = self.super_column_name_class
            else:
                d_type = self.column_name_class
            raise TypeError("%s cannot be converted to a type matching %s" %
                            (b, d_type))


    def _pack_value(self, value, col_name):
        if value is None:
            return

        if not self.autopack_values:
            if not isinstance(value, basestring):
                raise TypeError("A str or unicode column value was expected for " +
                                "column '%s', but %s was received instead (%s)"
                                % (str(col_name), value.__class__.__name__, str(value)))
            return value

        packed_col_name = self._pack_name(col_name, False)
        packer = self._column_validators.packers.get(packed_col_name, self._default_value_packer)
        try:
            return packer(value)
        except struct.error:
            d_type = self.column_validators.get(col_name, self._default_validation_class)
            raise TypeError("%s is not a compatible type for %s" %
                            (value.__class__.__name__, d_type))

    def _unpack_value(self, value, col_name):
        if not self.autopack_values:
            return value
        unpacker = self._column_validators.unpackers.get(col_name, self._default_value_unpacker)
        try:
            return unpacker(value)
        except struct.error:
            d_type = self.column_validators.get(col_name, self.default_validation_class)
            raise TypeError("%s cannot be converted to a type matching %s" %
                            (value, d_type))

    def _pack_key(self, key):
        if not self.autopack_keys or key == '':
            return key
        try:
            return self._key_packer(key)
        except struct.error:
            d_type = self.key_validation_class
            raise TypeError("%s is not a compatible type for %s" %
                            (key.__class__.__name__, d_type))

    def _unpack_key(self, b):
        if not self.autopack_keys:
            return b
        try:
            return self._key_unpacker(b)
        except struct.error:
            d_type = self.key_validation_class
            raise TypeError("%s cannot be converted to a type matching %s" %
                            (b, d_type))

    def _make_mutation_list(self, columns, timestamp, ttl):
        _pack_name = self._pack_name
        _pack_value = self._pack_value
        if not self.super:
            return map(lambda (c, v): Mutation(self._make_cosc(_pack_name(c), _pack_value(v, c), timestamp, ttl)),
                       columns.iteritems())
        else:
            mut_list = []
            for super_col, subcs in columns.items():
                subcols = map(lambda (c, v): self._make_column(_pack_name(c), _pack_value(v, c), timestamp, ttl),
                              subcs.iteritems())
                mut_list.append(Mutation(self._make_cosc(_pack_name(super_col, True), subcols)))
            return mut_list

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
            col_or_super = self.pool.execute('get', packed_key, cp,
                    read_consistency_level or self.read_consistency_level)
            return self._cosc_to_dict([col_or_super], include_timestamp)
        else:
            cp = self._column_parent(super_column)
            sp = self._slice_predicate(columns, column_start, column_finish,
                                       column_reversed, column_count, super_column)

            list_col_or_super = self.pool.execute('get_slice', packed_key, cp, sp,
                read_consistency_level or self.read_consistency_level)

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

        cl = read_consistency_level or self.read_consistency_level
        cp = self._column_parent()
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   column_reversed, column_count)

        new_exprs = []
        # Pack the values in the index clause expressions
        for expr in index_clause.expressions:
            value = self._pack_value(expr.value, expr.column_name)
            name = self._pack_name(expr.column_name)
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
            key_slices = self.pool.execute('get_indexed_slices', cp, clause, sp, cl)

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
        If left as ``None``, the ColumnFamily's :attr:`buffer_size` will be used.

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
            new_keymap = self.pool.execute('multiget_slice',
                packed_keys[offset:offset+buffer_size], cp, sp, consistency)
            keymap.update(new_keymap)
            offset += buffer_size

        ret = self.dict_class()

        # Keep the order of keys
        for key in keys:
            ret[key] = None

        empty_keys = []
        for packed_key, columns in keymap.iteritems():
            unpacked_key = self._unpack_key(packed_key)
            if len(columns) > 0:
                ret[unpacked_key] = self._cosc_to_dict(columns, include_timestamp)
            else:
                empty_keys.append(unpacked_key)

        for key in empty_keys:
            try:
                del ret[key]
            except KeyError:
                pass

        return ret

    MAX_COUNT = 2**31-1
    def get_count(self, key, super_column=None, read_consistency_level=None,
                  columns=None, column_start="", column_finish="",
                  column_reversed=False, max_count=None):
        """
        Count the number of columns in the row with key `key`.

        You may limit the columns or super columns counted to those in `columns`.
        Additionally, you may limit the columns or super columns counted to
        only those between `column_start` and `column_finish`.

        You may also count only the number of subcolumns in a single super column
        using `super_column`.  If this is set, `columns`, `column_start`, and
        `column_finish` only apply to the subcolumns of `super_column`.

        To put an upper bound on the number of columns that are counted,
        set `max_count`.

        """
        if max_count is None:
            max_count = self.MAX_COUNT

        packed_key = self._pack_key(key)
        cp = self._column_parent(super_column)
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   column_reversed, max_count, super_column)

        return self.pool.execute('get_count', packed_key, cp, sp,
                read_consistency_level or self.read_consistency_level)

    def multiget_count(self, keys, super_column=None,
                       read_consistency_level=None,
                       columns=None, column_start="",
                       column_finish="", buffer_size=None,
                       column_reversed=False, max_count=None):
        """
        Perform a column count in parallel on a set of rows.

        The parameters are the same as for :meth:`multiget()`, except that a list
        of keys may be used. A dictionary of the form ``{key: int}`` is
        returned.

        `buffer_size` is the number of rows from the total list to count at a time.
        If left as ``None``, the ColumnFamily's :attr:`buffer_size` will be used.

        To put an upper bound on the number of columns that are counted,
        set `max_count`.

        """
        if max_count is None:
            max_count = self.MAX_COUNT

        packed_keys = map(self._pack_key, keys)
        cp = self._column_parent(super_column)
        sp = self._slice_predicate(columns, column_start, column_finish,
                                   column_reversed, max_count, super_column)
        consistency = read_consistency_level or self.read_consistency_level

        buffer_size = buffer_size or self.buffer_size
        offset = 0
        keymap = {}
        while offset < len(packed_keys):
            new_keymap = self.pool.execute('multiget_count',
                packed_keys[offset:offset+buffer_size], cp, sp, consistency)
            keymap.update(new_keymap)
            offset += buffer_size

        ret = self.dict_class()

        # Keep the order of keys
        for key in keys:
            ret[key] = None

        for packed_key, count in keymap.iteritems():
            ret[self._unpack_key(packed_key)] = count

        return ret

    def get_range(self, start="", finish="", columns=None, column_start="",
                  column_finish="", column_reversed=False, column_count=100,
                  row_count=None, include_timestamp=False,
                  super_column=None, read_consistency_level=None,
                  buffer_size=None, filter_empty=True):
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
        server will overallocate memory and fail. `buffer_size` is the
        size of that buffer in number of rows. If left as ``None``, the
        ColumnFamily's :attr:`buffer_size` attribute will be used.

        When `filter_empty` is left as ``True``, empty rows (including
        `range ghosts <http://wiki.apache.org/cassandra/FAQ#range_ghosts>`_)
        will be skipped and will not count towards `row_count`.

        All other parameters are the same as those of :meth:`get()`.

        A generator over ``(key, {column_name: column_value})`` is returned.
        To convert this to a list, use ``list()`` on the result.

        """

        cl = read_consistency_level or self.read_consistency_level
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
            key_slices = self.pool.execute('get_range_slices', cp, sp, key_range, cl)
            # This may happen if nothing was ever inserted
            if key_slices is None:
                return
            for j, key_slice in enumerate(key_slices):
                # Ignore the first element after the first iteration
                # because it will be a duplicate.
                if j == 0 and i != 0:
                    continue
                if filter_empty and not key_slice.columns:
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
        if timestamp is None:
            timestamp = self.timestamp()

        packed_key = self._pack_key(key)

        if ((not self.super) and len(columns) == 1) or \
           (self.super and len(columns) == 1 and len(columns.values()[0]) == 1):

            if self.super:
                super_col = columns.keys()[0]
                cp = self._column_path(super_col)
                columns = columns.values()[0]
            else:
                cp = self._column_path()

            colname = columns.keys()[0]
            colval = self._pack_value(columns.values()[0], colname)
            colname = self._pack_name(colname, False)
            column = Column(colname, colval, timestamp, ttl)

            self.pool.execute('insert', packed_key, cp, column,
                    write_consistency_level or self.write_consistency_level)
        else:
            mut_list = self._make_mutation_list(columns, timestamp, ttl)
            mutations = {packed_key: {self.column_family: mut_list}}
            self.pool.execute('batch_mutate', mutations,
                    write_consistency_level or self.write_consistency_level)

        return timestamp

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

        cf = self.column_family
        mutations = {}
        for key, columns in rows.iteritems():
            packed_key = self._pack_key(key)
            mut_list = self._make_mutation_list(columns, timestamp, ttl)
            mutations[packed_key] = {cf: mut_list}

        if mutations:
            self.pool.execute('batch_mutate', mutations,
                    write_consistency_level or self.write_consistency_level)

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
        self.pool.execute('add', packed_key, cp, CounterColumn(column, value),
                          write_consistency_level or self.write_consistency_level)

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

        The timestamp used for the mutation is returned.
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
        self.pool.execute('remove_counter', packed_key, cp,
                          write_consistency_level or self.write_consistency_level)

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
        self.pool.execute('truncate', self.column_family)

PooledColumnFamily = ColumnFamily
