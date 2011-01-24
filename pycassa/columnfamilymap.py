"""
Provides a means for mapping an existing class to a column family.

.. seealso:: :mod:`pycassa.types`

In addition to the default :class:`~pycassa.types.Column` classes,
you may also define your own types for the mapper. For example, the
IntString may be defined as:

.. code-block:: python

    >>> class IntString(pycassa.Column):
    ...     def pack(self, val):
    ...         return str(val)
    ...     def unpack(self, val):
    ...         return int(val)
    ... 

"""

from pycassa.types import Column
from pycassa.cassandra.ttypes import IndexExpression, IndexClause

__all__ = ['ColumnFamilyMap']

def create_instance(cls, **kwargs):
    instance = cls()
    instance.__dict__.update(kwargs)
    return instance

class ColumnFamilyMap(object):
    """ Maps an existing class to a column family. """

    def __init__(self, cls, column_family, columns=None, raw_columns=False):
        """
        Maps an existing class to a column family.  Class fields become columns,
        and instances of that class can be represented as rows in standard column
        families or super columns in super column families.

        Instances of `cls` are returned from :meth:`get()`, :meth:`multiget()`,
        :meth:`get_range()` and :meth:`get_indexed_slices()`.

        `column_family` is a :class:`~pycassa.columnfamily.ColumnFamily` to
        tie with `cls`.  This :class:`ColumnFamily` should almost always have
        `autopack_names` and `autopack_values` set to ``False``.

        If `raw_columns` is ``True``, all columns will be fetched into the
        `raw_columns` field in requests.

        """
        self.cls = cls
        self.column_family = column_family

        self.raw_columns = raw_columns
        self.dict_class = self.column_family.dict_class
        self.columns = self.dict_class()

        for name, column in self.cls.__dict__.iteritems():
            if not isinstance(column, Column):
                continue

            self.columns[name] = column

    def combine_columns(self, columns):
        combined_columns = self.dict_class()
        if self.raw_columns:
            combined_columns['raw_columns'] = self.dict_class()
        for column, type in self.columns.iteritems():
            combined_columns[column] = type.default
        for column, value in columns.iteritems():
            col_cls = self.columns.get(column, None)
            if col_cls is not None:
                combined_columns[column] = col_cls.unpack(value)
            if self.raw_columns:
                combined_columns['raw_columns'][column] = value
        return combined_columns

    def get(self, key, *args, **kwargs):
        """
        Creates one or more instances of `cls` from the row with key `key`.

        The fields that are retreived may be specified using `columns`, which
        should be a list of column names.

        If the column family is a super column family, a list of `cls`
        instances will be returned, one for each super column.  If
        the `super_column` parameter is not supplied, then `columns`
        specifies which super columns will be used to create instances
        of `cls`.  If the `super_column` parameter *is* supplied, only
        one instance of `cls` will be returned; if `columns` is specified
        in this case, only those attributes listed in `columns` will be fetched.

        """
        if 'columns' not in kwargs and not self.column_family.super and not self.raw_columns:
            kwargs['columns'] = self.columns.keys()

        columns = self.column_family.get(key, *args, **kwargs)

        if self.column_family.super:
            if 'super_column' not in kwargs:
                vals = self.dict_class()
                for super_column, subcols in columns.iteritems():
                    combined = self.combine_columns(subcols)
                    vals[super_column] = create_instance(self.cls, key=key,
                            super_column=super_column, **combined)
                return vals

            combined = self.combine_columns(columns)
            return create_instance(self.cls, key=key,
                                   super_column=kwargs['super_column'],
                                   **combined)

        combined = self.combine_columns(columns)
        return create_instance(self.cls, key=key, **combined)

    def multiget(self, *args, **kwargs):
        """
        Like :meth:`get()`, but a list of keys may be specified.

        The result of multiget will be a dictionary where the keys
        are the keys from the `keys` argument, minus any missing rows.
        The value for each key in the dictionary will be the same as
        if :meth:`get()` were called on that individual key.

        """
        if 'columns' not in kwargs and not self.column_family.super and not self.raw_columns:
            kwargs['columns'] = self.columns.keys()
        kcmap = self.column_family.multiget(*args, **kwargs)
        ret = self.dict_class()
        for key, columns in kcmap.iteritems():
            if self.column_family.super:
                if 'super_column' not in kwargs:
                    vals = self.dict_class()
                    for super_column, subcols in columns.iteritems():
                        combined = self.combine_columns(subcols)
                        vals[super_column] = create_instance(self.cls, key=key, super_column=super_column, **combined)
                    ret[key] = vals
                else:
                    combined = self.combine_columns(columns)
                    ret[key] = create_instance(self.cls, key=key, super_column=kwargs['super_column'], **combined)
            else:
                combined = self.combine_columns(columns)
                ret[key] = create_instance(self.cls, key=key, **combined)
        return ret

    def get_count(self, *args, **kwargs):
        """
        Count the number of columns for a key.

        .. deprecated:: 0.6.0
            Use :meth:`pycassa.columnfamily.ColumnFamily.get()` instead.

        """
        return self.column_family.get_count(*args, **kwargs)

    def get_range(self, *args, **kwargs):
        """
        Get an iterator over instances in a specified key range.

        Like :meth:`multiget()`, whether a single instance or multiple
        instances are returned per-row when the column family is a super
        column family depends on what parameters are passed.

        For an explanation of how :meth:`get_range` works and a description
        of the parameters, see :meth:`pycassa.columnfamily.ColumnFamily.get_range()`.

        Example usage with a standard column family:

        .. code-block:: python

            >>> pool = pycassa.ConnectionPool('Keyspace1')
            >>> usercf =  pycassa.ColumnFamily(pool, 'Users')
            >>> cfmap = pycassa.ColumnFamilyMap(MyClass, usercf)
            >>> users = cfmap.get_range(row_count=2, columns=['name', 'age'])
            >>> for key, user in users:
            ...     print user.name, user.age
            Miles Davis 84
            Winston Smith 42

        """
        if 'columns' not in kwargs and not self.column_family.super and not self.raw_columns:
            kwargs['columns'] = self.columns.keys()
        for key, columns in self.column_family.get_range(*args, **kwargs):
            if self.column_family.super:
                if 'super_column' not in kwargs:
                    vals = self.dict_class()
                    for super_column, subcols in columns.iteritems():
                        combined = self.combine_columns(subcols)
                        vals[super_column] = create_instance(self.cls, key=key, super_column=super_column, **combined)
                    yield vals
                else:
                    combined = self.combine_columns(columns)
                    yield create_instance(self.cls, key=key, super_column=kwargs['super_column'], **combined)
            else:
                combined = self.combine_columns(columns)
                yield create_instance(self.cls, key=key, **combined)

    def get_indexed_slices(self, instance=None, *args, **kwargs):
        """
        Fetches a list of instances that satisfy an index clause. Similar
        to :meth:`get_range()`, but uses an index clause instead of a key range.

        If `instance` is supplied, its values will be used for each
        :class:`IndexExpression` where the name matches one of the instance's
        attribute names. This makes packing the values in the :class:`IndexExpresssion`
        simpler when possible.

        See :meth:`pycassa.columnfamily.ColumnFamily.get_indexed_slices()` for
        an explanation of the parameters.

        """

        assert not self.column_family.super, "get_indexed_slices() is not " \
                "supported by super column families"

        if 'columns' not in kwargs and not self.raw_columns:
            kwargs['columns'] = self.columns.keys()

        # Autopack the index clause's values
        if instance is not None:
            new_exprs = []
            for expr in kwargs['index_clause'].expressions:
                new_expr = IndexExpression(expr.column_name, expr.op,
                        value=self.columns[expr.column_name].pack(instance.__dict__[expr.column_name]))
                new_exprs.append(new_expr)
            old_clause = kwargs['index_clause']
            new_clause = IndexClause(new_exprs, old_clause.start_key, old_clause.count)
            kwargs['index_clause'] = new_clause

        keyslice_map = self.column_family.get_indexed_slices(*args, **kwargs)

        ret = self.dict_class()
        for key, columns in keyslice_map:
            combined = self.combine_columns(columns)
            ret[key] = create_instance(self.cls, key=key, **combined)
        return ret

    def insert(self, instance, columns=None, write_consistency_level=None):
        """
        Insert or update stored instances.

        `instance` should be an instance of `cls` to store.

        The `columns` parameter allows to you specify which attributes of
        `instance` should be inserted or updated. If left as ``None``, all
        attributes will be inserted.

        """
        insert_dict = {}
        if columns is None:
            columns = self.columns.keys()

        for column in columns:
            if instance.__dict__.has_key(column) and instance.__dict__[column] is not None:
                insert_dict[column] = self.columns[column].pack(instance.__dict__[column])

        if self.column_family.super:
            insert_dict = {instance.super_column: insert_dict}

        return self.column_family.insert(instance.key, insert_dict,
                                         write_consistency_level=write_consistency_level)

    def remove(self, instance, columns=None, write_consistency_level=None):
        """
        Removes a stored instance.

        The `columns` parameter is a list of columns that should be removed.
        If this is left as the default value of ``None``, the entire stored
        instance will be removed.

        """
        if self.column_family.super:
            return self.column_family.remove(instance.key,
                                             super_column=instance.super_column,
                                             columns=columns,
                                             write_consistency_level=write_consistency_level)
        else:
            return self.column_family.remove(instance.key, columns,
                                             write_consistency_level=write_consistency_level)
