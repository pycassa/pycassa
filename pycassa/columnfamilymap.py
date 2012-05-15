"""
Provides a way to map an existing class of objects to a column family.

This can help to cut down boilerplate code related to converting
objects to a row format and back again.  ColumnFamilyMap is primarily
useful when you have one "object" per row.

.. seealso:: :mod:`pycassa.types` for selecting data types for object
             attributes and infomation about creating custom data
             types.

"""

from pycassa.types import CassandraType
from pycassa.columnfamily import ColumnFamily
import pycassa.util as util

__all__ = ['ColumnFamilyMap']

def create_instance(cls, **kwargs):
    instance = cls()
    map(lambda (k,v): setattr(instance, k, v), kwargs.iteritems())
    return instance

class ColumnFamilyMap(ColumnFamily):
    """
    Maps an existing class to a column family.  Class fields become columns,
    and instances of that class can be represented as rows in standard column
    families or super columns in super column families.
    """

    def __init__(self, cls, pool, column_family, raw_columns=False, **kwargs):
        """
        Instances of `cls` are returned from :meth:`get()`, :meth:`multiget()`,
        :meth:`get_range()` and :meth:`get_indexed_slices()`.

        `pool` is a :class:`~pycassa.pool.ConnectionPool` that will be used
        in the same way a :class:`~.ColumnFamily` uses one.

        `column_family` is the name of a column family to tie to `cls`.

        If `raw_columns` is ``True``, all columns will be fetched into the
        `raw_columns` field in requests.
        """
        ColumnFamily.__init__(self, pool, column_family, **kwargs)

        self.cls = cls
        self.autopack_names = False

        self.raw_columns = raw_columns
        self.dict_class = util.OrderedDict
        self.defaults = {}
        self.fields = []
        for name, val_type in self.cls.__dict__.iteritems():
            if name != 'key' and isinstance(val_type, CassandraType):
                self.fields.append(name)
                self.column_validators[name] = val_type
                self.defaults[name] = val_type.default

        if hasattr(self.cls, 'key') and isinstance(self.cls.key, CassandraType):
            self.key_validation_class = self.cls.key

    def combine_columns(self, columns):
        combined_columns = columns

        if self.raw_columns:
            combined_columns['raw_columns'] = columns

        for column, default in self.defaults.items():
            combined_columns.setdefault(column, default)

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

        All other parameters behave the same as in :meth:`.ColumnFamily.get()`.

        """
        if 'columns' not in kwargs and not self.super and not self.raw_columns:
            kwargs['columns'] = self.fields

        columns = ColumnFamily.get(self, key, *args, **kwargs)

        if self.super:
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
        if 'columns' not in kwargs and not self.super and not self.raw_columns:
            kwargs['columns'] = self.fields

        kcmap = ColumnFamily.multiget(self, *args, **kwargs)
        ret = self.dict_class()
        for key, columns in kcmap.iteritems():
            if self.super:
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

    def get_range(self, *args, **kwargs):
        """
        Get an iterator over instances in a specified key range.

        Like :meth:`multiget()`, whether a single instance or multiple
        instances are returned per-row when the column family is a super
        column family depends on what parameters are passed.

        For an explanation of how :meth:`get_range` works and a description
        of the parameters, see :meth:`.ColumnFamily.get_range()`.

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
        if 'columns' not in kwargs and not self.super and not self.raw_columns:
            kwargs['columns'] = self.fields

        for key, columns in ColumnFamily.get_range(self, *args, **kwargs):
            if self.super:
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

    def get_indexed_slices(self, *args, **kwargs):
        """
        Fetches a list of instances that satisfy an index clause. Similar
        to :meth:`get_range()`, but uses an index clause instead of a key range.

        See :meth:`.ColumnFamily.get_indexed_slices()` for
        an explanation of the parameters.

        """

        assert not self.super, "get_indexed_slices() is not " \
                "supported by super column families"

        if 'columns' not in kwargs and not self.raw_columns:
            kwargs['columns'] = self.fields

        for key, columns in ColumnFamily.get_indexed_slices(self, *args, **kwargs):
            combined = self.combine_columns(columns)
            yield create_instance(self.cls, key=key, **combined)

    def _get_instance_as_dict(self, instance, columns=None):
        fields = columns or self.fields
        instance_dict = {}
        for field in fields:
            val = getattr(instance, field, None)
            if val is not None and not isinstance(val, CassandraType):
                instance_dict[field] = val
        if self.super:
            instance_dict = {instance.super_column: instance_dict}
        return instance_dict

    def insert(self, instance, columns=None, timestamp=None, ttl=None,
               write_consistency_level=None):
        """
        Insert or update stored instances.

        `instance` should be an instance of `cls` to store.

        The `columns` parameter allows to you specify which attributes of
        `instance` should be inserted or updated. If left as ``None``, all
        attributes will be inserted.
        """

        if columns is None:
            fields = self.fields
        else:
            fields = columns

        insert_dict = self._get_instance_as_dict(instance, columns=fields)
        return ColumnFamily.insert(self, instance.key, insert_dict,
                                   timestamp=timestamp, ttl=ttl,
                                   write_consistency_level=write_consistency_level)

    def batch_insert(self, instances, timestamp=None, ttl=None,
            write_consistency_level=None):
        """
        Insert or update stored instances.

        `instances` should be a list containing instances of `cls` to store.
        """
        insert_dict = dict(
            [(instance.key, self._get_instance_as_dict(instance))
                for instance in instances]
        )
        return ColumnFamily.batch_insert(self, insert_dict,
                timestamp=timestamp, ttl=ttl,
                write_consistency_level=write_consistency_level)

    def remove(self, instance, columns=None, write_consistency_level=None):
        """
        Removes a stored instance.

        The `columns` parameter is a list of columns that should be removed.
        If this is left as the default value of ``None``, the entire stored
        instance will be removed.

        """
        if self.super:
            return ColumnFamily.remove(self, instance.key,
                                       super_column=instance.super_column,
                                       columns=columns,
                                       write_consistency_level=write_consistency_level)
        else:
            return ColumnFamily.remove(self, instance.key, columns,
                                       write_consistency_level=write_consistency_level)
