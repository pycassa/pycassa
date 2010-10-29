"""
Provides a means for mapping an existing class to a column family.

"""

from pycassa.types import Column
from pycassa.cassandra.ttypes import IndexExpression, IndexClause

__all__ = ['ColumnFamilyMap']

def create_instance(cls, **kwargs):
    instance = cls()
    instance.__dict__.update(kwargs)
    return instance

class ColumnFamilyMap(object):
    """
    Maps an existing class to a column family.  Class fields become columns,
    and instances of that class can be represented as rows in the column
    family.

    """

    def __init__(self, cls, column_family, columns=None, raw_columns=False):
        """
        Construct a ObjectFamily

        :param cls: 
           Instances of cls are generated on ``get*()`` requests
        :type cls: class

        :param column_family:
           The :class:`~pycassa.columnfamily.ColumnFamily` to tie with cls.
           This should almost always have `autopack_names` and
           `autopack_values` set to ``False``.
        :type column_family: :class:`~pycassa.columnfamily.ColumnFamily`

        :param raw_columns:
           Whether all columns should be fetched into the `raw_columns` field in
           requests
        :type raw_columns: boolean
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
        Fetch a key from a Cassandra server
        
        :Parameters:
            `key`: str
                The key to fetch
            `columns`: [str]
                Limit the columns or super_columns fetched to the specified list
            `column_start`: str
                Only fetch when a column or super_column is >= column_start
            `column_finish`: str
                Only fetch when a column or super_column is <= column_finish
            `column_reversed`: bool
                Fetch the columns or super_columns in reverse order. This will do
                nothing unless you passed a dict_class to the constructor.
            `column_count`: int
                Limit the number of columns or super_columns fetched per key
            `super_column`: str
                Fetch only this super_column
            `read_consistency_level`: :class:`pycassa.cassandra.ttypes.ConsistencyLevel`
                Affects the guaranteed replication factor before returning from
                any read operation

        :Returns:
            Class instance
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

    def get_indexed_slices(self, instance=None, *args, **kwargs):
        """
        Fetches a list of KeySlices from a Cassandra server based on an index clause

        :Parameters:
            `index_clause`: :class:`~pycassa.cassandra.ttypes.IndexClause`
                Limits the keys that are returned based on expressions that compare
                the value of a column to a given value.  At least one of the
                expressions in the IndexClause must be on an indexed column.
            `columns`: [str]
                Limit the columns or super_columns fetched to the specified list
            `column_start`: str
                Only fetch when a column or super_column is >= column_start
            `column_finish`: str
                Only fetch when a column or super_column is <= column_finish
            `column_reversed`: bool
                Fetch the columns or super_columns in reverse order. This will do
                nothing unless you passed a dict_class to the constructor.
            `column_count`: int
                Limit the number of columns or super_columns fetched per key
            `include_timestamp`: bool
                If true, return a (value, timestamp) tuple for each column
            `super_column`: str
                Return columns only in this super_column
            `read_consistency_level`: :class:`pycassa.cassandra.ttypes.ConsistencyLevel`
                Affects the guaranteed replication factor before returning from
                any read operation
            `buffer_size`: When calling `get_indexed_slices()`, the
              intermediate results need to be buffered if we are fetching many
              rows, otherwise the Cassandra server will overallocate memory
              and fail.  This is the size of that buffer in number of rows.
              If left as ``None``, the :class:`~pycassa.cassandra.ColumnFamily`'s
              default `buffer_size` will be used.

        :Returns:
           {key: Class instance}

        .. seealso:: :meth:`pycassa.index.create_index_clause()` and
                     :meth:`pycassa.index.create_index_expression()`.

        """

        if 'columns' not in kwargs and not self.column_family.super and not self.raw_columns:
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

    def multiget(self, *args, **kwargs):
        """
        Fetch multiple keys from a Cassandra server

        :Parameters:
            `keys`: [str]
                A list of keys to fetch
            `columns`: [str]
                Limit the columns or super_columns fetched to the specified list
            `column_start`: str
                Only fetch when a column or super_column is >= column_start
            `column_finish`: str
                Only fetch when a column or super_column is <= column_finish
            `column_reversed`: bool
                Fetch the columns or super_columns in reverse order. This will do
                nothing unless you passed a dict_class to the constructor.
            `column_count`: int
                Limit the number of columns or super_columns fetched per key
            `include_timestamp`: bool
                If true, return a (value, timestamp) tuple for each column
            `super_column`: str
                Return columns only in this super_column
            `read_consistency_level`: :class:`pycassa.cassandra.ttypes.ConsistencyLevel`
                Affects the guaranteed replication factor before returning from
                any read operation

        :Returns:
            {'key': Class instance} 
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
        Count the number of columns for a key

        :Parameters:
            `key`: str
                The key with which to count columns
            `super_column`: str
                Count the columns only in this super_column
            `read_consistency_level`: :class:`pycassa.cassandra.ttypes.ConsistencyLevel`
                Affects the guaranteed replication factor before returning from
                any read operation

        :Returns:
            int Count of columns
        """
        return self.column_family.get_count(*args, **kwargs)

    def get_range(self, *args, **kwargs):
        """
        Get an iterator over keys in a specified range

        :Parameters:
            `start`: str
                Start from this key (inclusive)
            `finish`: str
                End at this key (inclusive)
            `columns`: [str]
                Limit the columns or super_columns fetched to the specified list
            `column_start`: str
                Only fetch when a column or super_column is >= column_start
            `column_finish`: str
                Only fetch when a column or super_column is <= column_finish
            `column_reversed`: bool
                Fetch the columns or super_columns in reverse order. This will do
                nothing unless you passed a dict_class to the constructor.
            `column_count`: int
                Limit the number of columns or super_columns fetched per key
            `row_count`: int
                Limit the number of rows fetched
            `include_timestamp`: bool
                If true, return a (value, timestamp) tuple for each column
            `super_column`: string
                Return columns only in this super_column
            `read_consistency_level`: :class:`pycassa.cassandra.ttypes.ConsistencyLevel`
                Affects the guaranteed replication factor before returning from
                any read operation

        :Returns:
            iterator over Class instance
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

    def insert(self, instance, columns=None):
        """
        Insert or update columns

        :Parameters:
            `instance`: Class instance
                The class to insert or update the columns in
            `columns`: dict
                Column: {'column': 'value'}
                SuperColumn: {'column': {'subcolumn': 'value'}}
                The columns or supercolumns to limit the insertion
                or update to.

        :Returns:
            int timestamp
        """
        insert_dict = {}
        if columns is None:
            columns = self.columns.keys()

        for column in columns:
            if instance.__dict__.has_key(column) and instance.__dict__[column] is not None:
                insert_dict[column] = self.columns[column].pack(instance.__dict__[column])

        if self.column_family.super:
            insert_dict = {instance.super_column: insert_dict}

        return self.column_family.insert(instance.key, insert_dict)

    def remove(self, instance, column=None):
        """
        Remove this instance

        :Parameters:
            `instance`: Class instance
                Remove the instance where the key is instance.key
            `column`: str
                If set, remove only this Column. Doesn't do anything for SuperColumns

        :Returns:
            int timestamp
        """
        # Hmm, should we only remove the columns specified on construction?
        # It's slower, so we'll leave it out.

        if self.column_family.super:
            return self.column_family.remove(instance.key, column=instance.super_column)
        return self.column_family.remove(instance.key, column)
