:mod:`pycassa.columnfamilymap` -- Maps Classes to Column Families
=================================================================

.. automodule:: pycassa.columnfamilymap

    .. autoclass:: pycassa.columnfamilymap.ColumnFamilyMap(cls, column_family[, columns][, raw_columns])

        .. automethod:: get(key[, columns][, column_start][, column_finish][, column_count][, column_reversed][, super_column][, read_consistency_level])

        .. automethod:: multiget(keys[, columns][, column_start][, column_finish][, column_count][, column_reversed][, super_column][, read_consistency_level])

        .. automethod:: get_count(key[, columns][, super_column][, read_consistency_level])

        .. automethod:: get_range([start][, finish][, columns][, column_start][, column_finish][, column_reversed][, column_count][, row_count][, super_column][, read_consistency_level][, buffer_size])

        .. automethod:: get_indexed_slices(index_clause[, columns][, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp][, read_consistency_level][, buffer_size])

        .. automethod:: insert(instance[, columns][, write_consistency_level])

        .. automethod:: remove(instance[, columns][, write_consistency_level])
