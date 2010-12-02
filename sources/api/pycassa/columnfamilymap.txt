:mod:`pycassa.columnfamilymap` -- Maps Classes to Column Families
=================================================================

.. automodule:: pycassa.columnfamilymap

    .. autoclass:: pycassa.columnfamilymap.ColumnFamilyMap(cls, column_family[, columns][, raw_columns])

        .. automethod:: get(key[, columns][, super_column][, read_consistency_level])

        .. automethod:: multiget(keys[, columns][, super_column][, read_consistency_level])

        .. automethod:: get_count(key[, columns][, super_column][, read_consistency_level])

        .. automethod:: get_range([start][, finish][, row_count][, columns][, super_column][, read_consistency_level][, buffer_size])

        .. automethod:: get_indexed_slices(index_clause[,instance][, columns][, super_column][, read_consistency_level][, buffer_size])

        .. automethod:: insert(instance[, columns][, write_consistency_level])

        .. automethod:: remove(instance[, columns][, write_consistency_level])
