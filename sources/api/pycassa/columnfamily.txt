:mod:`pycassa.columnfamily` -- Column Family
============================================

.. automodule:: pycassa.columnfamily

    .. automethod:: pycassa.columnfamily.gm_timestamp

    .. autoclass:: pycassa.columnfamily.ColumnFamily(pool, column_family[, buffer_size][, read_consistency_level][, write_consistency_level][, timestamp][, dict_class][, autopack_names][, autopack_values])

        .. automethod:: get(key[, columns][, column_start][, column_finish][, column_count][, column_reversed][, include_timestamp][, super_column][, read_consistency_level])

        .. automethod:: multiget(keys[, columns][, column_start][, column_finish][, column_count][, column_reversed][, include_timestamp][, super_column][, read_consistency_level])

        .. automethod:: get_count(key[, super_column][, columns][, column_start][, column_finish][, super_column][, read_consistency_level])

        .. automethod:: multiget_count(key[, super_column][, columns][, column_start][, column_finish][, super_column][, read_consistency_level])

        .. automethod:: get_range([start][, finish][, columns][, column_start][, column_finish][, column_reversed][, column_count][, row_count][, include_timestamp][, super_column][, read_consistency_level][, buffer_size])

        .. automethod:: get_indexed_slices(index_clause[, columns][, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp][, read_consistency_level][, buffer_size])

        .. automethod:: insert(key, columns[, timestamp][, ttl][, write_consistency_level])

        .. automethod:: batch_insert(rows[, timestamp][, ttl][, write_consistency_level])

        .. automethod:: remove(key[, columns][, super_column][, write_consistency_level])

        .. automethod:: truncate()

        .. automethod:: batch(self[, queue_size][, write_consistency_level])
