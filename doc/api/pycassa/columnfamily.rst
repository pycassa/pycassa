:mod:`pycassa.columnfamily` -- Column Family
============================================

.. automodule:: pycassa.columnfamily

    .. automethod:: pycassa.columnfamily.gm_timestamp

    .. autoclass:: pycassa.columnfamily.ColumnFamily(pool, column_family)

        .. autoattribute:: read_consistency_level

        .. autoattribute:: write_consistency_level

        .. autoattribute:: autopack_names

        .. autoattribute:: autopack_values

        .. autoattribute:: autopack_keys

        .. autoattribute:: column_name_class

        .. autoattribute:: super_column_name_class

        .. autoattribute:: default_validation_class

        .. autoattribute:: column_validators

        .. autoattribute:: key_validation_class

        .. autoattribute:: dict_class

        .. autoattribute:: buffer_size

        .. autoattribute:: column_buffer_size

        .. autoattribute:: timestamp

        .. automethod:: load_schema()

        .. automethod:: get(key[, columns][, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp][, super_column][, read_consistency_level])

        .. automethod:: multiget(keys[, columns][, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp][, super_column][, read_consistency_level][, buffer_size])

        .. automethod:: xget(key[, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp][, read_consistency_level][, buffer_size])

        .. automethod:: get_count(key[, super_column][, columns][, column_start][, column_finish][, super_column][, read_consistency_level][, column_reversed][, max_count])

        .. automethod:: multiget_count(key[, super_column][, columns][, column_start][, column_finish][, super_column][, read_consistency_level][, buffer_size][, column_reversed][, max_count])

        .. automethod:: get_range([start][, finish][, columns][, column_start][, column_finish][, column_reversed][, column_count][, row_count][, include_timestamp][, super_column][, read_consistency_level][, buffer_size][, filter_empty])

        .. automethod:: get_indexed_slices(index_clause[, columns][, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp][, read_consistency_level][, buffer_size])

        .. automethod:: insert(key, columns[, timestamp][, ttl][, write_consistency_level])

        .. automethod:: batch_insert(rows[, timestamp][, ttl][, write_consistency_level])

        .. automethod:: add(key, column[, value][, super_column][, write_consistency_level])

        .. automethod:: remove(key[, columns][, super_column][, write_consistency_level])

        .. automethod:: remove_counter(key, column[, super_column][, write_consistency_level])

        .. automethod:: truncate()

        .. automethod:: batch(self[, queue_size][, write_consistency_level])
