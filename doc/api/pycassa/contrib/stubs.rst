:mod:`pycassa.contrib.stubs` -- Pycassa Stubs
=============================================

.. automodule:: pycassa.contrib.stubs

    .. autoclass:: pycassa.contrib.stubs.ColumnFamilyStub(pool=None, column_family=None, rows=None)

        .. automethod:: get(key[, columns][, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp])

        .. automethod:: multiget(keys[, columns][, column_start][, column_finish][, column_reversed][, column_count][, include_timestamp])

        .. automethod:: get_range([columns][, include_timestamp])

        .. automethod:: get_indexed_slices(index_clause[, columns], include_timestamp])

        .. automethod:: insert(key, columns[, timestamp])

        .. automethod:: remove(key[, columns])

        .. automethod:: truncate()

        .. automethod:: batch(self)

    .. autoclass:: pycassa.contrib.stubs.ConnectionPoolStub()

    .. autoclass:: pycassa.contrib.stubs.SystemManagerStub()

        .. automethod:: create_column_family(keyspace, table_name)

        .. automethod:: alter_column(keyspace, table_name, column_name, column_type)

        .. automethod:: create_index(keyspace, table_name, column_name, column_type)

        .. automethod:: describe_schema_versions()
