:mod:`pycassa.batch` -- Batch Operations
========================================

.. automodule:: pycassa.batch

    .. autoclass:: pycassa.batch.Mutator

        .. automethod:: insert(column_family, key, columns[, timestamp][, ttl])

        .. automethod:: remove(column_family, key[, columns][, super_column][, timestamp])

        .. automethod:: send([write_consistency_level])

    .. autoclass:: pycassa.batch.CfMutator

        .. automethod:: insert(key, cols[, timestamp][, ttl])

        .. automethod:: remove(key[, columns][, super_column][, timestamp])
