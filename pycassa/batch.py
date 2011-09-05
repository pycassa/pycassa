"""
The batch interface allows insert, update, and remove operations to be performed
in batches. This allows a convenient mechanism for streaming updates or doing a
large number of operations while reducing number of RPC roundtrips.

Batch mutator objects are synchronized and can be safely passed around threads.

.. code-block:: python

    >>> b = cf.batch(queue_size=10)
    >>> b.insert('key1', {'col1':'value11', 'col2':'value21'})
    >>> b.insert('key2', {'col1':'value12', 'col2':'value22'}, ttl=15)
    >>> b.remove('key1', ['col2'])
    >>> b.remove('key2')
    >>> b.send()

One can use the `queue_size` argument to control how many mutations will be
queued before an automatic :meth:`send` is performed. This allows simple streaming
of updates. If set to ``None``, automatic checkpoints are disabled. Default is 100.

Supercolumns are supported:

.. code-block:: python

    >>> b = scf.batch()
    >>> b.insert('key1', {'supercol1': {'colA':'value1a', 'colB':'value1b'}
    ...                  {'supercol2': {'colA':'value2a', 'colB':'value2b'}})
    >>> b.remove('key1', ['colA'], 'supercol1')
    >>> b.send()

You may also create a :class:`.Mutator` directly, allowing operations
on multiple column families:

.. code-block:: python

    >>> b = Mutator(pool)
    >>> b.insert(cf, 'key1', {'col1':'value1', 'col2':'value2'})
    >>> b.insert(supercf, 'key1', {'subkey1': {'col1':'value1', 'col2':'value2'}})
    >>> b.send()

.. note:: This interface does not implement atomic operations across column
          families. All the limitations of the `batch_mutate` Thrift API call
          applies. Remember, a mutation in Cassandra is always atomic per key per
          column family only.

.. note:: If a single operation in a batch fails, the whole batch fails.

In Python >= 2.5, mutators can be used as context managers, where an implicit
:meth:`send` will be called upon exit.

.. code-block:: python

    >>> with cf.batch() as b:
    ...     b.insert('key1', {'col1':'value11', 'col2':'value21'})
    ...     b.insert('key2', {'col1':'value12', 'col2':'value22'})

Calls to :meth:`insert` and :meth:`remove` can also be chained:

.. code-block:: python

    >>> cf.batch().remove('foo').remove('bar').send()

"""

import threading
from pycassa.cassandra.ttypes import (ConsistencyLevel, Deletion, Mutation, SlicePredicate)

__all__ = ['Mutator', 'CfMutator']

class Mutator(object):
    """
    Batch update convenience mechanism.

    Queues insert/update/remove operations and executes them when the queue
    is full or `send` is called explicitly.

    """

    def __init__(self, pool, queue_size=100, write_consistency_level=None):
        """Creates a new Mutator object.

        `pool` is the :class:`~pycassa.pool.ConnectionPool` that will be used
        for operations.

        After `queue_size` operations, :meth:`send()` will be executed
        automatically.  Use 0 to disable automatic sends.

        """
        self._buffer = []
        self._lock = threading.RLock()
        self.pool = pool
        self.limit = queue_size
        if write_consistency_level is None:
            self.write_consistency_level = ConsistencyLevel.ONE
        else:
            self.write_consistency_level = write_consistency_level

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.send()

    def _enqueue(self, key, column_family, mutations):
        self._lock.acquire()
        try:
            mutation = (key, column_family.column_family, mutations)
            self._buffer.append(mutation)
            if self.limit and len(self._buffer) >= self.limit:
                self.send()
        finally:
            self._lock.release()
        return self

    def send(self, write_consistency_level=None):
        """ Sends all operations currently in the batch and clears the batch. """
        if write_consistency_level is None:
            write_consistency_level = self.write_consistency_level
        mutations = {}
        conn = None
        self._lock.acquire()
        try:
            for key, column_family, cols in self._buffer:
                mutations.setdefault(key, {}).setdefault(column_family, []).extend(cols)
            if mutations:
                conn = self.pool.get()
                conn.batch_mutate(mutations, write_consistency_level)
            self._buffer = []
        finally:
            if conn:
                conn.return_to_pool()
            self._lock.release()

    def insert(self, column_family, key, columns, timestamp=None, ttl=None):
        """
        Adds a single row insert to the batch.

        `column_family` is the :class:`~pycassa.columnfamily.ColumnFamily`
        that the insert will be executed on.

        """
        if columns:
            if timestamp is None:
                timestamp = column_family.timestamp()
            packed_key = column_family._pack_key(key)
            mut_list = column_family._make_mutation_list(columns, timestamp, ttl)
            self._enqueue(packed_key, column_family, mut_list)
        return self

    def remove(self, column_family, key, columns=None, super_column=None, timestamp=None):
        """
        Adds a single row remove to the batch.

        `column_family` is the :class:`~pycassa.columnfamily.ColumnFamily`
        that the remove will be executed on.

        """
        if timestamp == None:
            timestamp = column_family.timestamp()
        deletion = Deletion(timestamp=timestamp)
        _pack_name = column_family._pack_name
        if super_column:
            deletion.super_column = _pack_name(super_column, True)
        if columns:
            is_super = column_family.super and not super_column
            packed_cols = [_pack_name(col, is_super) for col in columns]
            deletion.predicate = SlicePredicate(column_names=packed_cols)
        mutation = Mutation(deletion=deletion)
        packed_key = column_family._pack_key(key)
        self._enqueue(packed_key, column_family, (mutation,))
        return self


class CfMutator(Mutator):
    """
    A :class:`~pycassa.batch.Mutator` that deals only with one column family.

    """

    def __init__(self, column_family, queue_size=100, write_consistency_level=None):
        """ A :class:`~pycassa.batch.Mutator` that deals only with one column family.

        `column_family` is the :class:`~pycassa.columnfamily.ColumnFamily`
        that all operations will be executed on.

        """
        wcl = write_consistency_level or column_family.write_consistency_level
        super(CfMutator, self).__init__(column_family.pool, queue_size=queue_size,
                                        write_consistency_level=wcl)
        self._column_family = column_family

    def insert(self, key, cols, timestamp=None, ttl=None):
        """ Adds a single row insert to the batch. """
        return super(CfMutator, self).insert(self._column_family, key, cols,
                                             timestamp=timestamp, ttl=ttl)

    def remove(self, key, columns=None, super_column=None, timestamp=None):
        """ Adds a single row remove to the batch. """
        return super(CfMutator, self).remove(self._column_family, key,
                                             columns=columns,
                                             super_column=super_column,
                                             timestamp=timestamp)
