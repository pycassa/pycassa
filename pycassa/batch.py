"""Tools to support batch operations."""

import threading
from pycassa.cassandra.ttypes import (Column, ColumnOrSuperColumn,
                                      ConsistencyLevel, Deletion, Mutation,
                                      SlicePredicate, SuperColumn)

__all__ = ['Mutator', 'CfMutator']

class Mutator(object):
    """
    Batch update convenience mechanism.

    Queues insert/update/remove operations and executes them when the queue
    is full or `send` is called explicitly.

    """

    def __init__(self, pool, queue_size=100, write_consistency_level=None):
        """Creates a new Mutator object.

        :Parameters:
            `client`: :class:`~pycassa.connection.Connection`
                The connection that will be used.
            `queue_size`: int
                The number of operations to queue before they are executed
                automatically.
            `write_consistency_level`: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`
                The Cassandra write consistency level.

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
            self._lock.release()
            if conn:
                conn.return_to_pool()

    def _make_mutations_insert(self, column_family, columns, timestamp, ttl):
        _pack_name = column_family._pack_name
        _pack_value = column_family._pack_value
        for c, v in columns.iteritems():
            cos = ColumnOrSuperColumn()
            if column_family.super:
                subc = [Column(name=_pack_name(subname),
                               value=_pack_value(subvalue, subname),
                               timestamp=timestamp, ttl=ttl)
                            for subname, subvalue in v.iteritems()]
                cos.super_column = SuperColumn(name=_pack_name(c, True),
                                               columns=subc)
            else:
                cos.column = Column(name=_pack_name(c), value=_pack_value(v, c),
                                    timestamp=timestamp, ttl=ttl)
            yield Mutation(column_or_supercolumn=cos)

    def insert(self, column_family, key, columns, timestamp=None, ttl=None):
        if columns:
            if timestamp == None:
                timestamp = column_family.timestamp()
            mutations = self._make_mutations_insert(column_family, columns,
                                                    timestamp, ttl)
            self._enqueue(key, column_family, mutations)
        return self

    def remove(self, column_family, key, columns=None, super_column=None, timestamp=None):
        if timestamp == None:
            timestamp = column_family.timestamp()
        deletion = Deletion(timestamp=timestamp)
        if columns:
            _pack_name = column_family._pack_name
            packed_cols = [_pack_name(col, column_family.super)
                           for col in columns]
            deletion.predicate = SlicePredicate(column_names=packed_cols)
            if super_column:
                deletion.super_column = super_column
        mutation = Mutation(deletion=deletion)
        self._enqueue(key, column_family, (mutation,))
        return self


class CfMutator(Mutator):
    """
    A :class:`~pycassa.batch.Mutator` that deals only with one column family.

    """

    def __init__(self, column_family, queue_size=100, write_consistency_level=None):
        """Creates a new CfMutator object.

        :Parameters:
            `column_family`: :class:`~pycassa.columnfamily.ColumnFamily`
                The column family that all operations will be on.
            `queue_size`: int
                The number of operations to queue before they are executed
                automatically.
            `write_consistency_level`: :class:`~pycassa.cassandra.ttypes.ConsistencyLevel`
                The Cassandra write consistency level.

        """
        wcl = write_consistency_level or column_family.write_consistency_level
        super(CfMutator, self).__init__(column_family.pool, queue_size=queue_size,
                                        write_consistency_level=wcl)
        self._column_family = column_family

    def insert(self, key, cols, timestamp=None, ttl=None):
        return super(CfMutator, self).insert(self._column_family, key, cols,
                                             timestamp=timestamp, ttl=ttl)

    def remove(self, key, columns=None, super_column=None, timestamp=None):
        return super(CfMutator, self).remove(self._column_family, key,
                                             columns=columns,
                                             super_column=super_column,
                                             timestamp=timestamp)

    def send(self, write_consistency_level=None):
        if write_consistency_level is None:
            write_consistency_level = self.write_consistency_level
        mutations = {}
        conn = None
        self._lock.acquire()
        try:
            for key, column_family, cols in self._buffer:
                mutations.setdefault(key, {}).setdefault(column_family, []).extend(cols)
            if mutations:
                conn = self._column_family.pool.get()
                conn.batch_mutate(mutations, write_consistency_level)
            self._buffer = []
        finally:
            if conn:
                conn.return_to_pool()
            self._lock.release()
