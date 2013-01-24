"""A functional set of stubs to be used for unit testing.

Projects that use pycassa and need to run an automated unit test suite on a
system like Jenkins can use these stubs to emulate interactions with Cassandra
without spinning up a cluster locally.

"""

import operator

from functools import partial

from pycassa import NotFoundException
from pycassa.util import OrderedDict
from pycassa.columnfamily import gm_timestamp
from pycassa.index import EQ, GT, GTE, LT, LTE


__all__ = ['ConnectionPoolStub', 'ColumnFamilyStub', 'SystemManagerStub']

class OrderedDictWithTime(OrderedDict):
    def __init__(self, *args, **kwargs):
        self.__timestamp = kwargs.pop('timestamp', None)
        super(OrderedDictWithTime, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value, timestamp=None):
        if timestamp is None:
            timestamp = self.__timestamp or gm_timestamp()

        super(OrderedDictWithTime, self).__setitem__(key, (value, timestamp))


operator_dict = {
    EQ: operator.eq,
    GT: operator.gt,
    GTE: operator.ge,
    LT: operator.lt,
    LTE: operator.le,
}


class ConnectionPoolStub(object):
    """Connection pool stub.

    Notes created column families in :attr:`self.column_families`.

    """
    def __init__(self, *args, **kwargs):
        self.column_families = {}

    def _register_mock_cf(self, name, cf):
        if name:
            self.column_families[name] = cf

    def dispose(self, *args, **kwargs):
        pass


class SystemManagerStub(object):
    """Functional System Manager stub object.

    Records when column families, columns, and indexes have been created. To
    see what has been recorded, look at :attr:`self.column_families`.

    """

    def __init__(self, *args, **kwargs):
        self.column_families = {}

    def create_column_family(self, keyspace, table_name, *args, **kwargs):
        """Create a column family and record its existence."""

        self.column_families[table_name] = {
            'keyspace': keyspace,
            'columns': {},
            'indexes': {},
        }

    def alter_column(self, keyspace, table_name, column_name, column_type):
        """Alter a column, recording its name and type."""

        self.column_families[table_name]['columns'][column_name] = column_type

    def create_index(self, keyspace, table_name, column_name, column_type):
        """Create an index, recording its name and type."""

        self.column_families[table_name]['indexes'][column_name] = column_type

    def _schema(self):
        ret = ','.join(self.column_families.keys())
        for k in self.column_families:
            for v in ('columns', 'indexes'):
                ret += ','.join(self.column_families[k][v])

        return hash(ret)

    def describe_schema_versions(self):
        """Describes the schema based on a hash of the stub system state."""

        return {self._schema(): ['1.1.1.1']}


class ColumnFamilyStub(object):
    """Functional ColumnFamily stub object.

    Acts very similar to a remote column family, supporting a basic version of
    the API. When instantiated, it registers itself with the supplied (stub)
    connection pool.

    """

    def __init__(self, pool=None, column_family=None, rows=None, **kwargs):
        rows = rows or OrderedDict()
        for r in rows.itervalues():
            if not isinstance(r, OrderedDictWithTime):
                r = OrderedDictWithTime(r)
        self.rows = rows

        if pool is not None:
            pool._register_mock_cf(column_family, self)

    def __len__(self):
        return len(self.rows)

    def __contains__(self, obj):
        return self.rows.__contains__(obj)

    def get(self, key, columns=None, include_timestamp=False, **kwargs):
        """Get a value from the column family stub."""

        my_columns = self.rows.get(key)
        if include_timestamp:
            get_value = lambda x: x
        else:
            get_value = lambda x: x[0]
        if not my_columns:
            raise NotFoundException()

        return OrderedDict((k, get_value(v)) for (k, v)
                           in my_columns.iteritems()
                           if not columns or k in columns)

    def multiget(self, keys, columns=None, include_timestamp=False, **kwargs):
        """Get multiple key values from the column family stub."""

        return OrderedDict(
            (key, self.get(
                key,
                columns,
                include_timestamp,
            )) for key in keys if key in self.rows)

    def batch(self, **kwargs):
        """Returns itself."""
        return self

    def send(self):
        pass

    def insert(self, key, columns, timestamp=None, **kwargs):
        """Insert data to the column family stub."""

        if key not in self.rows:
            self.rows[key] = OrderedDictWithTime([], timestamp=timestamp)

        for column in columns:
            self.rows[key].__setitem__(column, columns[column], timestamp)

        return self.rows[key][columns.keys()[0]][1]

    def get_indexed_slices(self, index_clause, **kwargs):
        """Grabs rows that match a pycassa index clause.

        See :meth:`pycassa.index.create_index_clause()` for creating such an
        index clause."""

        keys = []
        for key, row in self.rows.iteritems():
            for expr in index_clause.expressions:
                if (
                    expr.column_name in row and
                    operator_dict[expr.op](row[expr.column_name][0], expr.value)
                ):
                    keys.append(key)

        data = self.multiget(keys, **kwargs).items()
        return data

    def remove(self, key, columns=None):
        """Remove a key from the column family stub."""
        if key not in self.rows:
            raise NotFoundException()
        if columns is None:
            del self.rows[key]
        else:
            for c in columns:
                if c in self.rows[key]:
                    del self.rows[key][c]
            if not self.rows[key]:
                del self.rows[key]


    def get_range(self, include_timestamp=False, columns=None, **kwargs):
        """Currently just gets all values from the column family."""

        return [(key, self.get(key, columns, include_timestamp))
                for key in self.rows]

    def truncate(self):
        """Clears all data from the column family stub."""

        self.rows.clear()
