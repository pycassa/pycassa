.. _secondary-indexes:

Secondary Indexes
-----------------
Cassandra supports secondary indexes, which allow you to
efficiently get only rows which match a certain expression.

Here's a `description of secondary indexes and how to use them <http://www.datastax.com/dev/blog/whats-new-cassandra-07-secondary-indexes>`_.

In order to use :meth:`~pycassa.columnfamily.ColumnFamily.get_indexed_slices()`
to get data from an indexed column family using the indexed column,
we need to create an :class:`~pycassa.cassandra.ttypes.IndexClause` which contains
a list of :class:`~pycassa.cassandra.ttypes.IndexExpression` objects. 
The :class:`IndexExpression` objects inside the clause are ANDed together,
meaning every expression must match for a row to be returned.

Suppose we have a 'Users' column family with one row per user, and we
want to get all of the users from Utah with a birthdate after 1970.
We can make use of the :mod:`pycassa.index` module to make this easier:

.. code-block:: python

  >>> from pycassa.pool import ConnectionPool
  >>> from pycassa.columnfamily import ColumnFamily
  >>> from pycassa.index import *
  >>> pool = ConnectionPool('Keyspace1')
  >>> users = ColumnFamily(pool, 'Users')
  >>> state_expr = create_index_expression('state', 'Utah')
  >>> bday_expr = create_index_expression('birthdate', 1970, GT)
  >>> clause = create_index_clause([state_expr, bday_expr], count=20)
  >>> for key, user in users.get_indexed_slices(clause):
  ...     print user['name'] + ",", user['state'], user['birthdate']
  John Smith, Utah 1971
  Mike Scott, Utah 1980
  Jeff Bird, Utah 1973

Although at least one
:class:`~pycassa.cassandra.ttypes.IndexExpression` in the clause
must be on an indexed column, you may also have other expressions which are
on non-indexed columns.
