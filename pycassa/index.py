"""
Tools for using Cassandra's secondary indexes.

Example Usage:

.. code-block:: python

    >>> from pycassa.columnfamily import ColumnFamily
    >>> from pycassa.pool import ConnectionPool
    >>> from pycassa.index import *
    >>>
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

This gives you all of the rows (up to 20) which have a 'birthdate' value
above 1970 and a state value of 'Utah'.

.. seealso:: :class:`~pycassa.system_manager.SystemManager` methods
             :meth:`~pycassa.system_manager.SystemManager.create_index()`
             and :meth:`~pycassa.system_manager.SystemManager.drop_index()`

"""

from pycassa.cassandra.ttypes import IndexClause, IndexExpression,\
                                     IndexOperator

__all__ = ['create_index_clause', 'create_index_expression', 'EQ', 'GT', 'GTE',
           'LT', 'LTE']

EQ = IndexOperator.EQ
""" Equality (==) operator for index expressions """

GT = IndexOperator.GT
""" Greater-than (>) operator for index expressions """

GTE = IndexOperator.GTE
""" Greater-than-or-equal (>=) operator for index expressions """

LT = IndexOperator.LT
""" Less-than (<) operator for index expressions """

LTE = IndexOperator.LTE
""" Less-than-or-equal (<=) operator for index expressions """

def create_index_clause(expr_list, start_key='', count=100):
    """
    Constructs an :class:`~pycassa.cassandra.ttypes.IndexClause` for use with 
    :meth:`~pycassa.columnfamily.get_indexed_slices()`

    `expr_list` should be a list of
    :class:`~pycassa.cassandra.ttypes.IndexExpression` objects that
    must be matched for a row to be returned.  At least one of these expressions
    must be on an indexed column.

    Cassandra will only return matching rows with keys after `start_key`.  If this
    is the empty string, all rows will be considered.  Keep in mind that this
    is not as meaningful unless an OrderPreservingPartitioner is used.

    The number of rows to return is limited by `count`, which defaults to 100.

    """
    return IndexClause(expressions=expr_list, start_key=start_key,
                       count=count)

def create_index_expression(column_name, value, op=EQ):
    """
    Constructs an :class:`~pycassa.cassandra.ttypes.IndexExpression` to use
    in an :class:`~pycassa.cassandra.ttypes.IndexClause`

    The expression will be applied to the column with name `column_name`. A match
    will only occur if the operator specified with `op` returns ``True`` when used
    on the actual column value and the `value` parameter.

    The default operator is :const:`~EQ`, which tests for equality.

    """
    return IndexExpression(column_name=column_name, op=op, value=value)
