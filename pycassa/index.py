"""
Tools for using Cassandra's secondary indexes.

Example Usage:

.. code-block:: python

    >>> import pycassa
    >>> conn = pycassa.connect('Keyspace1')
    >>> cf = pycassa.ColumnFamily(conn, 'Indexed1')
    >>> index_expr1 = pycassa.create_index_expression('birthdate', 1970)
    >>> index_expr2 = pycassa.create_index_expression('age', 40)
    >>> index_clause = pycassa.create_index_clause([index_expr1, index_expr2], count=10000)
    >>> for row in cf.get_indexed_slices(index_clause):
    >>>     pass # do stuff here, or use list() on the result instead

This is give you all of the rows (up to 10000) which have a 'birthdate' value
of 1970 and an 'age' value of 40.

.. seealso:: :meth:`~pycassa.system_manager.SystemManager.create_index()`
             and :meth:`~pycassa.system_manager.SystemManager.drop_index()`

"""

from pycassa.cassandra.ttypes import IndexClause, IndexExpression,\
                                     IndexOperator

__all__ = ['create_index_clause', 'create_index_expression']

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

def create_index_expression(column_name, value, op=IndexOperator.EQ):
    """
    Constructs an :class:`~pycassa.cassandra.ttypes.IndexExpression` to use
    in an :class:`~pycassa.cassandra.ttypes.IndexClause`

    The expression will be applied to the column with name `column_name`. A match
    will only occur if the operator specified with `op` returns ``True`` when used
    on the actual column value and the `value` parameter.

    The default operator is :const:`pycassa.cassandra.ttypes.IndexOperator.EQ`, which
    tests for equality.

    """
    return IndexExpression(column_name=column_name, op=op, value=value)
