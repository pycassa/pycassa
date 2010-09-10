"""
Tools for using Cassandra's secondary indexes.

"""

from pycassa.cassandra.ttypes import IndexClause, IndexExpression,\
                                     IndexOperator

__all__ = ['create_index_clause', 'create_index_expression']

def create_index_clause(expr_list, start_key='', count=100):
    """
    Constructs an :class:`~pycassa.cassandra.ttypes.IndexClause` for use with 
    :meth:`~pycassa.columnfamily.get_indexed_slices()`

    :param expr_list: [:class:`~pycassa.cassandra.ttypes.IndexExpression`]
        A list of `IndexExpressions` to match
    :param start_key: str
        The key to begin searching from
    :param count: int
        The number of results to return

    """
    return IndexClause(expressions=expr_list, start_key=start_key,
                       count=count)

def create_index_expression(column_name, value, op=IndexOperator.EQ):
    """
    Constructs an :class:`~pycassa.cassandra.ttypes.IndexExpression` to use
    in an :class:`~pycassa.cassandra.ttypes.IndexClause`

    :param column_name: string
        Name of an indexed or non-indexed column
    :param value: 
        The value that will be compared to column values using op
    :param op: :class:`~pycassa.cassandra.ttypes.IndexOperator`
        The binary operator to apply to column values and `value`.  Defaults
        to `IndexOperator.EQ`, which tests for equality.

    """
    return IndexExpression(column_name=column_name, op=op, value=value)
