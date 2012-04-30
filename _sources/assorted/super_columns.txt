Super Columns
=============
Cassandra allows you to group columns in "super columns". 
You would create a super column family through :file:`cassandra-cli`
in the following way:

::

    [default@keyspace1] create column family Super1 with column_type=Super;
    632cf985-645e-11e0-ad9e-e700f669bcfc
    Waiting for schema agreement...
    ... schemas agree across the cluster

To use a super column in **pycassa**, you only need to
add an extra level to the dictionary:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(pool, 'Super1')
  >>> col_fam.insert('row_key', {'supercol_name': {'col_name': 'col_val'}})
  1354491238721345
  >>> col_fam.get('row_key')
  {'supercol_name': {'col_name': 'col_val'}}

The `super_column` parameter for :meth:`get()`-like methods allows
you to be selective about what subcolumns you get from a single
super column.

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(pool, 'Letters')
  >>> col_fam.insert('row_key', {'lowercase': {'a': '1', 'b': '2', 'c': '3'}})
  1354491239132744
  >>> col_fam.get('row_key', super_column='lowercase')
  {'supercol1': {'a': '1': 'b': '2', 'c': '3'}}
  >>> col_fam.get('row_key', super_column='lowercase', columns=['a', 'b'])
  {'supercol1': {'a': '1': 'b': '2'}}
  >>> col_fam.get('row_key', super_column='lowercase', column_start='b')
  {'supercol1': {'b': '1': 'c': '2'}}
  >>> col_fam.get('row_key', super_column='lowercase', column_finish='b', column_reversed=True)
  {'supercol1': {'c': '2', 'b': '1'}}

