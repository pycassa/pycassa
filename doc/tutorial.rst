Tutorial
========

This tutorial is intended as an introduction to working with
Cassandra and **pycassa**.

Prerequisites
-------------
Before we start, make sure that you have **pycassa**
:doc:`installed <installation>`. In the Python shell, the following
should run without raising an exception:

.. doctest::

  >>> import pycassa

This tutorial also assumes that a Cassandra instance is running on the
default host and port. Read the `instructions for getting started
with Cassandra <http://wiki.apache.org/cassandra/GettingStarted>`_ , 
making sure that you choose a `version that is compatible with
pycassa <http://wiki.github.com/pycassa/pycassa/pycassa-cassandra-compatibility>`_.
You can start Cassandra like so:

.. code-block:: bash

  $ pwd
  ~/cassandra
  $ bin/cassandra -f

and import the included schema to start out:

.. code-block:: bash

  $ bin/schematool localhost 8080 import

Making a Connection
-------------------
The first step when working with **pycassa** is to create a
:class:`~pycassa.connection.Connection` to the running cassandra instance:

.. code-block:: python

  >>> import pycassa
  >>> connection = pycassa.connect('Keyspace1')

The above code will connect on the default host and port. We can also
specify the host and port explicitly, as follows:

.. code-block:: python

  >>> connection = pycassa.connect('Keyspace1', ['localhost:9160'])

Getting a ColumnFamily
----------------------
A column family is a collection of rows and columns in Cassandra,
and can be thought of as roughly the equivalent of a table in a
relational database. We'll use one of the column families that
were already included in the schema file:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(connection, 'Standard1')

Inserting Data
--------------
To insert a row into a column family we can use the
:meth:`~pycassa.columnfamily.ColumnFamily.insert` method:

.. code-block:: python

  >>> col_fam.insert('row_key', {'col_name': 'col_val'})
  1354459123410932

We can also insert more than one column at a time:

.. code-block:: python

  >>> col_fam.insert('row_key', {'name1':'val1', 'name2':'val2'})
  1354459123410932

And we can insert more than one row at a time:

.. code-block:: python

  >>> col_fam.batch_insert({'row1': {'name1':'val1'},
  ...                       'row2': {'foo':'bar'})
  1354491238721387

Getting Data
------------
There are many more ways to get data out of Cassandra than there are
to insert data.

The simplest way to get data is to use
:meth:`~pycassa.columnfamily.ColumnFamily.get()`:

.. code-block:: python

  >>> col_fam.get('row_key')
  {'colname': 'col_val'}

Without any other arguments, :meth:`~pycassa.columnfamily.ColumnFamily.get()`
returns every column in the row (up to `column_count`, which defaults to 100).
If you only want a few of the columns and you know them by name, you can
specify them using a `columns` argument:

.. code-block:: python

  >>> col_fam.get('row_key', columns=['name1', 'name2')
  {'name1': 'foo', 'name2': 'bar'}

We may also get a slice (or subrange) or the columns in a row. To do this,
use the `column_start` and `column_finish` parameters. If one of these may be
left empty to allow the slice to extend to the end of the row in one direction.
Also note that `column_finish` is inclusive. Assuming we've inserted several
columns with names '1' through '9', we could do the following:

.. code-block:: python

  >>> col_fam.get('row_key', column_start='5', column_finish='7')
  {'5':'foo', '6':'bar', '7':'baz'}

There are also two ways to get multiple rows at the same time.
The first is to specify them by name using
:meth:`~pycassa.columnfamily.ColumnFamily.multiget()`:

.. code-block:: python

  >>> col_fam.multiget(['row_key1', 'row_key2'])
  {'row_key1': {'name':'val'}, 'row_key2': {'name':'val'}}

The other way is to get a range of keys at once by using
:meth:`~pycassa.columnfamily.ColumnFamily.get_range()`. The parameter
`finish` is also inclusive here too.  Assuming we've inserted some rows
with keys 'row_key1' through 'row_key9', we could do this:

.. code-block:: python

  >>> col_fam.get_range(start='row_key5', finish='row_key7')
  {'row_key5': {'name':'val'}, 'row_key6': {'name':'val'}, 'row_key7': {'name':'val'}}

It's also possible to specify a set of columns or a slice for 
:meth:`~pycassa.columnfamily.ColumnFamily.multiget()` and
:meth:`~pycassa.columnfamily.ColumnFamily.get_range()` just like we did for
:meth:`~pycassa.columnfamily.ColumnFamily.get()`.

Counting
--------
If you just want to know how many columns are in a row, you can use
:meth:`~pycassa.columnfamily.ColumnFamily.get_count()`:

.. code-block:: python

  >>> col_fam.get_count('row_key')
  3

Indexes
-------
Cassandra 0.7.0 adds support for secondary indexes, which allow you to
efficiently get only rows which match a certain expression.

To use secondary indexes with Cassandra, you need to specify what columns
will be indexed.  In a ``cassandra.yaml`` file, this might look like:

::

  - name: Indexed1
    column_type: Standard
    column_metadata:
      - name: birthdate
        validator_class: LongType
        index_type: KEYS

In order to use :meth:`~pycassa.columnfamily.ColumnFamily.get_indexed_slices()`
to get data from Indexed1 using the indexed column, we need to create an 
:class:`~pycassa.cassandra.ttypes.IndexClause` which contains 
:class:`~pycassa.cassandra.ttypes.IndexExpression`.  The module
:mod:`pycassa.index` is designed to make this easier.

Suppose we are only interested in rows where birthdate is 1984. We might do
the following:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(connection, 'Indexed1')
  >>> from pycassa.index import *
  >>> index_exp = create_index_expression('birthdate', 1984)
  >>> index_clause = create_index_clause([index_exp])
  >>> col_fam.get_indexed_slices(index_clause)
  {'winston smith': {'birthdate': 1984}}

Although at least one
:class:`~pycassa.cassandra.ttypes.IndexExpression` in every clause
must be on an indexed column, you may also have other expressions which are
on non-indexed columns.
