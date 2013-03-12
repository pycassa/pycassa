Tutorial
========

This tutorial is intended as an introduction to working with
Cassandra and **pycassa**.

.. toctree::
    :maxdepth: 2

Prerequisites
-------------
Before we start, make sure that you have **pycassa**
:doc:`installed <installation>`. In the Python shell, the following
should run without raising an exception:

.. code-block:: python

  >>> import pycassa

This tutorial also assumes that a Cassandra instance is running on the
default host and port. Read the `instructions for getting started
with Cassandra <http://www.datastax.com/docs/0.7/getting_started/index>`_ if
you need help with this.

You can start Cassandra like so:

.. code-block:: bash

  $ pwd
  ~/cassandra
  $ bin/cassandra -f

Creating a Keyspace and Column Families
---------------------------------------
We need to create a keyspace and some column families to work with.  There
are two good ways to do this: using cassandra-cli, or using pycassaShell. Both
are documented below.

Using cassandra-cli
^^^^^^^^^^^^^^^^^^^
The cassandra-cli utility is included with Cassandra. It allows you to create
and modify the schema, explore or modify data, and examine a few things about
your cluster.  Here's how to create the keyspace and column family we need
for this tutorial:

.. code-block:: none

    user@~ $ cassandra-cli 
    Welcome to cassandra CLI.

    Type 'help;' or '?' for help. Type 'quit;' or 'exit;' to quit.
    [default@unknown] connect localhost/9160;
    Connected to: "Test Cluster" on localhost/9160
    [default@unknown] create keyspace Keyspace1;
    4f9e42c4-645e-11e0-ad9e-e700f669bcfc
    Waiting for schema agreement...
    ... schemas agree across the cluster
    [default@unknown] use Keyspace1;
    Authenticated to keyspace: Keyspace1
    [default@Keyspace1] create column family ColumnFamily1;
    632cf985-645e-11e0-ad9e-e700f669bcfc
    Waiting for schema agreement...
    ... schemas agree across the cluster
    [default@Keyspace1] quit;
    user@~ $

This connects to a local instance of Cassandra and creates a keyspace
named 'Keyspace1' with a column family named 'ColumnFamily1'.

You can find further `documentation for the CLI online
<http://www.datastax.com/docs/1.1/dml/using_cli>`_.

Using pycassaShell
^^^^^^^^^^^^^^^^^^
:ref:`pycassa-shell` is an interactive Python shell that is included
with **pycassa**.  Upon starting, it sets up many of the objects that
you typically work with when using **pycassa**.  It provides most of the
functionality that cassandra-cli does, but also gives you a full Python
environment to work with.

Here's how to create the keyspace and column family:

.. code-block:: none

    user@~ $ pycassaShell 
    ----------------------------------
    Cassandra Interactive Python Shell
    ----------------------------------
    Keyspace: None
    Host: localhost:9160

    ColumnFamily instances are only available if a keyspace is specified with -k/--keyspace

    Schema definition tools and cluster information are available through SYSTEM_MANAGER.

.. code-block:: python

    >>> SYSTEM_MANAGER.create_keyspace('Keyspace1', strategy_options={"replication_factor": "1"})
    >>> SYSTEM_MANAGER.create_column_family('Keyspace1', 'ColumnFamily1')

Connecting to Cassandra
-----------------------
The first step when working with **pycassa** is to connect to the
running cassandra instance:

.. code-block:: python

  >>> from pycassa.pool import ConnectionPool
  >>> pool = ConnectionPool('Keyspace1')

The above code will connect by default to ``localhost:9160``. We can
also specify the host (or hosts) and port explicitly as follows:

.. code-block:: python

  >>> pool = ConnectionPool('Keyspace1', ['localhost:9160'])

This creates a small connection pool for use with a
:class:`~pycassa.columnfamily.ColumnFamily` . See `Connection Pooling`_
for more details.

Getting a ColumnFamily
----------------------
A column family is a collection of rows and columns in Cassandra,
and can be thought of as roughly the equivalent of a table in a
relational database. We'll use one of the column families that
are included in the default schema file:

.. code-block:: python

  >>> from pycassa.pool import ConnectionPool
  >>> from pycassa.columnfamily import ColumnFamily
  >>>
  >>> pool = ConnectionPool('Keyspace1')
  >>> col_fam = ColumnFamily(pool, 'ColumnFamily1')

If you get an error about the keyspace or column family not
existing, make sure you created the keyspace and column family as
shown above.

Inserting Data
--------------
To insert a row into a column family we can use the
:meth:`~pycassa.columnfamily.ColumnFamily.insert` method:

.. code-block:: python

  >>> col_fam.insert('row_key', {'col_name': 'col_val'})
  1354459123410932

We can also insert more than one column at a time:

.. code-block:: python

  >>> col_fam.insert('row_key', {'col_name':'col_val', 'col_name2':'col_val2'})
  1354459123410932

And we can insert more than one row at a time:

.. code-block:: python

  >>> col_fam.batch_insert({'row1': {'name1': 'val1', 'name2': 'val2'},
  ...                       'row2': {'foo': 'bar'}})
  1354491238721387

Getting Data
------------
There are many more ways to get data out of Cassandra than there are
to insert data.

The simplest way to get data is to use
:meth:`~pycassa.columnfamily.ColumnFamily.get()`:

.. code-block:: python

  >>> col_fam.get('row_key')
  {'col_name': 'col_val', 'col_name2': 'col_val2'}

Without any other arguments, :meth:`~pycassa.columnfamily.ColumnFamily.get()`
returns every column in the row (up to `column_count`, which defaults to 100).
If you only want a few of the columns and you know them by name, you can
specify them using a `columns` argument:

.. code-block:: python

  >>> col_fam.get('row_key', columns=['col_name', 'col_name2'])
  {'col_name': 'col_val', 'col_name2': 'col_val2'}

We may also get a slice (or subrange) of the columns in a row. To do this,
use the `column_start` and `column_finish` parameters.  One or both of these may
be left empty to allow the slice to extend to one or both ends.
Note that `column_finish` is inclusive.

.. code-block:: python

    >>> for i in range(1, 10):
    ...     col_fam.insert('row_key', {str(i): 'val'})
    ... 
    1302542571215334
    1302542571218485
    1302542571220599
    1302542571221991
    1302542571223388
    1302542571224629
    1302542571225859
    1302542571227029
    1302542571228472
    >>> col_fam.get('row_key', column_start='5', column_finish='7')
    {'5': 'val', '6': 'val', '7': 'val'}

Sometimes you want to get columns in reverse sorted order.  A common
example of this is getting the last N columns from a row that
represents a timeline.  To do this, set `column_reversed` to ``True``.
If you think of the columns as being sorted from left to right, when
`column_reversed` is ``True``, `column_start` will determine the right
end of the range while `column_finish` will determine the left.

Here's an example of getting the last three columns in a row:

.. code-block:: python

  >>> col_fam.get('row_key', column_reversed=True, column_count=3)
  {'9': 'val', '8': 'val', '7': 'val'}

There are a few ways to get multiple rows at the same time.
The first is to specify them by name using
:meth:`~pycassa.columnfamily.ColumnFamily.multiget()`:

.. code-block:: python

  >>> col_fam.multiget(['row1', 'row2'])
  {'row1': {'name1': 'val1', 'name2': 'val2'}, 'row_key2': {'foo': 'bar'}}

Another way is to get a range of keys at once by using
:meth:`~pycassa.columnfamily.ColumnFamily.get_range()`. The parameter
`finish` is also inclusive here, too.  Assuming we've inserted some rows
with keys 'row_key1' through 'row_key9', we can do this:

.. code-block:: python

  >>> result = col_fam.get_range(start='row_key5', finish='row_key7')
  >>> for key, columns in result:
  ...     print key, '=>', columns
  ...
  'row_key5' => {'name':'val'}
  'row_key6' => {'name':'val'}
  'row_key7' => {'name':'val'}

.. note:: Cassandra must be using an OrderPreservingPartitioner for you to be
          able to get a meaningful range of rows; the default, RandomPartitioner,
          stores rows in the order of the MD5 hash of their keys. See
          http://www.datastax.com/docs/1.1/cluster_architecture/partitioning.

The last way to get multiple rows at a time is to take advantage of
secondary indexes by using :meth:`~pycassa.columnfamily.ColumnFamily.get_indexed_slices()`,
which is described in the :ref:`secondary-indexes` section.

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

If you only want to get a count of the number of columns that are inside
of a slice or have particular names, you can do that as well:

.. code-block:: python

  >>> col_fam.get_count('row_key', columns=['foo', 'bar'])
  2
  >>> col_fam.get_count('row_key', column_start='foo')
  3

You can also do this in parallel for multiple rows using
:meth:`~pycassa.columnfamily.ColumnFamily.multiget_count()`:

.. code-block:: python

  >>> col_fam.multiget_count(['fib0', 'fib1', 'fib2', 'fib3', 'fib4'])
  {'fib0': 1, 'fib1': 1, 'fib2': 2, 'fib3': 3, 'fib4': 5'}

.. code-block:: python

  >>> col_fam.multiget_count(['fib0', 'fib1', 'fib2', 'fib3', 'fib4'],
  ...                        columns=['col1', 'col2', 'col3'])
  {'fib0': 1, 'fib1': 1, 'fib2': 2, 'fib3': 3, 'fib4': 3'}

.. code-block:: python

  >>> col_fam.multiget_count(['fib0', 'fib1', 'fib2', 'fib3', 'fib4'],
  ...                        column_start='col1', column_finish='col3')
  {'fib0': 1, 'fib1': 1, 'fib2': 2, 'fib3': 3, 'fib4': 3'}

Typed Column Names and Values
-----------------------------
Within a column family, column names have a specified `comparator type`
which controls how they are sorted. Column values and row keys may also
have a `validation class`, which validates that inserted values are
the correct type.

The different types available include ASCII strings, integers, dates,
UTF8, raw bytes, UUIDs, and more. See :mod:`pycassa.types` for a full
list.

Cassandra requires you to pack column names and values into a format it can
understand by using something like :meth:`struct.pack()`.  Fortunately,
when **pycassa** sees that a column family has a particular comparator type
or validation class, it knows to pack and unpack these data types automatically
for you. So, if we want to write to the StandardInt column family, which has
an IntegerType comparator, we can do the following:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(pool, 'StandardInt')
  >>> col_fam.insert('row_key', {42: 'some_val'})
  1354491238721387
  >>> col_fam.get('row_key')
  {42: 'some_val'}

Notice that 42 is an integer here, not a string.

As mentioned above, Cassandra also offers validators on column values and keys
with the same set of types. Column value validators can be set for an entire
column family, for individual columns, or both.  **pycassa** knows to pack these
column values automatically too. Suppose we have a `Users` column family with
two columns, ``name`` and ``age``, with types UTF8Type and IntegerType:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(pool, 'Users')
  >>> col_fam.insert('thobbs', {'name': 'Tyler', 'age': 24})
  1354491238782746
  >>> col_fam.get('thobbs')
  {'name': 'Tyler', 'age': 24}

Of course, if **pycassa**'s automatic behavior isn't working for you, you
can turn it off or change it using :attr:`~.ColumnFamily.autopack_names`,
:attr:`~.ColumnFamily.autopack_values`, :attr:`~.ColumnFamily.column_name_class`,
:attr:`~.ColumnFamily.default_validation_class`, and so on.  

Connection Pooling
------------------
Pycassa uses connection pools to maintain connections to Cassandra servers.
The :class:`~pycassa.pool.ConnectionPool` class is used to create the connection
pool.  After creating the pool, it may be used to create multiple
:class:`~pycassa.columnfamily.ColumnFamily` objects.

.. code-block:: python

  >>> pool = pycassa.ConnectionPool('Keyspace1', pool_size=20)
  >>> standard_cf = pycassa.ColumnFamily(pool, 'Standard1')
  >>> standard_cf.insert('key', {'col': 'val'})
  1354491238782746
  >>> super_cf = pycassa.ColumnFamily(pool, 'Super1')
  >>> super_cf.insert('key2', {'column' : {'col': 'val'}})
  1354491239779182
  >>> standard_cf.get('key')
  {'col': 'val'}
  >>> pool.dispose()

Automatic retries (or "failover") happen by default with ConectionPools.
This means that if any operation fails, it will be transparently retried
on other servers until it succeeds or a maximum number of failures is reached.
