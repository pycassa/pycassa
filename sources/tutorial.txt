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


Super Columns
-------------
Cassandra allows you to group columns in "super columns". In a
``cassandra.yaml`` file, this looks like this:

::

  - name: Super1
    column_type: Super 

To use a super column in **pycassa**, you only need to do one thing:
when creating the :class:`~pycassa.columnfamily.ColumnFamily`,
set the `super` argument to be ``True``. If this is done, you can
use a super column family just like a regular one, except there is an
extra level in the dictionary:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(connection, 'Super1', super=True)
  >>> col_fam.insert('row_key', {'supercol_name': {'col_name': 'col_val'}})
  1354491238721345
  >>> col_fam.get('row_key')
  {'supercol_name': {'col_name': 'col_val'}}

Typed Column Names and Values
-----------------------------
In Cassandra 0.7, you can specify a comparator type for column names
and a validator type for column values.

The types available are:

* BytesType - no type
* IntegerType - 32 bit integer
* LongType - 64 bit integer
* AsciiType - ASCII string
* UTF8Type - UTF8 encoded string
* TimeUUIDType - version 1 UUID (timestamp based)
* LexicalUUID - non-version 1 UUID

The column name comparator types affect how columns are sorted within
a row. You can use these with standard column families as well as with
super column families; with super column families, the subcolumns may
even have a different comparator type.  Here's an example ``cassandra.yaml``:

::

  - name: StandardInt
    column_type: Standard
    compare_with: IntegerType

  - name: SuperLongSubAscii
    column_type: Super
    compare_with: LongType
    compare_subcolumns_with: AsciiType

Cassandra still requires you to pack these types into a format it can
understand by using something like :meth:`struct.pack()`.  Fortunately,
when **pycassa** sees that a column family uses these types, it knows
to pack and unpack these data types automatically for you. So, if we want to
write to the StandardInt column family, we can do the following:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(connection, 'StandardInt')
  >>> col_fam.insert('row_key', {42: 'some_val'})
  1354491238721387
  >>> col_fam.get('row_key')
  {42: 'some_val'}

Notice that 42 is an integer here, not a string.

As mentioned above, Cassandra also offers validators on column values with
the same set of types.  Validators can be set for an entire column family,
for individual columns, or both.  Here's another example ``cassandra.yaml``:

::

  - name: AllLongs
    column_type: Standard
    default_validation_class: LongType

  - name: OneUUID
    column_type: Standard
    column_metadata:
      - name: uuid
        validator_class: TimeUUIDType

  - name: LongsExceptUUID
    column_type: Standard
    default_validation_class: LongType
    column_metadata:
      - name: uuid
        validator_class: TimeUUIDType

**pycassa** knows to pack these column values automatically too:

.. code-block:: python

  >>> import uuid
  >>> col_fam = pycassa.ColumnFamily(connection, 'LongsExceptUUID')
  >>> col_fam.insert('row_key', {'foo': 123456789, 'uuid': uuid.uuid1()})
  1354491238782746
  >>> col_fam.get('row_key')
  {'foo': 123456789, 'uuid': UUID('5880c4b8-bd1a-11df-bbe1-00234d21610a')}

Of course, if **pycassa**'s automatic behavior isn't working for you, you
can turn it off when you create the
:class:`~pycassa.columnfamily.ColumnFamily`:

.. code-block:: python

  >>> col_fam = pycassa.ColumnFamily(connection, 'Standard1',
  ...                                autopack_names=False,
  ...                                autopack_values=False)

This mainly needs to be done when working with
:class:`~pycassa.columnfamily.ColumnFamilyMap`.

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

Connection Pooling
------------------
Several types of connection pools are offered for different usages:

* :class:`~pycassa.pool.QueuePool` – typical connection pool that maintains a queue of open connections
* :class:`~pycassa.pool.SingletonThreadPool` – one connection per thread
* :class:`~pycassa.pool.StaticPool` – a single connection used for all operations
* :class:`~pycassa.pool.NullPool` – no pooling is performed, but failover is supported
* :class:`~pycassa.pool.AssertionPool` – asserts that at most one connection is open at a time; useful for debugging

For example, to create a :class:`~pycassa.pool.QueuePool` and use a connection:

.. code-block:: python

  >>> pool = pycassa.QueuePool(keyspace='Keyspace1')
  >>> connection = pool.get()
  >>> cf = pycassa.ColumnFamily(connection, 'Standard1')
  >>> cf.insert('key', {'col': 'val'})
  1354491238782746
  >>> connection.return_to_pool()

Automatic retries (or failover) are supported with all types of pools except
for :class:`~pycassa.pool.StaticPool`. This means that if any operation fails,
it will be transparently retried on other servers until it succeeds or a
maximum number of failures is reached.

Class Mapping with Column Family Map
------------------------------------
You can map existing classes to column families using
:class:`~pycassa.columnfamily.ColumnFamilyMap`.

.. code-block:: python

  >>> class Test(object):
  ...     string_column       = pycassa.String(default='Your Default')
  ...     int_str_column      = pycassa.IntString(default=5)
  ...     float_str_column    = pycassa.FloatString(default=8.0)
  ...     float_column        = pycassa.Float64(default=0.0)
  ...     datetime_str_column = pycassa.DateTimeString() # default=None

The defaults will be filled in whenever you retrieve instances from the
Cassandra server and the column doesn't exist. If, for example, you add
columns in the future, you simply add the relevant column and the default
will be there when you get old instances.

:class:`~pycassa.types.IntString`, :class:`~pycassa.types.FloatString`, and
:class:`~pycassa.types.DateString`, all use string representations for storage.
:class:`~pycassa.types.Float64` is stored as a double and is native-endian.
Be aware of any endian issues if you use it on different architectures, or
perhaps make your own column type.

.. code-block:: python

  >>> Test.objects = pycassa.ColumnFamilyMap(Test, cf)

All the functions are exactly the same, except that they return instances of the supplied class when possible.

.. code-block:: python

  >>> t = Test()
  >>> t.key = 'maptest'
  >>> t.string_column = 'string test'
  >>> t.int_str_column = 18
  >>> t.float_column = t.float_str_column = 35.8
  >>> from datetime import datetime
  >>> t.datetime_str_column = datetime.now()
  >>> Test.objects.insert(t)
  1261395560186855

.. code-block:: python

  >>> Test.objects.get(t.key).string_column
  'string test'
  >>> Test.objects.get(t.key).int_str_column
  18
  >>> Test.objects.get(t.key).float_column
  35.799999999999997
  >>> Test.objects.get(t.key).datetime_str_column
  datetime.datetime(2009, 12, 23, 17, 6, 3)

.. code-block:: python

  >>> Test.objects.multiget([t.key])
  {'maptest': <__main__.Test object at 0x7f8ddde0b9d0>}
  >>> list(Test.objects.get_range())
  [<__main__.Test object at 0x7f8ddde0b710>]
  >>> Test.objects.get_count(t.key)
  7

.. code-block:: python

  >>> Test.objects.remove(t)
  1261395603906864
  >>> Test.objects.get(t.key)
  Traceback (most recent call last):
  ...
  cassandra.ttypes.NotFoundException: NotFoundException()

Note that, as mentioned previously, get_range() may continue to return removed rows for some time:

.. code-block:: python

  >>> Test.objects.remove(t)
  1261395603756875
  >>> list(Test.objects.get_range())
  [<__main__.Test object at 0x7fac9c85ea90>]
  >>> list(Test.objects.get_range())[0].string_column
  'Your Default'
