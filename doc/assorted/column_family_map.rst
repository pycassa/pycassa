Class Mapping with Column Family Map
====================================
You can map existing classes to column families using
:class:`~pycassa.columnfamilymap.ColumnFamilyMap`.

.. code-block:: python

  >>> class Test(object):
  ...     string_column       = pycassa.String(default='Your Default')
  ...     int_str_column      = pycassa.IntString(default=5)
  ...     float_str_column    = pycassa.FloatString(default=8.0)
  ...     float_column        = pycassa.Float64(default=0.0)
  ...     datetime_str_column = pycassa.DateTimeString() # default=None

The defaults will be filled in whenever you retrieve instances from the
Cassandra server and the column doesn't exist. If you want to add a
column in the future, you can simply add the relevant attribute to the class
and the default value will be used when you get old instances.

:class:`~pycassa.types.IntString`, :class:`~pycassa.types.FloatString`, and
:class:`~pycassa.types.DateTimeString` all use string representations for
storage. :class:`~pycassa.types.Float64` is stored as a double and is
native-endian. Be aware of any endian issues if you use it on different
architectures, or perhaps make your own column type.

.. code-block:: python

  >>> pool = pycassa.ConnectionPool('Keyspace1')
  >>> cf = pycassa.ColumnFamily(pool, 'Standard1', autopack_names=False, autopack_values=False)
  >>> Test.objects = pycassa.ColumnFamilyMap(Test, cf)

.. note:: As shown in the example, `autopack_names` and `autopack_values` should
          be set to ``False`` when a ColumnFamily is used with a ColumnFamilyMap.

All the functions are exactly the same, except that they return
instances of the supplied class when possible.

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
  5

.. code-block:: python

  >>> Test.objects.remove(t)
  1261395603906864
  >>> Test.objects.get(t.key)
  Traceback (most recent call last):
  ...
  cassandra.ttypes.NotFoundException: NotFoundException()

You may also use a ColumnFamilyMap with super columns:

.. code-block:: python

  >>> Test.objects = pycassa.ColumnFamilyMap(Test, cf)
  >>> t = Test()
  >>> t.key = 'key1'
  >>> t.super_column = 'super1'
  >>> t.string_column = 'foobar'
  >>> t.int_str_column = 5
  >>> t.float_column = t.float_str_column = 35.8
  >>> t.datetime_str_column = datetime.now()
  >>> Test.objects.insert(t)
  >>> Test.objects.get(t.key)
  {'super1': <__main__.Test object at 0x20ab350>}
  >>> Test.objects.multiget([t.key])
  {'key1': {'super1': <__main__.Test object at 0x20ab550>}}
