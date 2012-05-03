.. _column-family-map:

Class Mapping with Column Family Map
====================================
You can map existing classes to column families using
:class:`~pycassa.columnfamilymap.ColumnFamilyMap`.

To specify the fields to be persisted, use any of the
subclasses of :class:`pycassa.types.CassandraType` available
in :mod:`pycassa.types`.

.. code-block:: python

  >>> from pycassa.types import *
  >>> class User(object):
  ...     key = LexicalUUIDType()
  ...     name = Utf8Type()
  ...     age = IntegerType()
  ...     height = FloatType()
  ...     score = DoubleType(default=0.0)
  ...     joined = DateType()

The defaults will be filled in whenever you retrieve instances from the
Cassandra server and the column doesn't exist. If you want to add a
column in the future, you can simply add the relevant attribute to the class
and the default value will be used when you get old instances.

.. code-block:: python

  >>> from pycassa.pool import ConnectionPool
  >>> from pycassa.columnfamilymap import ColumnFamilyMap
  >>>
  >>> pool = ConnectionPool('Keyspace1')
  >>> cfmap = ColumnFamilyMap(User, pool, 'users')

All the functions are exactly the same as for :class:`ColumnFamily`,
except that they return instances of the supplied class when possible.

.. code-block:: python

  >>> from datetime import datetime
  >>> import uuid
  >>>
  >>> key = uuid.uuid4()
  >>>
  >>> user = User()
  >>> user.key = key
  >>> user.name = 'John'
  >>> user.age = 18
  >>> user.height = 5.9
  >>> user.joined = datetime.now()
  >>> cfmap.insert(user)
  1261395560186855

.. code-block:: python

  >>> user = cfmap.get(key)
  >>> user.name
  "John"
  >>> user.age
  18

.. code-block:: python

  >>> users = cfmap.multiget([key1, key2])
  >>> print users[0].name
  "John"
  >>> for user in cfmap.get_range():
  ...    print user.name
  "John"
  "Bob"
  "Alex"

.. code-block:: python

  >>> cfmap.remove(user)
  1261395603906864
  >>> cfmap.get(user.key)
  Traceback (most recent call last):
  ...
  cassandra.ttypes.NotFoundException: NotFoundException()
