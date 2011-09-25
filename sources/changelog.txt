Changelog
=========

Changes in Version 1.2.1
------------------------
This is strictly a bug-fix release addressing a few
issues created in 1.2.0.

Bug Fixes
~~~~~~~~~
- Correctly check for Counters in :class:`.ColumnFamily`
  when setting `default_validation_class`
- Pass kwargs in :class:`.ColumnFamilyMap` to
  :class:`.ColumnFamily`
- Avoid potential UnboundLocal in :meth:`.ConnectionPool.execute`
  when :meth:`~.ConnectionPool.get` fails
- Fix ez_setup dependency/bundling so that package installations
  using easy_install or pip don't fail without ez_setup installed

Changes in Version 1.2.0
------------------------
This should be a fairly smooth upgrade from pycassa 1.1. The
primary changes that may introduce minor incompatibilities are
the changes to :class:`.ColumnFamilyMap` and the automatic
skipping of "ghost ranges" in :meth:`.ColumnFamily.get_range()`.

Features
~~~~~~~~
- Add :meth:`.ConnectionPool.fill()`
- Add :class:`~.FloatType`, :class:`~.DoubleType`, 
  :class:`~.DateType`, and :class:`~.BooleanType` support.
- Add :class:`~.CompositeType` support for static composites.
  See :ref:`composite-types` for more details.
- Add `timestamp`, `ttl` to :meth:`.ColumnFamilyMap.insert()`
  params 
- Support variable-length integers with :class:`~.IntegerType`.
  This allows more space-efficient small integers as well as
  integers that exceed the size of a long.
- Make :class:`~.ColumnFamilyMap` a subclass of
  :class:`~.ColumnFamily` instead of using one as a component.
  This allows all of the normal adjustments normally done
  to a :class:`~.ColumnFamily` to be done to a :class:`~.ColumnFamilyMap`
  instead. See :ref:`column-family-map` for examples of
  using the new version.
- Expose the following :class:`~.ConnectionPool` attributes,
  allowing them to be altered after creation: 
  :attr:`~.ConnectionPool.max_overflow`, :attr:`~.ConnectionPool.pool_timeout`,
  :attr:`~.ConnectionPool.recycle`, :attr:`~.ConnectionPool.max_retries`,
  and :attr:`~.ConnectionPool.logging_name`.
  Previously, these were all supplied as constructor arguments.
  Now, the preferred way to set them is to alter the attributes
  after creation. (However, they may still be set in the
  constructor by using keyword arguments.)
- Automatically skip "ghost ranges" in :meth:`ColumnFamily.get_range()`.
  Rows without any columns will not be returned by the generator,
  and these rows will not count towards the supplied `row_count`.

Bug Fixes
~~~~~~~~~
- Add connections to :class:`~.ConnectionPool` more readily
  when `prefill` is ``False``.
  Before this change, if the ConnectionPool was created with
  ``prefill=False``, connections would only be added to the pool
  when there was concurrent demand for connections.
  After this change, if ``prefill=False`` and ``pool_size=N``, the
  first `N` operations will each result in a new connection
  being added to the pool.
- Close connection and adjust the :class:`~.ConnectionPool`'s
  connection count after a :exc:`.TApplicationException`. This
  exception generally indicates programmer error, so it's not
  extremely common.
- Handle typed keys that evaluate to ``False``

Deprecated
~~~~~~~~~~
- :meth:`.ConnectionPool.recreate()`
- :meth:`.ConnectionPool.status()`

Miscellaneous
~~~~~~~~~~~~~
- Better failure messages for :class:`~.ConnectionPool` failures
- More efficient packing and unpacking
- More efficient multi-column inserts in :meth:`.ColumnFamily.insert()`
  and :meth:`.ColumnFamily.batch_insert()`
- Prefer Python 2.7's :class:`collections.OrderedDict` over the
  bundled version when available

Changes in Version 1.1.1
------------------------

Features
~~~~~~~~
- Add ``max_count`` and ``column_reversed`` params to :meth:`~.ColumnFamily.get_count()`
- Add ``max_count`` and ``column_reversed`` params to :meth:`~.ColumnFamily.multiget_count()`

Bug Fixes
~~~~~~~~~
- Don't retry operations after a ``TApplicationException``. This exception
  is reserved for programmatic errors (such as a bad API parameters), so
  retries are not needed.
- If the read_consistency_level kwarg was used in a :class:`~.ColumnFamily`
  constructor, it would be ignored, resulting in a default read consistency
  level of :const:`ONE`. This did not affect the read consistency level if it was
  specified in any other way, including per-method or by setting the
  :attr:`~.ColumnFamily.read_consistency_level` attribute.

Changes in Version 1.1.0
------------------------
This release adds compatibility with Cassandra 0.8, including support
for counters and key_validation_class. This release is
backwards-compatible with Cassandra 0.7, and can support running against
a mixed cluster of both Cassandra 0.7 and 0.8.


Changes related to Cassandra 0.8
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- Addition of :data:`~.system_manager.COUNTER_COLUMN_TYPE` to
  :mod:`~.system_manager`.

- Several new column family attributes, including ``key_validation_class``,
  ``replicate_on_write``, ``merge_shards_chance``, ``row_cache_provider``,
  and ``key_alias``.

- The new :meth:`.ColumnFamily.add()` and :meth:`.ColumnFamily.remove_counter()`
  methods.

- Support for counters in :mod:`pycassa.batch` and 
  :meth:`.ColumnFamily.batch_insert()`.

- Autopacking of keys based on ``key_validation_class``.

Other Features
~~~~~~~~~~~~~~
- :meth:`.ColumnFamily.multiget()` now has a `buffer_size` parameter

- :meth:`.ColumnFamily.multiget_count()` now returns rows
  in the order that the keys were passed in, similar to how
  :meth:`~.ColumnFamily.multiget()` behaves. It also uses
  the :attr:`~.ColumnFamily.dict_class` attribute for the containing
  class instead of always using a :class:`dict`.

- Autpacking behavior is now more transparent and configurable,
  allowing the user to get functionality similar to the CLI's
  ``assume`` command, whereby items are packed and unpacked as
  though they were a certain data type, even if Cassandra does
  not use a matching comparator type or validation class. This
  behavior can be controlled through the following attributes:

  - :attr:`.ColumnFamily.column_name_class`

  - :attr:`.ColumnFamily.super_column_name_class` 

  - :attr:`.ColumnFamily.key_validation_class` 

  - :attr:`.ColumnFamily.default_validation_class`

  - :attr:`.ColumnFamily.column_validators`

- A :class:`.ColumnFamily` may reload its schema to handle
  changes in validation classes with :meth:`.ColumnFamily.load_schema()`.

Bug Fixes
~~~~~~~~~
There were several related issues with overlow in :class:`.ConnectionPool`:

- Connection failures when a :class:`.ConnectionPool` was in a state
  of overflow would not result in adjustment of the overflow counter,
  eventually leading the :class:`.ConnectionPool` to refuse to create
  new connections.

- Settings of -1 for :attr:`.ConnectionPool.overflow` erroneously caused
  overflow to be disabled.

- If overflow was enabled in conjunction with `prefill` being disabled,
  the effective overflow limit was raised to ``max_overflow + pool_size``.

Other
~~~~~
- Overflow is now disabled by default in :class:`.ConnectionPool`.

- :class:`.ColumnFamilyMap` now sets the underlying :class:`.ColumnFamily`'s
  :attr:`~.ColumnFamily.autopack_names` and
  :attr:`~.ColumnFamily.autopack_values` attributes to ``False`` upon
  construction.

- Documentation and tests will no longer be included in the
  packaged tarballs.

Removed Deprecated Items
~~~~~~~~~~~~~~~~~~~~~~~~
The following deprecated items have been removed:

- :meth:`.ColumnFamilyMap.get_count()`

- The `instance` parameter from :meth:`.ColumnFamilyMap.get_indexed_slices()`

- The :class:`~.types.Int64` Column type.

- :meth:`.SystemManager.get_keyspace_description()`

Deprecated
~~~~~~~~~~
Athough not technically deprecated, most :class:`.ColumnFamily`
constructor arguments should instead be set by setting the
corresponding attribute on the :class:`.ColumnFamily` after
construction. However, all previous constructor arguments
will continue to be supported if passed as keyword arguments.

Changes in Version 1.0.8
------------------------
- Pack :class:`.IndexExpression` values in :meth:`~.ColumnFamilyMap.get_indexed_slices()`
  that are supplied through the :class:`.IndexClause` instead of just the `instance`
  parameter.

- Column names and values which use Cassandra's IntegerType are unpacked as though they
  are in a BigInteger-like format. This is (backwards) compatible with the format
  that pycassa uses to pack IntegerType data. This fixes an incompatibility with
  the format that cassandra-cli and other clients use to pack IntegerType data.

- Restore Python 2.5 compatibility that was broken through out of order keyword
  arguments in :class:`.ConnectionWrapper`.

- Pack `column_start` and `column_finish` arguments in :class:`.ColumnFamily`
  ``*get*()`` methods when the `super_column` parameter is used.

- Issue a :class:`DeprecationWarning` when a method, parameter, or class that
  has been deprecated is used. Most of these have been deprecated for several
  releases, but no warnings were issued until now.

- Deprecations are now split into separate sections for each release in the
  changelog.

Deprecated
~~~~~~~~~~
- The `instance` parameter of :meth:`ColumnFamilyMap.get_indexed_slices()`


Changes in Version 1.0.7
------------------------
- Catch KeyError in :meth:`pycassa.columnfamily.ColumnFamily.multiget()` empty
  row removal. If the same non-existent key was passed multiple times, a
  :exc:`KeyError` was raised when trying to remove it from the OrderedDictionary
  after the first removal. The :exc:`KeyError` is caught and ignored now.

- Handle connection failures during retries. When a connection fails, it tries to
  create a new connection to replace itself. Exceptions during this process were
  not properly handled; they are now handled and count towards the retry count for
  the current operation.

- Close connection when a :exc:`MaximumRetryException` is raised. Normally a connection
  is closed when an operation it is performing fails, but this was not happening
  for the final failure that triggers the :exc:`MaximumRetryException`. 


Changes in Version 1.0.6
------------------------
- Add :exc:`EOFError` to the list of exceptions that cause a connection swap and retry

- Improved autopacking efficiency for AsciiType, UTF8Type, and BytesType

- Preserve sub-second timestamp precision in datetime arguments for insertion
  or slice bounds where a TimeUUID is expected. Previously, precision below a
  second was lost.

- In a :exc:`MaximumRetryException`'s message, include details about the last
  :exc:`Exception` that caused the :exc:`MaximumRetryException` to be raised

- :meth:`pycassa.pool.ConnectionPool.status()` now always reports a non-negative
  overflow; 0 is now used when there is not currently any overflow

- Created :class:`pycassa.types.Long` as a replacement for :class:`pycassa.types.Int64`.
  :class:`Long` uses big-endian encoding, which is compatible with Cassandra's LongType,
  while :class:`Int64` used little-endian encoding.

Deprecated
~~~~~~~~~~
- :class:`pycassa.types.Int64` has been deprecated in favor of :class:`pycassa.types.Long`


Changes in Version 1.0.5
------------------------
- Assume port 9160 if only a hostname is given 

- Remove super_column param from :meth:`pycassa.columnfamily.ColumnFamily.get_indexed_slices()`

- Enable failover on functions that previously lacked it

- Increase base backoff time to 0.01 seconds

- Add a timeout paremeter to :class:`pycassa.system_manager.SystemManger`

- Return timestamp on single-column inserts 


Changes in Version 1.0.4
------------------------
- Fixed threadlocal issues that broke multithreading

- Fix bug in :meth:`pycassa.columnfamily.ColumnFamily.remove()` when a super_column
  argument is supplied

- Fix minor PoolLogger logging bugs

- Added :meth:`pycassa.system_manager.SystemManager.describe_partitioner()`

- Added :meth:`pycassa.system_manager.SystemManager.describe_snitch()`

- Added :meth:`pycassa.system_manager.SystemManager.get_keyspace_properties()`

- Moved :meth:`pycassa.system_manager.SystemManager.describe_keyspace()`
  and :meth:`pycassa.system_manager.SystemManager.describe_column_family()`
  to pycassaShell describe_keyspace() and describe_column_family()

Deprecated
~~~~~~~~~~
- Renamed :meth:`pycassa.system_manager.SystemManager.get_keyspace_description()`
  to :meth:`pycassa.system_manager.SystemManager.get_keyspace_column_families()`
  and deprecated the previous name


Changes in Version 1.0.3
------------------------
- Fixed supercolumn slice bug in get()

- pycassaShell now runs scripts with execfile to allow for multiline statements

- 2.4 compatability fixes


Changes in Version 1.0.2
------------------------
- Failover handles a greater set of potential failures

- pycassaShell now loads/reloads :class:`pycassa.columnfamily.ColumnFamily`
  instances when the underlying column family is created or updated

- Added an option to pycassaShell to run a script after startup

- Added :meth:`pycassa.system_manager.SystemManager.list_keyspaces()`


Changes in Version 1.0.1
------------------------
- Allow pycassaShell to be run without specifying a keyspace

- Added :meth:`pycassa.system_manager.SystemManager.describe_schema_versions()`


Changes in Version 1.0.0
------------------------
- Created the :class:`~pycassa.system_manager.SystemManager` class to
  allow for keyspace, column family, and index creation, modification,
  and deletion. These operations are no longer provided by a Connection
  class.

- Updated pycassaShell to use the SystemManager class

- Improved retry behavior, including exponential backoff and proper
  resetting of the retry attempt counter

- Condensed connection pooling classes into only
  :class:`pycassa.pool.ConnectionPool` to provide a simpler API

- Changed :meth:`pycassa.connection.connect()` to return a
  connection pool

- Use more performant Thrift API methods for :meth:`insert()`
  and :meth:`get()` where possible

- Bundled :class:`~pycassa.util.OrderedDict` and set it as the
  default dictionary class for column families

- Provide better :exc:`TypeError` feedback when columns are the wrong
  type

- Use Thrift API 19.4.0

Deprecated
~~~~~~~~~~
- :meth:`ColumnFamilyMap.get_count()` has been deprecated. Use
  :meth:`ColumnFamily.get_count()` instead.


Changes in Version 0.5.4
------------------------
- Allow for more backward and forward compatibility

- Mark a server as being down more quickly in
  :class:`~pycassa.connection.Connection`


Changes in Version 0.5.3
------------------------
- Added :class:`~pycassa.columnfamily.PooledColumnFamily`, which makes
  it easy to use connection pooling automatically with a ColumnFamily.


Changes in Version 0.5.2
------------------------
- Support for adding/updating/dropping Keyspaces and CFs
  in :class:`pycassa.connection.Connection`

- :meth:`~pycassa.columnfamily.ColumnFamily.get_range()` optimization
  and more configurable batch size

- batch :meth:`~pycassa.columnfamily.ColumnFamily.get_indexed_slices()`
  similar to :meth:`.ColumnFamily.get_range()`

- Reorganized pycassa logging

- More efficient packing of data types

- Fix error condition that results in infinite recursion

- Limit pooling retries to only appropriate exceptions

- Use Thrift API 19.3.0


Changes in Version 0.5.1
------------------------
- Automatically detect if a column family is a standard column family
  or a super column family

- :meth:`~pycassa.columnfamily.ColumnFamily.multiget_count()` support

- Allow preservation of key order in
  :meth:`~pycassa.columnfamily.ColumnFamily.multiget()` if an ordered
  dictionary is used

- Convert timestamps to v1 UUIDs where appropriate

- pycassaShell documentation

- Use Thrift API 17.1.0


Changes in Version 0.5.0
------------------------
- Connection Pooling support: :mod:`pycassa.pool`

- Started moving logging to :mod:`pycassa.logger`

- Use Thrift API 14.0.0


Changes in Version 0.4.3
------------------------
- Autopack on CF's default_validation_class

- Use Thrift API 13.0.0


Changes in Version 0.4.2
------------------------
- Added batch mutations interface: :mod:`pycassa.batch`

- Made bundled thrift-gen code a subpackage of pycassa

- Don't attempt to reencode already encoded UTF8 strings


Changes in Version 0.4.1
------------------------
- Added :meth:`~pycassa.columnfamily.ColumnFamily.batch_insert()`

- Redifined :meth:`~pycassa.columnfamily.ColumnFamily.insert()`
  in terms of :meth:`~pycassa.columnfamily.ColumnFamily.batch_insert()`

- Fixed UTF8 autopacking

- Convert datetime slice args to uuids when appropriate

- Changed how thrift-gen code is bundled

- Assert that the major version of the thrift API is the same on the
  client and on the server

- Use Thrift API 12.0.0


Changes in Version 0.4.0
------------------------
- Added pycassaShell, a simple interactive shell

- Converted the test config from xml to yaml

- Fixed overflow error on
  :meth:`~pycassa.columnfamily.ColumnFamily.get_count()`

- Only insert columns which exist in the model object

- Make ColumnFamilyMap not ignore the ColumnFamily's dict_class

- Specify keyspace as argument to :meth:`~pycassa.connection.connect()`

- Add support for framed transport and default to using it

- Added autopacking for column names and values

- Added support for secondary indexes with
  :meth:`~pycassa.columnfamily.ColumnFamily.get_indexed_slices()`
  and :mod:`pycassa.index`

- Added :meth:`~pycassa.columnfamily.ColumnFamily.truncate()`

- Use Thrift API 11.0.0
