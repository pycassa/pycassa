Changelog
=========

Changes in Version 1.11.0
-------------------------

Features
~~~~~~~~
- Upgrade Thrift interface to 19.36.1, which adds support for the
  ``LOCAL_ONE consistency`` level and the ``populate_io_cache_on_flush``
  column family attribute.

Bug Fixes
~~~~~~~~~
- Return timestamp from ``remove()`` in stub ColumnFamily

Miscellaneous
~~~~~~~~~~~~~
- Upgrade bundled ``ez_setup.py``


Changes in Version 1.10.0
-------------------------
This release only adds one feature: support for Cassandra
1.2's atomic batches.

Features
~~~~~~~~
- Add support for Cassandra 1.2+ atomic batches through a new
  ``atomic`` parameter for :class:`.batch.Mutator`,
  :class:`.batch.CfMutator`, and :meth:`.ColumnFamily.batch()`.

Changes in Version 1.9.1
------------------------
This release fixes a few edge cases around connection pooling that
can affect long-running applications.  It also adds token range
support to :meth:`.ColumnFamily.get_range()`, which can be useful
for parallelizing full-ring scans.

Features
~~~~~~~~
- Add token range support to :meth:`.ColumnFamily.get_range()`

Bug Fixes
~~~~~~~~~
- Prevent possible double connection disposal when recycling connections
- Handle empty strings for IntegerType values
- Prevent closed connections from being returned to the pool.
- Ensure connection count is decremented when pool is disposed

Changes in Version 1.9.0
------------------------
This release adds a couple of minor new features and improves multithreaded
locking behavior in :class:`~.ConnectionPool`.  There should be no
backwards-compatibility concerns.

Features
~~~~~~~~
- Full support for ``column_start``, ``column_finish``, ``column_count``, and
  ``column_reversed`` parameters in :mod:`~.contrib.stubs`
- Addition of an ``include_ttl`` parameter to :class:`~.ColumnFamily` fetching
  methods which works like the existing ``include_timestamp`` parameter.

Bug Fixes
~~~~~~~~~
- Reduce the locked critical section in :class:`~.ConnectionPool`, primarily
  to make sure lock aquisition time is not ignored outside of the pool's
  ``timeout`` setting.

Changes in Version 1.8.0
------------------------
This release requires either Python 2.6 or 2.7. Python 2.4 and 2.5
are no longer supported. There are no concrete plans for Python 3
compatibility yet.

Features
~~~~~~~~
- Add configurable ``socket_factory`` attribute and constructor parameter
  to :class:`~.ConnectionPool` and :class:`~.SystemManager`.
- Add SSL support via the new ``socket_factory`` attribute.
- Add support for :class:`~.DynamicCompositeType`
- Add mock support through a new :mod:`pycassa.contrib.stubs` module

Bug Fixes
~~~~~~~~~
- Don't return closed connections to the pool. This was primarily a
  problem when operations failed after retrying up to the limit,
  resulting in a :exc:`~.MaximumRetryException` or
  :exc:`~.AllServersUnavailable`.
- Set keyspace for connection after logging in instead of before.
  This fixes authentication against Cassandra 1.2, which requires
  logging in prior to setting a keyspace.
- Specify correct UUID variant when creating v1 :class:`uuid.UUID` objects
  from datetimes or timestamps
- Add 900ns to v1 :class:`uuid.UUID` timestamps when the "max" TimeUUID for
  a specific datetime or timestamp is requested, such as a
  column slice end
- Also look at attributes of parent classes when creating
  columns from attributes in :class:`~.ColumnFamilyMap`

Other
~~~~~
- Upgrade bundled Thrift-generated python to 19.35.0, generated
  with Thrift 0.9.0.

Changes in Version 1.7.2
------------------------
This release fixes a minor bug and upgrades the bundled Cassandra
Thrift client interface to 19.34.0, matching Cassandra 1.2.0-beta1.
This doesn't affect any existing Thrift methods, only adds new ones
(that aren't yet utilized by pycassa), so there should not be any
breakage.

Bug Fixes
~~~~~~~~~
- Fix single-component composite packing
- Avoid cyclic imports during installation in setup.py

Other
~~~~~
- Travis CI integration

Changes in Version 1.7.1
------------------------
This release has few changes, and should make for a smooth upgrade
from 1.7.0.

Features
~~~~~~~~
- Add support for DecimalType: :class:`~.types.DecimalType`

Bug Fixes
~~~~~~~~~
- Fix bad slice ends when using :meth:`~.ColumnFamily.xget()` with
  composite columns and a `column_finish` parameter
- Fix bad documentation paths in debian packaging scripts

Other
~~~~~
- Add ``__version__`` and ``__version_info__`` attributes to the
  :mod:`pycassa` module


Changes in Version 1.7.0
------------------------
This release has a few relatively large changes in it: a new
connection pool stats collector, compatibility with Cassandra 0.7
through 1.1, and a change in timezone behavior for datetimes.

Before upgrading, take special care to make sure datetimes that you
pass to pycassa (for TimeUUIDType or DateType data) are in UTC, and
make sure your code expects to get UTC datetimes back in return.

Likewise, the SystemManager changes *should* be backwards compatible,
but there may be minor differences, mostly in
:meth:`~.SystemManager.create_column_family` and
:meth:`~.SystemManager.alter_column_family`. Be sure to test any code
that works programmatically with these.

Features
~~~~~~~~
- Added :class:`~.StatsLogger` for tracking :class:`~.ConnectionPool`
  metrics
- Full Cassandra 1.1 compatibility in :class:`.SystemManager`. To support
  this, all column family or keyspace attributes that have existed since
  Cassandra 0.7 may be used as keyword arguments for
  :meth:`~.SystemManager.create_column_family` and
  :meth:`~.SystemManager.alter_column_family`.  It is up to the user to
  know which attributes are available and valid for their version of
  Cassandra.
  As part of this change, the version-specific thrift-generated cassandra
  modules (``pycassa.cassandra.c07``, ``pycassa.cassandra.c08``, and
  ``pycassa.cassandra.c10``) have been replaced by ``pycassa.cassandra``.
  A minor related change is that individual connections now
  now longer ask for the node's API version, and that information is
  no longer stored as an attribute of the :class:`.ConnectionWrapper`.

Bug Fixes
~~~~~~~~~
- Fix :meth:`~.ColumnFamily.xget()` paging for non-string comparators
- Add :meth:`~.ColumnFamilyMap.batch_insert()` to :class:`.ColumnFamilyMap`
- Use `setattr` instead of directly updating the object's ``__dict__`` in
  :class:`.ColumnFamilyMap` to avoid breaking descriptors
- Fix single-column counter increments with :meth:`.ColumnFamily.insert()`
- Include `AuthenticationException` and `AuthorizationException` in
  the ``pycassa`` module
- Support counters in :meth:`~.ColumnFamily.xget()`
- Sort column families in pycassaShell for display
- Raise ``TypeError`` when bad keyword arguments are used when creating
  a :class:`.ColumnFamily` object

Other
~~~~~
All ``datetime`` objects create by pycassa now use UTC as their timezone
rather than the local timezone. Likewise, naive ``datetime`` objects that are
passed to pycassa are now assumed to be in UTC time, but ``tz_info`` is respected
if set.

Specifically, the types of data that you may need to make adjustments for
when upgrading are TimeUUIDType and DateType (including OldPycassaDateType
and IntermediateDateType).

Changes in Version 1.6.0
------------------------
This release adds a few minor features and several important bug fixes.

The most important change to take note of if you are using composite
comparators is the change to the default inclusive/exclusive behavior
for slice ends.

Other than that, this should be a smooth upgrade from 1.5.x.

Features
~~~~~~~~
- New script for easily building RPM packages
- Add request and parameter information to PoolListener callback
- Add :meth:`.ColumnFamily.xget()`, a generator version of
  :meth:`~.ColumnFamily.get()` that automatically pages over columns
  in reasonably sized chunks
- Add support for Int32Type, a 4-byte signed integer format
- Add constants for the highest and lowest possible TimeUUID values
  to :mod:`pycassa.util`

Bug Fixes
~~~~~~~~~
- Various 2.4 syntax errors
- Raise :exc:`~.AllServersUnavailable` if ``server_list`` is empty
- Handle custom types inside of composites
- Don't erase ``comment`` when updating column families
- Match Cassandra's sorting of TimeUUIDType values when the timestamps
  tie.  This could result in some columns being erroneously left off of
  the end of column slices when datetime objects or timestamps were used
  for ``column_start`` or ``column_finish``
- Use gevent's queue in place of the stdlib version when gevent monkeypatching
  has been applied
- Avoid sub-microsecond loss of precision with TimeUUID timestamps when using
  :func:`pycassa.util.convert_time_to_uuid`
- Make default slice ends inclusive when using ``CompositeType`` comparator
  Previously, the end of the slice was exclusive by default (as was the start
  of the slice when ``column_reversed`` was ``True``)

Changes in Version 1.5.1
------------------------
This release only affects those of you using DateType data,
which has been supported since pycassa 1.2.0.  If you are
using DateType, it is **very** important that you read this
closely.

DateType data is internally stored as an 8 byte integer timestamp.
Since version 1.2.0 of pycassa, the timestamp stored has counted
the number of *microseconds* since the unix epoch.  The actual
format that Cassandra standardizes on is *milliseconds* since the
epoch.

If you are only using pycassa, you probably won't have noticed any
problems with this. However, if you try to use cassandra-cli,
sstable2json, Hector, or any other client that supports DateType,
DateType data written by pycassa will appear to be far in the future.
Similarly, DateType data written by other clients will appear to
be in the past when loaded by pycassa.

This release changes the default DateType behavior to comply with
the standard, millisecond-based format.  **If you use DateType,
and you upgrade to this release without making any modifications,
you will have problems.**  Unfortunately, this is a bit of a tricky
situation to resolve, but the appropriate actions to take are detailed
below.

To temporarily continue using the old behavior, a new class
has been created: :class:`pycassa.types.OldPycassaDateType`.
This will read and write DateType data exactly the same as
pycassa 1.2.0 to 1.5.0 did.

If you want to convert your data to the new format, the other
new class, :class:`pycassa.types.IntermediateDateType`, may be useful.
It can read either the new or old format correctly (unless you have
used dates close to 1970 with the new format) and will write only
the new format. The best case for using this is if you have DateType
validated columns that don't have a secondary index on them.

To tell pycassa to use :class:`~.types.OldPycassaDateType` or
:class:`~.types.IntermediateDateType`, use the :class:`~.ColumnFamily`
attributes that control types: :attr:`~.ColumnFamily.column_name_class`,
:attr:`~.ColumnFamily.key_validation_class`,
:attr:`~.ColumnFamily.column_validators`, and so on.  Here's an example:

.. code-block:: python

    from pycassa.types import OldPycassaDateType, IntermediateDateType
    from pycassa.column_family import ColumnFamily
    from pycassa.pool import ConnectionPool

    pool = ConnectionPool('MyKeyspace', ['192.168.1.1'])

    # Our tweet timeline has a comparator_type of DateType
    tweet_timeline_cf = ColumnFamily(pool, 'tweets')
    tweet_timeline_cf.column_name_class = OldPycassaDateType()

    # Our tweet timeline has a comparator_type of DateType
    users_cf = ColumnFamily(pool, 'users')
    users_cf.column_validators['join_date'] = IntermediateDateType()

If you're using DateType for the `key_validation_class`, column names,
column values with a secondary index on them, or are using the DateType
validated column as a non-indexed part of an index clause with
`get_indexed_slices()` (eg. "where state = 'TX' and join_date > 2012"),
you need to be more careful about the conversion process, and
:class:`~.types.IntermediateDateType` probably isn't a good choice.

In most of cases, if you want to switch to the new date format,
a manual migration script to convert all existing DateType
data to the new format will be needed. In particular, if you
convert keys, column names, or indexed columns on a live data set,
be very careful how you go about it. If you need any assistance or
suggestions at all with migrating your data, please feel free to
send an email to tyler@datastax.com; I would be glad to help.

Changes in Version 1.5.0
------------------------
The main change to be aware of for this release is the
new no-retry behavior for counter operations.  If you have been
maintaining a separate connection pool with retries disabled
for usage with counters, you may discontinue that practice
after upgrading.

Features
~~~~~~~~
- By default, counter operations will not be retried
  automatically. This makes it easier to use a single
  connection pool without worrying about overcounting.

Bug Fixes
~~~~~~~~~
- Don't remove entire row when an empty list is supplied for the
  `columns` parameter of :meth:`~ColumnFamily.remove()` or the
  batch remove methods.
- Add python-setuptools to debian build dependencies
- Batch :meth:`~.Mutator.remove()` was not removing subcolumns
  when the specified supercolumn was 0 or other "falsey" values
- Don't request an extra row when reading fewer than `buffer_size`
  rows with :meth:`~.ColumnFamily.get_range()` or
  :meth:`~.ColumnFamily.get_indexed_slices()`.
- Remove `pool_type` from logs, which showed up as ``None`` in
  recent versions
- Logs were erroneously showing the same server for retries
  of failed operations even when the actual server being
  queried had changed

Changes in Version 1.4.0
------------------------
This release is primarily a bugfix release with a couple
of minor features and removed deprecated items.

Features
~~~~~~~~
- Accept column_validation_classes when creating or altering
  column families with SystemManager
- Ignore UNREACHABLE nodes when waiting for schema version
  agreement

Bug Fixes
~~~~~~~~~
- Remove accidental print statement in SystemManager
- Raise TypeError when unexpected types are used for
  comparator or validator types when creating or altering
  a Column Family
- Fix packing of column values using column-specific validators
  during batch inserts when the column name is changed by packing
- Always return timestamps from inserts
- Fix NameError when timestamps are used where a DateType is
  expected
- Fix NameError in python 2.4 when unpacking DateType objects
- Handle reading composites with trailing components missing
- Upgrade ez_setup.py to fix broken setuptools link

Removed Deprecated Items
~~~~~~~~~~~~~~~~~~~~~~~~
- :meth:`pycassa.connect()`
- :meth:`pycassa.connect_thread_local()`
- :meth:`.ConnectionPool.status()`
- :meth:`.ConnectionPool.recreate()`


Changes in Version 1.3.0
------------------------
This release adds full compatibility with Cassandra 1.0 and
removes support for schema manipulation in Cassandra 0.7.

In this release, schema manipulation should work with Cassandra 0.8
and 1.0, but not 0.7.  The data API should continue to work with all
three versions.

Bug Fixes
~~~~~~~~~
- Don't ignore `columns` parameter in :meth:`.ColumnFamilyMap.insert()`
- Handle empty instance fields in :meth:`.ColumnFamilyMap.insert()`
- Use the same default for `timeout` in :meth:`pycassa.connect()` as
  :class:`~.ConnectionPool` uses
- Fix typo which caused a different exception to be thrown when an
  :exc:`.AllServersUnavailable` exception was raised
- IPython 0.11 compatibility in pycassaShell
- Correct dependency declaration in :file:`setup.py`
- Add UUIDType to supported types

Features
~~~~~~~~
- The `filter_empty` parameter was added to
  :meth:`~.ColumnFamily.get_range()` with a default of ``True``; this
  allows empty rows to be kept if desired

Deprecated
~~~~~~~~~~
- :meth:`pycassa.connect()`
- :meth:`pycassa.connect_thread_local()`


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
