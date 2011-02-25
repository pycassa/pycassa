Changelog
=========

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
- Renamed :meth:`pycassa.system_manager.SystemManager.get_keyspace_description()`
  to :meth:`pycassa.system_manager.SystemManager.get_keyspace_column_families()`
  and deprecated the previous name
- Moved :meth:`pycassa.system_manager.SystemManager.describe_keyspace()`
  and :meth:`pycassa.system_manager.SystemManager.describe_column_family()`
  to pycassaShell describe_keyspace() and describe_column_family()

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
- fixed overflow error on
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
