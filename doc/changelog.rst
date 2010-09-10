Changelog
=========

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
- Added `~pycassa.columnfamily.ColumnFamily.truncate()`
- Use Thrift API 11.0.0
