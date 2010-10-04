pycassaShell 
============
**pycassaShell** is an interactive Cassandra python shell. It is useful for
exploring Cassandra, especially for those who are just beginning.

Requirements
------------
Python 2.4 or later is required.

It is **strongly** recommended that you have
`IPython <http://ipython.scipy.org/moin/>`_, an enhanced interactive
python shell, installed. This gives you tab completion, colors, and working
arrow keys!

On Debian based systems, this can be installed by:

.. code-block:: bash

    apt-get install ipython

Alternatively, if ``easy_install`` is available:

.. code-block:: bash

    easy_install ipython

Usage
-----

.. code-block:: bash

    pycassaShell -k KEYSPACE [OPTIONS]

The available options are:

* ``-H``, ``--host`` - The hostname to connect to. Defaults to 'localhost'
* ``-p``, ``--port`` - The port to connect to. Defaults to 9160.
* ``-u``, ``--user`` - If authentication or authorization are enabled, this username is used.
* ``-P``, ``--passwd`` - If authentication or authorization are enabled, this password is used.
* ``-S``, ``--streaming`` - Use a streaming transport. Works with Cassandra 0.6.x and below.
* ``-F``, ``--framed`` - Use a streaming transport. Works with Cassandra 0.7.x. This is the default.

Once connected, the client is available as ``CLIENT``.  This can be used as you
would typically use a **pycassa** connection.  For example:

.. code-block:: python

    >>> column_family = pycassa.ColumnFamily(CLIENT, 'Standard1')
    >>> column_family.insert('key', {'colname': 'val'})
    1286048238391943
    >>> CLIENT.describe_keyspace('Keyspace1')
    KsDef(strategy_options=None, cf_defs=[CfDef(comment='', min_compaction_threshold=4, name='SuperLongSubInt', column_type='Super', preload_row_cache=False, key_cache_size=200000.0, gc_grace_seconds=0, column_metadata=[], keyspace='Keyspace1', default_validation_class='org.apache.cassandra.db.marshal.BytesType', max_compaction_threshold=32, subcomparator_type='org.apache.cassandra.db.marshal.IntegerType', read_repair_chance=1.0, comparator_type='org.apache.cassandra.db.marshal.LongType', id=1021, row_cache_size=0.0)], strategy_class='org.apache.cassandra.locator.SimpleStrategy', name='Keyspace1', replication_factor=1)

For working with data in Cassandra, check out :mod:`~pycassa.columnfamily`. As
shown in the example, once you make a :class:`~pycassa.columnfamily.ColumnFamily`
object, its easy to insert and retrieve data from Cassandra.

If you are interested in the keyspace and column family definitions,
the :meth:`system_*()` calls are also availble directly on ``CLIENT``.
These allow you to create, drop, or edit keyspaces and column families,
although they are not very friendly to use.
