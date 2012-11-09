.. _pycassa-shell:

pycassaShell 
============
**pycassaShell** is an interactive Cassandra python shell. It is useful for
exploring Cassandra, especially for those who are just beginning.

Requirements
------------
Python 2.6 or later is required.

Make sure you have **pycassa** installed as shown in :ref:`installing`.

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
* ``-F``, ``--framed`` - Use a framed transport. Works with Cassandra 0.7.x. This is the default.

When pycassaShell starts, it creates a
:class:`~pycassa.columnfamily.ColumnFamily` for every existing column family and prints
the names of the objects. You can use these to easily insert and retrieve data from Cassandra.

.. code-block:: python

    >>> STANDARD1.insert('key', {'colname': 'val'})
    1286048238391943
    >>> STANDARD1.get('key')
    {'colname': 'val'}

.. _pycassa-shell-sys-man:

If you are interested in the keyspace and column family definitions,
**pycassa** provides several methods that can be used with ``SYSTEM_MANAGER``:

* :meth:`~pycassa.system_manager.SystemManager.create_keyspace()`
* :meth:`~pycassa.system_manager.SystemManager.alter_keyspace()`
* :meth:`~pycassa.system_manager.SystemManager.drop_keyspace()`
* :meth:`~pycassa.system_manager.SystemManager.create_column_family()`
* :meth:`~pycassa.system_manager.SystemManager.alter_column_family()`
* :meth:`~pycassa.system_manager.SystemManager.drop_column_family()`
* :meth:`~pycassa.system_manager.SystemManager.alter_column()`
* :meth:`~pycassa.system_manager.SystemManager.create_index()`
* :meth:`~pycassa.system_manager.SystemManager.drop_index()`

Example usage:

.. code-block:: python

    >>> describe_keyspace('Keyspace1')

    Name:                                Keyspace1

    Replication Strategy:                SimpleStrategy
    Replication Factor:                  1

    Column Families:
       Indexed1
       Standard2
       Standard1
       Super1

    >>> describe_column_family('Keyspace1', 'Indexed1')

    Name:                                Indexed1
    Description:                         
    Column Type:                         Standard

    Comparator Type:                     BytesType
    Default Validation Class:            BytesType

    Cache Sizes
      Row Cache:                         Disabled
      Key Cache:                         200000 keys

    Read Repair Chance:                  100.0%

    GC Grace Seconds:                    864000

    Compaction Thresholds
      Min:                               4
      Max:                               32

    Memtable Flush After Thresholds
      Throughput:                        63 MiB
      Operations:                        295312 operations
      Time:                              60 minutes

    Cache Save Periods
      Row Cache:                         Disabled
      Key Cache:                         3600 seconds

    Column Metadata
      - Name:                            birthdate
        Value Type:                      LongType
        Index Type:                      KEYS
        Index Name:                      None

    >>> SYSTEM_MANAGER.create_keyspace('Keyspace1', strategy_options={"replication_factor": "1"})
    >>> SYSTEM_MANAGER.create_column_family('Keyspace1', 'Users', comparator_type=INT_TYPE)
    >>> SYSTEM_MANAGER.alter_column_family('Keyspace1', 'Users', key_cache_size=100)
    >>> SYSTEM_MANAGER.create_index('Keyspace1', 'Users', 'birthdate', LONG_TYPE, index_name='bday_index')
    >>> SYSTEM_MANAGER.drop_keyspace('Keyspace1')

