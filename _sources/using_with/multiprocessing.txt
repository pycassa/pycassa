.. _using_with_multiprocessing:

Using with :mod:`multiprocessing`
---------------------------------
The :class:`~.ConnectionPool` class is not :mod:`multiprocessing`-safe.
If you're using pycassa with multiprocessing, be sure to create one
:class:`~.ConnectionPool` per process. Creating a :class:`~.ConnectionPool`
before forking and sharing it among processes is inherently unsafe and will
result in race conditions.
