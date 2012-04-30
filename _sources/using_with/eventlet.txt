.. _using_with_eventlet:

Using with Eventlet
===================
Because the :class:`~.ConnectionPool` class uses concurrency primitives from
the :mod:`threading` module, you can use :mod:`eventlet` green threads after 
`monkey patching <http://eventlet.net/doc/basic_usage.html#patching-functions>`_
the standard library. Specifically, the :mod:`threading` and :mod:`socket`
modules must monkey-patched.

Be aware that you may need to install `dnspython <http://pypi.python.org/pypi/dnspython>`_
in order to connect to your nodes.
