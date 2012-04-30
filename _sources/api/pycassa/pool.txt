:mod:`pycassa.pool` -- Connection Pooling
=========================================

.. automodule:: pycassa.pool

    .. autoclass:: pycassa.pool.ConnectionPool

        .. autoattribute:: max_overflow

        .. autoattribute:: pool_timeout

        .. autoattribute:: recycle

        .. autoattribute:: max_retries

        .. autoattribute:: logging_name

        .. automethod:: get

        .. automethod:: put

        .. automethod:: execute

        .. automethod:: fill

        .. automethod:: dispose

        .. automethod:: set_server_list

        .. automethod:: size

        .. automethod:: overflow

        .. automethod:: checkedin

        .. automethod:: checkedout

        .. automethod:: add_listener

    .. autoexception:: pycassa.pool.AllServersUnavailable

    .. autoexception:: pycassa.pool.NoConnectionAvailable

    .. autoexception:: pycassa.pool.MaximumRetryException

    .. autoexception:: pycassa.pool.InvalidRequestError

    .. autoclass:: pycassa.pool.ConnectionWrapper
       :members:

    .. autoclass:: pycassa.pool.PoolListener
       :members:
