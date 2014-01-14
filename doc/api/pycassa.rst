:mod:`pycassa` - Exceptions and Enums
=====================================

.. module:: pycassa

.. exception:: AuthenticationException

    The credentials supplied when creating a connection did not validate,
    indicating a bad username or password.

.. exception:: AuthorizationException

    The user that is currently logged in for a connection was not permitted
    to perform an action.

.. exception:: InvalidRequestException

    Something about the request made was invalid or malformed. The request
    should not be repeated without modification. Sometimes checking the server
    logs may help debug what was wrong with the request.

.. exception:: NotFoundException

    The row requested does not exist, or the slice requested was empty.

.. exception:: UnavailableException

    Not enough replicas are up to satisfy the requested consistency level.

.. exception:: TimedOutException

    The replica node did not respond to the coordinator node within
    ``rpc_timeout_in_ms`` (as configured in :file:`cassandra.yaml`),
    typically indicating that the replica is overloaded or just went
    down.

.. class:: pycassa.ConsistencyLevel

    .. data:: ANY

        Only requires that one replica receives the write *or* the coordinator
        stores a hint to replay later. Valid only for writes.

    .. data:: ONE

        Only one replica needs to respond to consider the operation a success

    .. data:: QUORUM

        `ceil(RF/2)` replicas must respond to consider the operation a success

    .. data:: ALL

        All replicas must respond to consider the operation a success

    .. data:: LOCAL_QUORUM

        Requres a quorum of replicas in the local datacenter

    .. data:: LOCAL_ONE

        Has the same behavior as ONE, except that Only replicas in the local
        datacenter are sent queries

    .. data:: EACH_QUORUM

        Requres a quorum of replicas in each datacenter

    .. data:: TWO

        Two replicas must respond to consider the operation a success

    .. data:: THREE

        Three replicas must respond to consider the operation a success
