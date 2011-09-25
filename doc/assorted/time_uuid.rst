Version 1 UUIDs (TimeUUIDType)
==============================
Version 1 UUIDs are `frequently used for timelines <http://www.riptano.com/docs/0.8/data_model/uuids>`_
instead of timestamps.  Normally, this makes it difficult to get a slice of
columns for some time range or to create a column name or value for some
specific time.

To make this easier, if a :class:`datetime` object or a timestamp with the
same precision as the output of ``time.time()`` is passed where a TimeUUID
is expected, **pycassa** will convert that into a :class:`uuid.UUID` with an equivalent
timestamp component.

Suppose we have something like Twissandra's public timeline but with TimeUUIDs
for column names. If we want to get all tweets that happened yesterday, we
can do:

.. code-block:: python

  >>> import datetime
  >>> line = pycassa.ColumnFamily(pool, 'Userline')
  >>> today = datetime.datetime.utcnow()
  >>> yesterday = today - datetime.timedelta(days=1)
  >>> tweets = line.get('__PUBLIC__', column_start=yesterday, column_finish=today)

Now, suppose there was a tweet that was supposed to be posted on December 11th
at 8:02:15, but it was dropped and now we need to put it in the public timeline.
There's no need to generate a UUID, we can just pass another datetime object instead:

.. code-block:: python

  >>> from datetime import datetime
  >>> line = pycassa.ColumnFamily(pool, 'Userline')
  >>> time = datetime(2010, 12, 11, 8, 2, 15)
  >>> line.insert('__PUBLIC__', {time: 'some tweet stuff here'})

One limitation of this is that you can't ask for one specific column with a
TimeUUID name by passing a :class:`datetime` through something like the `columns` parameter
for :meth:`get()`; this is because there is no way to know the non-timestamp
components of the UUID ahead of time.  Instead, simply pass the same :class:`datetime`
object for both `column_start` and `column_finish` and you'll get one or more
columns for that exact moment in time.

Note that Python does not sort UUIDs the same way that Cassandra does. When Cassandra sorts V1
UUIDs it first compares the time component, and then the raw bytes of the UUID. Python on the 
other hand just sorts the raw bytes. If you need to sort UUIDs in Python the same way Cassandra
does you will want to use something like this:

.. code-block:: python

  >>> import uuid, random
  >>> uuids = [uuid.uuid1() for _ xrange(10)]
  >>> random.shuffle(uuids)
  >>> improperly_sorted = sorted(uuids)
  >>> properly_sorted = sorted(uuids, key=lambda k: (k.time, k.bytes))
