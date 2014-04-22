.. _composite-types:

Composite Types
===============
pycassa currently supports static CompositeTypes. DynamicCompositeType support
is planned.

Creating a CompositeType Column Family
--------------------------------------
When creating a column family, you can specify a CompositeType comparator
or validator using :class:`~.CompositeType` in conjunction with
the other types in :mod:`pycassa.types`.

.. code-block:: python

    >>> from pycassa.types import *
    >>> from pycassa.system_manager import *
    >>>
    >>> sys = SystemManager()
    >>> comparator = CompositeType(LongType(reversed=True), AsciiType())
    >>> sys.create_column_family("Keyspace1", "CF1", comparator_type=comparator)

This example creates a column family with column names that have two components.
The first component is a :class:`~.LongType`, sorted in reverse order; the
second is a normally sorted :class:`~.AsciiType`.

You may put an arbitrary number of components in a :class:`~.CompositeType`,
and each component may be reversed or not.

Insert CompositeType Data
-------------------------
When inserting data, where a :class:`~.CompositeType` is expected, you should supply
a tuple which includes all of the components.

Continuing the example from above:

.. code-block:: python

    >>> cf = ColumnFamily(pool, "CF1")
    >>> cf.insert("key", {(1234, "abc"): "colval"})

When dealing with composite keys or column values, supply tuples in exactly
the same manner.

Fetching CompositeType Data
---------------------------
:class:`.CompositeType` data is also returned in a tuple format.

.. code-block:: python

    >>> cf.get("key")
    {(1234, "abc"): "colval"}

When fetching a slice of columns, slice ends are specified using tuples as
well.  However, you are only required to supply at least the first component
of the :class:`.CompositeType`; elements may be left off of the end of the
tuple in order to slice columns based on only the first or first few
components.

For example, suppose our `comparator_type` is 
``CompositeType(LongType, AsciiType, LongType)``. Valid slice ends would
include ``(1, )``, ``(1, "a")``, and ``(1, "a", 2011)``.

If you supply a slice start and a slice end that only specify the first
component, you will get back all columns where the first component falls
in that range, regardless of what the value of the other components is.

When slicing columns, the second component is only compared to the second
component of the slice start if the first component of the column name
matches the first component of the slice start. Likewise with the slice
end, the second component will only be checked if the first components
match.  In essence, components after the first only serve as "tie-breakers"
at the slice ends, and have no effect in the "middle" of the slice. Keep
in mind the sorted order of the columns within Cassandra, and that when you
get a slice of columns, you can only get a contiguous slice, not separate
chunks out of the row.

Inclusive or Exclusive Slice Ends
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, slice ends are inclusive on the final component you supply
for that slice end. This means that if you give a ``column_finish`` of
``(123, "b")``, then columns named ``(123, "a", 2011)``, ``(123, "b", 0)``,
and ``(123, "b" 123098123012)`` would all be returned.

With composite types, you have the option to make the slice start and
finish exclusive.  To do so, replace the final component in your slice end
with a tuple like ``(value, False)``. (Think of the ``False`` as being
short for ``inclusive=False``. You can also explicitly specify ``True``,
but this is redundant.)  Now, if you gave a ``column_finish`` of
``(123, ("b", False))``, you would only get back ``(123, "a", 2011)``.
The same principle applies for ``column_start``.
