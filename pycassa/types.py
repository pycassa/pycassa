import pycassa.marshal as marshal

class CassandraType(object):

    def __init__(self, reversed=False, default=None):
        """
        A data type that Cassandra is aware of and knows
        how to validate and sort.

        If `reversed` is true and this is used as a column comparator,
        the columns will be sorted in reverse order.

        This is also used to specify fields to be persisted when
        a class is used with :class:`~.ColumnFamilyMap`.

        The `default` parameter only applies to use of this
        with ColumnFamilyMap, where `default` is used if a row
        does not contain a column corresponding to this item.

        .. versionadded:: 1.2.0

        """
        self.reversed = reversed
        self.default = default
        self.pack = marshal.packer_for(self.__class__.__name__)
        self.unpack = marshal.unpacker_for(self.__class__.__name__)

    def __str__(self):
        return self.__class__.__name__ + "(reversed=" + str(self.reversed).lower() + ")"

class BytesType(CassandraType):
    """ Stores data as a byte array """
    pass

class LongType(CassandraType):
    """ Stores data as an 8 byte integer """
    pass

class IntegerType(CassandraType):
    """
    Stores data as a variable-length integer. This
    is a more compact format for storing small integers
    than :class:`~.LongType`, and the limits
    on the size of the integer are much higher.

    .. versionchanged:: 1.2.0
        Prior to 1.2.0, this was always stored as a 4 byte
        integer.

    """
    pass

class AsciiType(CassandraType):
    """ Stores data as ASCII text """
    pass

class UTF8Type(CassandraType):
    """ Stores data as UTF8 encoded text """
    pass

class TimeUUIDType(CassandraType):
    """ Stores data as a version 1 UUID """
    pass

class LexicalUUIDType(CassandraType):
    """ Stores data as a non-version 1 UUID """
    pass

class CounterColumnType(CassandraType):
    """ A 64bit counter column """
    pass

class DoubleType(CassandraType):
    """
    Stores data as an 8 byte double.

    .. versionadded:: 1.2.0

    """
    pass

class FloatType(CassandraType):
    """
    Stores data as an 4 byte float.

    .. versionadded:: 1.2.0

    """
    pass

class BooleanType(CassandraType):
    """
    Stores data as a 1 byte boolean.

    .. versionadded:: 1.2.0

    """
    pass

class DateType(CassandraType):
    """
    An 8 byte timestamp. This will be returned
    as a :class:`datetime.datetime` instance by pycassa. Either
    :class:`datetime` instances or timestamps will be accepted.

    .. versionadded:: 1.2.0

    """
    pass

class CompositeType(CassandraType):
    """
    A type composed of one or more components, each of
    which have their own type.  When sorted, items are
    primarily sorted by their first component, secondarily
    by their second component, and so on.

    Each of `*components` should be an instance of
    a subclass of :class:`CassandraType`.

    .. seealso:: :ref:`composite-types`

    .. versionadded:: 1.2.0

    """

    def __init__(self, *components):
        self.components = components

    def __str__(self):
        return "CompositeType(" + ", ".join(map(str, self.components)) + ")"
