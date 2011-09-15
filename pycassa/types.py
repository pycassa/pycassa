import pycassa.marshal as marshal

class CassandraType(object):

    def __init__(self, reversed=False, default=None):
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
    """ Stores data as an 4 byte integer """
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
    """ Stores data as an 8 byte double """
    pass

class FloatType(CassandraType):
    """ Stores data as an 4 byte float """
    pass

class BooleanType(CassandraType):
    """ Stores data as an 1 byte boolean """
    pass

class DateType(CassandraType):
    """ A timestamp as a 8 byte integer """
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
    """

    def __init__(self, *components):
        self.components = components

    def __str__(self):
        return "CompositeType(" + ", ".join(map(str, self.components)) + ")"
