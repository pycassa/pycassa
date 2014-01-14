"""
Data type definitions that are used when converting data to and from
the binary format that the data will be stored in.

In addition to the default classes included here, you may also define
custom types by creating a new class that extends :class:`~.CassandraType`.
For example, IntString, which stores an arbitrary integer as a string, may
be defined as follows:

.. code-block:: python

    >>> class IntString(pycassa.types.CassandraType):
    ...
    ...    @staticmethod
    ...    def pack(intval):
    ...        return str(intval)
    ...
    ...    @staticmethod
    ...    def unpack(strval):
    ...        return int(strval)

"""

import calendar
from datetime import datetime

import pycassa.marshal as marshal

__all__ = ('CassandraType', 'BytesType', 'LongType', 'IntegerType',
           'AsciiType', 'UTF8Type', 'TimeUUIDType', 'LexicalUUIDType',
           'CounterColumnType', 'DoubleType', 'FloatType', 'DecimalType',
           'BooleanType', 'DateType', 'OldPycassaDateType',
           'IntermediateDateType', 'CompositeType',
           'UUIDType', 'DynamicCompositeType', 'TimestampType')

class CassandraType(object):
    """
    A data type that Cassandra is aware of and knows
    how to validate and sort. All of the other classes in this
    module are subclasses of this class.

    If `reversed` is true and this is used as a column comparator,
    the columns will be sorted in reverse order.


    The `default` parameter only applies to use of this
    with ColumnFamilyMap, where `default` is used if a row
    does not contain a column corresponding to this item.
    """

    def __init__(self, reversed=False, default=None):
        self.reversed = reversed
        self.default = default
        if not hasattr(self.__class__, 'pack'):
            self.pack = marshal.packer_for(self.__class__.__name__)
        if not hasattr(self.__class__, 'unpack'):
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

class Int32Type(CassandraType):
    """ Stores data as a 4 byte integer """
    pass

class AsciiType(CassandraType):
    """ Stores data as ASCII text """
    pass

class UTF8Type(CassandraType):
    """ Stores data as UTF8 encoded text """
    pass

class UUIDType(CassandraType):
    """ Stores data as a type 1 or type 4 UUID """
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
    """ Stores data as a 4 byte float """
    pass

class DecimalType(CassandraType):
    """
    Stores an unlimited precision decimal number.  `decimal.Decimal`
    objects are used by pycassa to represent these objects.
    """
    pass

class BooleanType(CassandraType):
    """ Stores data as a 1 byte boolean """
    pass

class DateType(CassandraType):
    """
    An 8 byte timestamp. This will be returned
    as a :class:`datetime.datetime` instance by pycassa. Either
    :class:`datetime` instances or timestamps will be accepted.

    .. versionchanged:: 1.7.0
        Prior to 1.7.0, datetime objects were expected to be in
        local time. In 1.7.0 and beyond, naive datetimes are
        assumed to be in UTC and tz-aware objects will be
        automatically converted to UTC for storage in Cassandra.
    """
    pass

TimestampType = DateType


def _to_timestamp(v, use_micros=False):
    # Expects Value to be either date or datetime
    if use_micros:
        scale = 1e6
        micro_scale = 1.0
    else:
        scale = 1e3
        micro_scale = 1e3

    try:
        converted = calendar.timegm(v.utctimetuple())
        converted = (converted * scale) + \
                    (getattr(v, 'microsecond', 0) / micro_scale)
    except AttributeError:
        # Ints and floats are valid timestamps too
        if type(v) not in marshal._number_types:
            raise TypeError('DateType arguments must be a datetime or timestamp')

        converted = v * scale
    return long(converted)

class OldPycassaDateType(CassandraType):
    """
    This class can only read and write the DateType format
    used by pycassa versions 1.2.0 to 1.5.0.

    This formats store the number of microseconds since the
    unix epoch, rather than the number of milliseconds, which
    is what cassandra-cli and other clients supporting DateType
    use.

    .. versionchanged:: 1.7.0
        Prior to 1.7.0, datetime objects were expected to be in
        local time. In 1.7.0 and beyond, naive datetimes are
        assumed to be in UTC and tz-aware objects will be
        automatically converted to UTC for storage in Cassandra.
    """

    @staticmethod
    def pack(v, *args, **kwargs):
        ts = _to_timestamp(v, use_micros=True)
        return marshal._long_packer.pack(ts)

    @staticmethod
    def unpack(v):
        ts = marshal._long_packer.unpack(v)[0] / 1e6
        return datetime.utcfromtimestamp(ts)

class IntermediateDateType(CassandraType):
    """
    This class is capable of reading either the DateType
    format by pycassa versions 1.2.0 to 1.5.0 or the correct
    format used in pycassa 1.5.1+.  It will only write the
    new, correct format.

    This type is a good choice when you are using DateType
    as the validator for non-indexed column values and you are
    in the process of converting from thee old format to
    the new format.

    It almost certainly *should not be used* for row keys,
    column names (if you care about the sorting), or column
    values that have a secondary index on them.

    .. versionchanged:: 1.7.0
        Prior to 1.7.0, datetime objects were expected to be in
        local time. In 1.7.0 and beyond, naive datetimes are
        assumed to be in UTC and tz-aware objects will be
        automatically converted to UTC for storage in Cassandra.
    """

    @staticmethod
    def pack(v, *args, **kwargs):
        ts = _to_timestamp(v, use_micros=False)
        return marshal._long_packer.pack(ts)

    @staticmethod
    def unpack(v):
        raw_ts = marshal._long_packer.unpack(v)[0] / 1e3

        try:
            return datetime.utcfromtimestamp(raw_ts)
        except ValueError:
            # convert from bad microsecond format to millis
            corrected_ts = raw_ts / 1e3
            return datetime.utcfromtimestamp(corrected_ts)

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

    @property
    def pack(self):
        return marshal.get_composite_packer(composite_type=self)

    @property
    def unpack(self):
        return marshal.get_composite_unpacker(composite_type=self)

class DynamicCompositeType(CassandraType):
    """
    A type composed of one or more components, each of
    which have their own type.  When sorted, items are
    primarily sorted by their first component, secondarily
    by their second component, and so on.

    Unlike CompositeType, DynamicCompositeType columns
    need not all be of the same structure. Each column
    can be composed of different component types.

    Components are specified using a 2-tuple made up of
    a comparator type and value. Aliases for comparator
    types can optionally be specified with a dictionary 
    during instantiation.

    """

    def __init__(self, *aliases):
        self.aliases = {}
        for alias in aliases:
            if isinstance(alias, dict):
                self.aliases.update(alias)

    def __str__(self):
        aliases = []
        for k, v in self.aliases.iteritems():
            aliases.append(k + '=>' + str(v))
        return "DynamicCompositeType(" + ", ".join(aliases) + ")"

