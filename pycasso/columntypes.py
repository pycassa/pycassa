from datetime import datetime
import struct
import time

__all__ = ['Column', 'DateTimeColumn', 'DateTimeStringColumn', 'Float64Column',
           'FloatStringColumn', 'Int64Column', 'IntStringColumn',
           'StringColumn']

class Column(object):
    def __init__(self, default=None):
        self.default = default

class DateTimeColumn(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('q')

    def pack(self, val):
        if not isinstance(val, datetime):
            raise TypeError('expected datetime, %s found' % type(val).__name__)
        return self.struct.pack(int(time.mktime(val.timetuple())))

    def unpack(self, val):
        return datetime.fromtimestamp(self.struct.unpack(val)[0])

class DateTimeStringColumn(Column):
    format = '%Y-%m-%d %H:%M:%S'
    def pack(self, val):
        if not isinstance(val, datetime):
            raise TypeError('expected datetime, %s found' % type(val).__name__)
        return val.strftime(self.format)

    def unpack(self, val):
        return datetime.strptime(val, self.format)

class Float64Column(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('d')

    def pack(self, val):
        if not isinstance(val, float):
            raise TypeError('expected float, %s found' % type(val).__name__)
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class FloatStringColumn(Column):
    def pack(self, val):
        if not isinstance(val, float):
            raise TypeError('expected float, %s found' % type(val).__name__)
        return str(val)

    def unpack(self, val):
        return float(val)

class Int64Column(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('q')

    def pack(self, val):
        if not isinstance(val, int):
            raise TypeError('expected int, %s found' % type(val).__name__)
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class IntStringColumn(Column):
    def pack(self, val):
        if not isinstance(val, int):
            raise TypeError('expected int, %s found' % type(val).__name__)
        return str(val)

    def unpack(self, val):
        return int(val)

class StringColumn(Column):
    def pack(self, val):
        if not isinstance(val, str) and not isinstance(val, unicode):
            raise TypeError('expected str or unicode, %s found' % type(val).__name__)
        return val

    def unpack(self, val):
        return val
