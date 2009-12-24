from datetime import datetime
import struct
import time

__all__ = ['Column', 'BytesColumn', 'DateTimeColumn', 'DateTimeStringColumn',
           'Float64Column', 'FloatStringColumn', 'Int64Column',
           'IntStringColumn', 'StringColumn']

class Column(object):
    def __init__(self, default=None):
        self.default = default

class BytesColumn(Column):
    def pack(self, val):
        return val

    def unpack(self, val):
        return val

class DateTimeColumn(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('q')

    def pack(self, val):
        return self.struct.pack(int(time.mktime(val.timetuple())))

    def unpack(self, val):
        return datetime.fromtimestamp(self.struct.unpack(val)[0])

class DateTimeStringColumn(Column):
    format = '%Y-%m-%d %H:%M:%S'
    def pack(self, val):
        return val.strftime(self.format)

    def unpack(self, val):
        return datetime.strptime(val, self.format)

class Float64Column(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('d')

    def pack(self, val):
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class FloatStringColumn(Column):
    def pack(self, val):
        return str(val)

    def unpack(self, val):
        return float(val)

class Int64Column(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('q')

    def pack(self, val):
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class IntStringColumn(Column):
    def pack(self, val):
        return str(val)

    def unpack(self, val):
        return int(val)

class StringColumn(BytesColumn):
    pass
