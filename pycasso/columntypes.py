from datetime import datetime
import struct
import time

__all__ = ['Column', 'BytesColumn', 'DateTimeColumn', 'Float64Column',
           'Int64Column', 'StringColumn']

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

class Float64Column(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('d')

    def pack(self, val):
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class Int64Column(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = struct.Struct('q')

    def pack(self, val):
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class StringColumn(BytesColumn):
    pass
