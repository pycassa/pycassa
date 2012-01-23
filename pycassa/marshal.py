"""
Tools for marshalling and unmarshalling data stored
in Cassandra.
"""

import uuid
import time
import struct
from datetime import datetime

import pycassa.util as util

_number_types = frozenset((int, long, float))

if hasattr(struct, 'Struct'): # new in Python 2.5
    _have_struct = True
    _bool_packer   = struct.Struct('>B')
    _float_packer  = struct.Struct('>f')
    _double_packer = struct.Struct('>d')
    _long_packer = struct.Struct('>q')
    _int_packer = struct.Struct('>i')
    _short_packer = struct.Struct('>H')
else:
    _have_struct = False

_BASIC_TYPES = ['BytesType', 'LongType', 'IntegerType', 'UTF8Type',
                'AsciiType', 'LexicalUUIDType', 'TimeUUIDType',
                'CounterColumnType', 'FloatType', 'DoubleType',
                'DateType', 'BooleanType', 'UUIDType']

def extract_type_name(typestr):
    if typestr is None:
        return 'BytesType'

    if "CompositeType" in typestr:
        return _get_composite_name(typestr)

    if "ReversedType" in typestr:
        return _get_inner_type(typestr)

    index = typestr.rfind('.')
    if index != -1:
        typestr = typestr[index + 1: ]
    if typestr not in _BASIC_TYPES:
        typestr = 'BytesType'
    return typestr

def _get_inner_type(typestr):
    """ Given a str like 'org.apache...ReversedType(LongType)',
    return just 'LongType' """
    first_paren = typestr.find('(')
    return typestr[first_paren + 1 : -1]

def _get_inner_types(typestr):
    """ Given a str like 'org.apache...CompositeType(LongType, DoubleType)',
    return a tuple of the inner types, like ('LongType', 'DoubleType') """
    internal_str = _get_inner_type(typestr)
    return map(str.strip, internal_str.split(','))

def _get_composite_name(typestr):
    types = map(extract_type_name, _get_inner_types(typestr))
    return "CompositeType(" + ", ".join(types) + ")"

def _to_timestamp(v):
    # Expects Value to be either date or datetime
    try:
        converted = time.mktime(v.timetuple())
        converted = converted * 1e6 + getattr(v, 'microsecond', 0)
    except AttributeError:
        # Ints and floats are valid timestamps too
        if type(v) not in _number_types:
            raise TypeError('DateType arguments must be a datetime or timestamp')

        converted = v * 1e6
    return converted

def get_composite_packer(typestr):
    packers = map(packer_for, _get_inner_types(typestr))

    if _have_struct:
        len_packer = _short_packer.pack
    else:
        len_packer = lambda v: struct.pack('>H', v)

    def pack_composite(items, slice_start=None):
        s = ''
        for item, packer in zip(items, packers):
            eoc = '\x00'
            if isinstance(item, tuple):
                item, inclusive = item
                if inclusive:
                    if slice_start:
                        eoc = '\xff'
                    elif slice_start is False:
                        eoc = '\x01'
                else:
                    if slice_start:
                        eoc = '\x01'
                    elif slice_start is False:
                        eoc = '\xff'
            packed = packer(item)
            s += ''.join((len_packer(len(packed)), packed, eoc))
        return s

    return pack_composite

def get_composite_unpacker(typestr):
    unpackers = map(unpacker_for, _get_inner_types(typestr))

    if _have_struct:
        len_unpacker = lambda v: _short_packer.unpack(v)[0]
    else:
        len_unpacker = lambda v: struct.unpack('>H', v)[0]

    def unpack_composite(bytestr):
        # The composite format for each component is:
        #   <len>   <value>   <eoc>
        # 2 bytes | ? bytes | 1 byte
        components = []
        i = iter(unpackers)
        while bytestr:
            unpacker = i.next()
            length = len_unpacker(bytestr[:2])
            components.append(unpacker(bytestr[2:2+length]))
            bytestr = bytestr[3+length:]
        return tuple(components)

    return unpack_composite

def packer_for(typestr):
    if typestr is None:
        return lambda v: v

    if "CompositeType" in typestr:
        return get_composite_packer(typestr)

    if "ReversedType" in typestr:
        return packer_for(_get_inner_type(typestr))

    data_type = extract_type_name(typestr)

    if data_type == 'DateType':
        if _have_struct:
            def pack_date(v, _=None):
                return _long_packer.pack(_to_timestamp(v))
        else:
            def pack_date(v, _=None):
                return struct.pack('>q', _to_timestamp(v))
        return pack_date

    elif data_type == 'BooleanType':
        if _have_struct:
            def pack_bool(v, _=None):
                return _bool_packer.pack(bool(v))
        else:
            def pack_bool(v, _=None):
                return struct.pack('>B', bool(v))
        return pack_bool

    elif data_type == 'DoubleType':
        if _have_struct:
            def pack_double(v, _=None):
                return _double_packer.pack(v)
        else:
            def pack_double(v, _=None):
                return struct.pack('>d', v)
        return pack_double

    elif data_type == 'FloatType':
        if _have_struct:
            def pack_float(v, _=None):
                return _float_packer.pack(v)
        else:
            def pack_float(v, _=None):
                return struct.pack('>f', v)
        return pack_float

    elif data_type == 'LongType':
        if _have_struct:
            def pack_long(v, _=None):
                return _long_packer.pack(v)
        else:
            def pack_long(v, _=None):
                return struct.pack('>q', v)
        return pack_long

    elif data_type == 'IntegerType':
        return encode_int

    elif data_type == 'UTF8Type':
        def pack_utf8(v, _=None):
            try:
                return v.encode('utf-8')
            except UnicodeDecodeError:
                # v is already utf-8 encoded
                return v
        return pack_utf8

    elif 'UUIDType' in data_type:
        def pack_uuid(value, slice_start=None):
            if slice_start is None:
                value = util.convert_time_to_uuid(value,
                        randomize=True)
            else:
                value = util.convert_time_to_uuid(value,
                        lowest_val=slice_start,
                        randomize=False)

            if not hasattr(value, 'bytes'):
                raise TypeError("%s is not valid for UUIDType" % value)
            return value.bytes
        return pack_uuid

    elif data_type == "CounterColumnType":
        def noop(value, slice_start=None):
            return value
        return noop

    else: # data_type == 'BytesType' or something unknown
        def pack_bytes(v, _=None):
            if not isinstance(v, basestring):
                raise TypeError("A str or unicode value was expected, " +
                                "but %s was received instead (%s)"
                                % (v.__class__.__name__, str(v)))
            return v
        return pack_bytes

def unpacker_for(typestr):
    if typestr is None:
        return lambda v: v

    if "CompositeType" in typestr:
        return get_composite_unpacker(typestr)

    if "ReversedType" in typestr:
        return unpacker_for(_get_inner_type(typestr))

    data_type = extract_type_name(typestr)

    if data_type == 'BytesType':
        return lambda v: v

    elif data_type == 'DateType':
        if _have_struct:
            return lambda v: datetime.fromtimestamp(_long_packer.unpack(v)[0] / 1e6)
        else:
            return lambda v: datetime.fromtimestamp(struct.unpack('>q', v)[0] / 1e6)

    elif data_type == 'BooleanType':
        if _have_struct:
            return lambda v: bool(_bool_packer.unpack(v)[0])
        else:
            return lambda v: bool(struct.unpack('>B', v)[0])

    elif data_type == 'DoubleType':
        if _have_struct:
            return lambda v: _double_packer.unpack(v)[0]
        else:
            return lambda v: struct.unpack('>d', v)[0]

    elif data_type == 'FloatType':
        if _have_struct:
            return lambda v: _float_packer.unpack(v)[0]
        else:
            return lambda v: struct.unpack('>f', v)[0]

    elif data_type == 'LongType':
        if _have_struct:
            return lambda v: _long_packer.unpack(v)[0]
        else:
            return lambda v: struct.unpack('>q', v)[0]

    elif data_type == 'IntegerType':
        return decode_int

    elif data_type == 'UTF8Type':
        return lambda v: v.decode('utf-8')

    elif 'UUIDType' in data_type:
        return lambda v: uuid.UUID(bytes=v)

    else:
        return lambda v: v

def encode_int(x, *args):
    if x >= 0:
        out = []
        while x >= 256:
            out.append(struct.pack('B', 0xff & x))
            x >>= 8
        out.append(struct.pack('B', 0xff & x))
        if x > 127:
            out.append('\x00')
    else:
        x = -1 - x
        out = []
        while x >= 256:
            out.append(struct.pack('B', 0xff & ~x))
            x >>= 8
        if x <= 127:
            out.append(struct.pack('B', 0xff & ~x))
        else:
            out.append(struct.pack('>H', 0xffff & ~x))

    return ''.join(reversed(out))

def decode_int(term, *args):
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        val = val - (1 << (len(term) * 8))
    return val
