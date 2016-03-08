from libc.stdio cimport *
from cpython cimport array
from libc.stdlib cimport malloc, free
import array
import struct


cdef extern from "optimized.h":
    int read_bitpacked_internal(void *data, int data_len, int mask, int* res, int total, int bit_width);
    long read_litle_endian_int(unsigned char *data);


cdef class BinaryReader:
    """ This support optimized operations needed for some Parquet reads, providing
    an order of magnitude performance gain
    """

    cdef array.array _array
    cdef int _array_size
    cdef bytes _zero_data

    def __init__(self):
        # Initialize array size to something reasonable
        self._ensure_array(1024)
        self._zero_data = b"\x00\x00\x00\x00"

    def __del__(self):
        self._array = None

    def _ensure_array(self, size):
        if size <= self._array_size:
            return
        self._array = array.array('i', [0] * size)
        self._array_size = size


    def read_unsigned_var_int(self, fo):
        result = 0
        shift = 0
        while True:
            byte = struct.unpack("<B", fo.read(1))[0]
            result |= ((byte & 0x7F) << shift)
            if (byte & 0x80) == 0:
                break
            shift += 7
        return result


    def read_bitpacked_data(self, data, mask, width):

        cdef bytes py_bytes = data
        cdef int bit_width = width
        cdef int data_mask = mask
        cdef char * py_raw = py_bytes

        total = len(data) * 8
        self._ensure_array(total)
        cdef int* native =  &self._array.data.as_ints[0]
        size = read_bitpacked_internal(py_raw, total, data_mask, native, total, bit_width)
        return self._array[:size]

    def read_rle(self, fo, header, width):
        count = header >> 1
        data = fo.read(width)
        data = data + self._zero_data[len(data):]
        value = struct.unpack("<i", data)[0]
        return [ value for i in range(count)]

    def read_plain_boolean(self, fo):
        """Reads a boolean using the plain encoding"""
        raise NotImplemented


    def read_plain_int32(self, fo):
        """Reads a 32-bit int using the plain encoding"""
        cdef bytes py_bytes = fo.read(4)
#if PY_LITTLE_ENDIAN
        cdef unsigned char * py_raw = py_bytes
        return read_litle_endian_int(py_raw)
#else
        tup = struct.unpack("<i", py_bytes)
        return tup[0]
#endif


    def read_plain_int64(self, fo):
        """Reads a 64-bit int using the plain encoding"""
        tup = struct.unpack("<q", fo.read(8))
        return tup[0]


    def read_plain_int96(self, fo):
        """Reads a 96-bit int using the plain encoding"""
        #return read_plain_byte_array_fixed(fo, 12)
        tup = struct.unpack("<qi", fo.read(12))
        return tup[0] << 32 | tup[1]


    def read_plain_float(self, fo):
        """Reads a 32-bit float using the plain encoding"""
        tup = struct.unpack("<f", fo.read(4))
        return tup[0]


    def read_plain_double(self, fo):
        """Reads a 64-bit float (double) using the plain encoding"""
        tup = struct.unpack("<d", fo.read(8))
        return tup[0]


    def read_plain_byte_array(self, fo):
        """Reads a byte array using the plain encoding"""
        length = self.read_plain_int32(fo)
        return fo.read(length)


    def read_plain_byte_array_fixed(self, fo, fixed_length):
        """Reads a byte array of the given fixed_length"""
        return fo.read(fixed_length)