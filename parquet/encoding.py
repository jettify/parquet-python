import array
import math
import struct
import io
import logging
import parquet._optimized
from parquet.ttypes import Type


def byte_width(bit_width):
    "Returns the byte width for the given bit_width"
    return int((bit_width + 7) / 8)


def width_from_max_int(value):
    """Converts the value specified to a bit_width."""
    return int(math.ceil(math.log(value + 1, 2)))


def _mask_for_bits(i):
    """Helper function for read_bitpacked to generage a mask to grab i bits."""
    return (1 << i) - 1


class Encoding(object):
    def __init__(self, bit_width):
        self._bit_width = bit_width
        self._byte_width = byte_width(bit_width)
        self._mask = _mask_for_bits(bit_width)
        self._fast_reader = parquet._optimized.BinaryReader()
        self._DECODE_PLAIN = {
            Type.BOOLEAN: self._fast_reader.read_plain_boolean,
            Type.INT32: self._fast_reader.read_plain_int32,
            Type.INT64: self._fast_reader.read_plain_int64,
            Type.INT96: self._fast_reader.read_plain_int96,
            Type.FLOAT: self._fast_reader.read_plain_float,
            Type.DOUBLE: self._fast_reader.read_plain_double,
            Type.BYTE_ARRAY: self._fast_reader.read_plain_byte_array,
            Type.FIXED_LEN_BYTE_ARRAY: self._fast_reader.read_plain_byte_array_fixed
        }

    def filter_values(self, dictionary, values, definition_levels):
        return self._fast_reader.filter_values(dictionary, values, definition_levels)

    def read_plain(self, fo, type_, type_length):
        return self._DECODE_PLAIN[type_](fo, type_length)

    def read_rle(self, fo, header):
        """Read a run-length encoded run from the given fo with the given header
        and bit_width.

        The count is determined from the header and the width is used to grab the
        value that's repeated. Yields the value repeated count times.
        """
        return self._fast_reader.read_rle(fo, header, self._byte_width)

    def read_bitpacked(self, fo, header):
        """Reads a bitpacked run of the rle/bitpack hybrid.

        Supports width >8 (crossing bytes).
        """
        num_groups = header >> 1
        count = num_groups * 8
        byte_count = int((self._bit_width * count)/8)
        data = fo.read(byte_count)
        return self.read_bitpacked_data(data)


    def read_bitpacked_data(self, data):
        return self._fast_reader.read_bitpacked_data(data, self._mask, self._bit_width)


    def read_bitpacked_deprecated(self, fo, byte_count, count):
        raw_bytes = array.array('B', fo.read(byte_count)).tolist()

        mask = self._mask
        index = 0
        res = []
        word = 0
        bits_in_word = 0
        while len(res) < count and index <= len(raw_bytes):
            if bits_in_word >= self._bit_width:
                # how many bits over the value is stored
                offset = (bits_in_word - self._bit_width)
                # figure out the value
                value = (word & (mask << offset)) >> offset
                res.append(value)

                bits_in_word -= self._bit_width
            else:
                word = (word << 8) | raw_bytes[index]
                index += 1
                bits_in_word += 8
        return res


    def read_rle_bit_packed_hybrid(self, fo, length=None):
        """Implementation of a decoder for the rel/bit-packed hybrid encoding.

        If length is not specified, then a 32-bit int is read first to grab the
        length of the encoded data.
        """
        io_obj = fo
        if length is None:
            length = self._fast_reader.read_plain_int32(fo)
        start = io_obj.tell()
        limit = length + start
        res = []
        while io_obj.tell() < limit:
            header = self._fast_reader.read_unsigned_var_int(io_obj)
            if header & 1 == 0:
                res += self.read_rle(io_obj, header)
            else:
                res += self.read_bitpacked(io_obj, header)
        return res
