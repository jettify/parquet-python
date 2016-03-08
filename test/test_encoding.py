import array
import struct
from io import BytesIO
import unittest

import parquet.encoding
import parquet._optimized
from parquet.ttypes import Type
from nose import SkipTest


class TestPlain(unittest.TestCase):

    def test_int32(self):
        reader = parquet._optimized.BinaryReader()
        self.assertEquals(
            999,
            reader.read_plain_int32(
                BytesIO(struct.pack("<i", 999))))

    def test_int64(self):
        reader = parquet._optimized.BinaryReader()
        self.assertEquals(
            999,
            reader.read_plain_int64(
                BytesIO(struct.pack("<q", 999))))
        self.assertEquals(
            0,
            reader.read_plain_int64(
                BytesIO(struct.pack("<q", 0))))

    def test_int96(self):
        reader = parquet._optimized.BinaryReader()
        self.assertEquals(
            999,
            reader.read_plain_int96(
                BytesIO(struct.pack("<qi", 0, 999))))

    def test_float(self):
        reader = parquet._optimized.BinaryReader()
        self.assertAlmostEquals(
            9.99,
            reader.read_plain_float(
                BytesIO(struct.pack("<f", 9.99))),
            2)

    def test_double(self):
        reader = parquet._optimized.BinaryReader()
        self.assertEquals(
            9.99,
            reader.read_plain_double(
                BytesIO(struct.pack("<d", 9.99))))

    def test_fixed(self):
        reader = parquet._optimized.BinaryReader()
        data = b"foobar"
        fo = BytesIO(data)
        self.assertEquals(
            data[:3].decode('utf-8'),
            reader.read_plain_byte_array_fixed(
                fo, 3))
        self.assertEquals(
            data[3:].decode('utf-8'),
            reader.read_plain_byte_array_fixed(
                fo, 3))

    def test_fixed_read_plain(self):
        reader = parquet.encoding.Encoding(1)
        data = b"foobar"
        fo = BytesIO(data)
        self.assertEquals(
            data[:3].decode('utf-8'),
            reader.read_plain(
                fo, Type.FIXED_LEN_BYTE_ARRAY, 3))


class TestRle(unittest.TestCase):

    def testFourByteValue(self):
        fo = BytesIO(struct.pack("<i", 1 << 30))
        reader = parquet.encoding.Encoding(30)
        out = reader.read_rle(fo, 2 << 1)
        self.assertEquals([1 << 30] * 2, list(out))


class TestVarInt(unittest.TestCase):

    def testSingleByte(self):
        reader = parquet._optimized.BinaryReader()
        fo = BytesIO(struct.pack("<B", 0x7F))
        out = reader.read_unsigned_var_int(fo)
        self.assertEquals(0x7F, out)

    def testFourByte(self):
        reader = parquet._optimized.BinaryReader()
        fo = BytesIO(struct.pack("<BBBB", 0xFF, 0xFF, 0xFF, 0x7F))
        out = reader.read_unsigned_var_int(fo)
        self.assertEquals(0x0FFFFFFF, out)


class TestBitPacked(unittest.TestCase):

    def testFromExample(self):
        raw_data_in = [0b10001000, 0b11000110, 0b11111010]
        encoded_bitstring = array.array('B', raw_data_in).tostring()
        fo = BytesIO(encoded_bitstring)
        count = 3 << 1
        reader = parquet.encoding.Encoding(3)
        res = reader.read_bitpacked(fo, count)
        self.assertEquals([x for x in range(8)], res.tolist())


class TestBitPackedDeprecated(unittest.TestCase):

    def testFromExample(self):
        encoded_bitstring = array.array(
            'B', [0b00000101, 0b00111001, 0b01110111]).tostring()
        fo = BytesIO(encoded_bitstring)
        reader = parquet.encoding.Encoding(3)
        res = reader.read_bitpacked_deprecated(fo, 3, 8)
        self.assertEquals([x for x in range(8)], res)


class TestWidthFromMaxInt(unittest.TestCase):

    def testWidths(self):
        self.assertEquals(0, parquet.encoding.width_from_max_int(0))
        self.assertEquals(1, parquet.encoding.width_from_max_int(1))
        self.assertEquals(2, parquet.encoding.width_from_max_int(2))
        self.assertEquals(2, parquet.encoding.width_from_max_int(3))
        self.assertEquals(3, parquet.encoding.width_from_max_int(4))
        self.assertEquals(3, parquet.encoding.width_from_max_int(5))
        self.assertEquals(3, parquet.encoding.width_from_max_int(6))
        self.assertEquals(3, parquet.encoding.width_from_max_int(7))
        self.assertEquals(4, parquet.encoding.width_from_max_int(8))
        self.assertEquals(4, parquet.encoding.width_from_max_int(15))
        self.assertEquals(5, parquet.encoding.width_from_max_int(16))
        self.assertEquals(5, parquet.encoding.width_from_max_int(31))
        self.assertEquals(6, parquet.encoding.width_from_max_int(32))
        self.assertEquals(6, parquet.encoding.width_from_max_int(63))
        self.assertEquals(7, parquet.encoding.width_from_max_int(64))
        self.assertEquals(7, parquet.encoding.width_from_max_int(127))
        self.assertEquals(8, parquet.encoding.width_from_max_int(128))
        self.assertEquals(8, parquet.encoding.width_from_max_int(255))
