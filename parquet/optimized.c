

int read_bitpacked_internal(void *data, int data_len, int mask, int* res, int total, int bit_width)
{
    unsigned char * raw_bytes =  (unsigned char*)data;
    int current_byte = 0;
    unsigned char b = raw_bytes[current_byte];
    int bits_wnd_l = 8;
    int bits_wnd_r = 0;
    int idx = 0;
    while (total >= bit_width) {
        if (bits_wnd_r >= 8) {
            bits_wnd_r -= 8;
            bits_wnd_l -= 8;
            b >>= 8;
        }
        else if (bits_wnd_l - bits_wnd_r >= bit_width) {
            res[idx] = (b >> bits_wnd_r) & mask;
            idx += 1;
            total -= bit_width;
            bits_wnd_r += bit_width;
        }
        else if (current_byte + 1 < data_len) {
            current_byte += 1;
            b |= (raw_bytes[current_byte] << bits_wnd_l);
            bits_wnd_l += 8;
        }
    }
    return idx;

}

 long read_litle_endian_int(unsigned char *data)
 {
    long x = 0;
    int i = 4;
    do {
        x = (x<<8) | data[--i];
    } while (i > 0);
    x |= -(x & (1L << ((8 * 4) - 1)));
    return x;

 }