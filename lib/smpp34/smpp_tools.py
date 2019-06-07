#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Module smpp_tools. Contains various utility functions, validators and stuff.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
#
###############################################################################

__all__ = [ "int2bin1_be", "int2bin2_be", "int2bin4_be", "int2bin8_be",
            "int2bin1_le", "int2bin2_le", "int2bin4_le", "int2bin8_le",
            "bin2int1_be", "bin2int2_be", "bin2int4_be", "bin2int8_be",
            "bin2int1_le", "bin2int2_le", "bin2int4_le", "bin2int8_le",
            "bin2hex1_be", "bin2hex2_be", "bin2hex4_be", "bin2hex8_be",
            "bin2hex1_le", "bin2hex2_le", "bin2hex4_le", "bin2hex8_le",
            "byte", "word", "dword", "qword", "bytes_len", "quote_bytes",
            "win1251_to_ucs2", "enum", "valid_smpp_time", "encode_gsm7",
            "encode_ucs2" ]

###############################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import *

###############################################################################

byte = lambda i: isinstance(i, int) and 0 <= i <= 0xff
word = lambda i: isinstance(i, int) and 0 <= i <= 0xffff
dword = lambda i: isinstance(i, int) and 0 <= i <= 0xffffffff
qword = lambda i: isinstance(i, int) and 0 <= i <= 0xffffffffffffffff
bytes_len = lambda el: lambda b: isinstance(b, bytes) and len(b) == el
valid_smpp_time = by_regex(b"^[0-9]{15}[R+-]$")

###############################################################################

class enum(dict):
    def __init__(self, items):
        dict.__init__(self, items)
        self.__dict__.update((d["name"], k) for k, d in self.items())

###############################################################################
# integer -> binary string, big endian

@typecheck
def int2bin1_be(b: byte) -> bytes:
    return bytes((b, ))

@typecheck
def int2bin2_be(w: word) -> bytes:
    return bytes((w >> 8, w & 0xff))

@typecheck
def int2bin4_be(dw: dword) -> bytes:
    return bytes((dw >> 24, (dw >> 16) & 0xff, (dw >> 8) & 0xff, dw & 0xff))

@typecheck
def int2bin8_be(qw: qword) -> bytes:
    return bytes((qw >> 56, (qw >> 48) & 0xff, (qw >> 40) & 0xff, (qw >> 32) & 0xff,
                  (qw >> 24) & 0xff, (qw >> 16) & 0xff, (qw >> 8) & 0xff, qw & 0xff))

###############################################################################
# integer -> binary string, little endian

@typecheck
def int2bin1_le(b: byte) -> bytes:
    return bytes((b, ))

@typecheck
def int2bin2_le(w: word) -> bytes:
    return bytes((w & 0xff, w >> 8))

@typecheck
def int2bin4_le(dw: dword) -> bytes:
    return bytes((dw & 0xff, (dw >> 8) & 0xff, (dw >> 16) & 0xff, dw >> 24))

@typecheck
def int2bin8_le(qw: qword) -> bytes:
    return bytes((qw & 0xff, (qw >> 8) & 0xff, (qw >> 16) & 0xff, (qw >> 24) & 0xff,
                  (qw >> 32) & 0xff, (qw >> 40) & 0xff, (qw >> 48) & 0xff, qw >> 56))

###############################################################################
# binary string -> integer, big endian

@typecheck
def bin2int1_be(b: bytes_len(1)) -> int:
    return b[0]

@typecheck
def bin2int2_be(b2: bytes_len(2)) -> int:
    return (b2[0] << 8) | b2[1]

@typecheck
def bin2int4_be(b4: bytes_len(4)) -> int:
    return (b4[0] << 24) | (b4[1] << 16) | (b4[2] << 8) | b4[3]

@typecheck
def bin2int8_be(b8: bytes_len(8)) -> int:
    return (b8[0] << 56) | (b8[1] << 48) | (b8[2] << 40) | (b8[3] << 32) | \
           (b8[4] << 24) | (b8[5] << 16) | (b8[6] << 8) | b8[7]

###############################################################################
# binary string -> integer, little endian

@typecheck
def bin2int1_le(b: bytes_len(1)) -> int:
    return b[0]

@typecheck
def bin2int2_le(b2: bytes_len(2)) -> int:
    return (b2[1] << 8) | b2[0]

@typecheck
def bin2int4_le(b4: bytes_len(4)) -> int:
    return (b4[3] << 24) | (b4[2] << 16) | (b4[1] << 8) | b4[0]

@typecheck
def bin2int8_le(b8: bytes_len(8)) -> int:
    return (b8[7] << 56) | (b8[6] << 48) | (b8[5] << 40) | (b8[4] << 32) | \
           (b8[3] << 24) | (b8[2] << 16) | (b8[1] << 8) | b8[0]

###############################################################################
# binary string -> hex string, big endian

@typecheck
def bin2hex1_be(b: bytes_len(1)) -> str:
    return "{0:02x}".format(bin2int1_be(b))

@typecheck
def bin2hex2_be(b2: bytes_len(2)) -> str:
    return "{0:04x}".format(bin2int2_be(b2))

@typecheck
def bin2hex4_be(b4: bytes_len(4)) -> str:
    return "{0:08x}".format(bin2int4_be(b4))

@typecheck
def bin2hex8_be(b8: bytes_len(8)) -> str:
    return "{0:016x}".format(bin2int8_be(b8))

###############################################################################
# binary string -> hex string, little endian

@typecheck
def bin2hex1_le(b: bytes_len(1)) -> str:
    return "{0:02x}".format(bin2int1_le(b))

@typecheck
def bin2hex2_le(b2: bytes_len(2)) -> str:
    return "{0:04x}".format(bin2int2_le(b2))

@typecheck
def bin2hex4_le(b4: bytes_len(4)) -> str:
    return "{0:08x}".format(bin2int4_le(b4))

@typecheck
def bin2hex8_le(b8: bytes_len(8)) -> str:
    return "{0:016x}".format(bin2int8_le(b8))

###############################################################################
# string -> ucs2 binary string (assuming windows-1251 encoding)

@typecheck
def win1251_to_ucs2(s: str) -> bytes:
    return b"".join(c >= 0xc0 and bytes((0x04, c - 0xb0)) or
                    c == 0xa8 and b"\x04\x15" or
                    c == 0xb8 and b"\x04\x35" or
                    bytes((0x00, c))
                    for c in s.encode("windows-1251"))

@typecheck
def quote_bytes(b: bytes) -> str:
    return "\"{0:s}\"".format("".join((0x20 <= c < 0x22 or 0x22 < c < 0x7f) and chr(c) or
                                      c != 0x22 and "\\x{0:02x}".format(c) or "\\\"" for c in b))

###############################################################################

_gsm_chars = \
{
0x0040:   0, 0x00a3:   1, 0x0024:   2, 0x00a5:   3, 0x00e8:   4, 0x00e9:   5, 0x00f9:   6, 0x00ec:   7,
0x00f2:   8, 0x00c7:   9, 0x000a:  10, 0x00d8:  11, 0x00f8:  12, 0x000d:  13, 0x00c5:  14, 0x00e5:  15,
0x0394:  16, 0x005f:  17, 0x03a6:  18, 0x0393:  19, 0x039b:  20, 0x03a9:  21, 0x03a0:  22, 0x03a8:  23,
0x03a3:  24, 0x0398:  25, 0x039e:  26, 0x001b:  27, 0x00c6:  28, 0x00e6:  29, 0x00df:  30, 0x00c9:  31,
0x0020:  32, 0x0021:  33, 0x0022:  34, 0x0023:  35, 0x00a4:  36, 0x0025:  37, 0x0026:  38, 0x0027:  39,
0x0028:  40, 0x0029:  41, 0x002a:  42, 0x002b:  43, 0x002c:  44, 0x002d:  45, 0x002e:  46, 0x002f:  47,
0x0030:  48, 0x0031:  49, 0x0032:  50, 0x0033:  51, 0x0034:  52, 0x0035:  53, 0x0036:  54, 0x0037:  55,
0x0038:  56, 0x0039:  57, 0x003a:  58, 0x003b:  59, 0x003c:  60, 0x003d:  61, 0x003e:  62, 0x003f:  63,
0x00a1:  64, 0x0041:  65, 0x0042:  66, 0x0043:  67, 0x0044:  68, 0x0045:  69, 0x0046:  70, 0x0047:  71,
0x0048:  72, 0x0049:  73, 0x004a:  74, 0x004b:  75, 0x004c:  76, 0x004d:  77, 0x004e:  78, 0x004f:  79,
0x0050:  80, 0x0051:  81, 0x0052:  82, 0x0053:  83, 0x0054:  84, 0x0055:  85, 0x0056:  86, 0x0057:  87,
0x0058:  88, 0x0059:  89, 0x005a:  90, 0x00c4:  91, 0x00d6:  92, 0x00d1:  93, 0x00dc:  94, 0x0060:  95,
0x00bf:  96, 0x0061:  97, 0x0062:  98, 0x0063:  99, 0x0064: 100, 0x0065: 101, 0x0066: 102, 0x0067: 103,
0x0068: 104, 0x0069: 105, 0x006a: 106, 0x006b: 107, 0x006c: 108, 0x006d: 109, 0x006e: 110, 0x006f: 111,
0x0070: 112, 0x0071: 113, 0x0072: 114, 0x0073: 115, 0x0074: 116, 0x0075: 117, 0x0076: 118, 0x0077: 119,
0x0078: 120, 0x0079: 121, 0x007a: 122, 0x00e4: 123, 0x00f6: 124, 0x00f1: 125, 0x00fc: 126, 0x00e0: 127,
}

_ext_chars = \
{
0x005b:  60, 0x005c:  47, 0x005d:  62, 0x005e:  20, 0x0060:   0, 0x007b:  40, 0x007c:  64, 0x007d:  41,
0x007e:  61, 0x20ac: 101,
}

# string -> gsm7 binary string

@typecheck
def encode_gsm7(s: str, pack: optional(bool) = False) -> bytes:

    bb = []
    for i, b in enumerate(map(ord, s)):
        c = _gsm_chars.get(b)
        if c is not None:
            bb.append(c)
            continue
        c = _ext_chars.get(b)
        if c is not None:
            bb.append(0x1b)
            bb.append(c)
        else:
            raise ValueError("GSM7 codec can't encode character "
                             "\\u{0:04x} in position {1:d}".format(b, i))

    return bytes([
        ((b & 0x7f) >> (i % 8) | nb << (7 - (i % 8))) & 0xff
        for i, (b, nb) in enumerate(zip(bb, bb[1:] + [0x00]))
        if i % 8 != 7 ]) if pack else bytes(bb)

###############################################################################

@typecheck
def encode_ucs2(s: str) -> bytes:
    return b"".join([ bytes([ord(c) >> 8, ord(c) & 0xff]) for c in s ])

###############################################################################

if __name__ == "__main__":

    print("self-testing module smpp_tools.py:")

    from binascii import b2a_hex

    assert int2bin1_be(0x01) == b"\x01"
    assert int2bin2_be(0x0123) == b"\x01\x23"
    assert int2bin4_be(0x01234567) == b"\x01\x23\x45\x67"
    assert int2bin8_be(0x0123456789abcdef) == b"\x01\x23\x45\x67\x89\xab\xcd\xef"

    assert int2bin1_le(0x01) == b"\x01"
    assert int2bin2_le(0x0123) == b"\x23\x01"
    assert int2bin4_le(0x01234567) == b"\x67\x45\x23\x01"
    assert int2bin8_le(0x0123456789abcdef) == b"\xef\xcd\xab\x89\x67\x45\x23\x01"

    assert bin2int1_be(b"\x01") == 0x01
    assert bin2int2_be(b"\x01\x23") == 0x0123
    assert bin2int4_be(b"\x01\x23\x45\x67") == 0x01234567
    assert bin2int8_be(b"\x01\x23\x45\x67\x89\xab\xcd\xef") == 0x0123456789abcdef

    assert bin2int1_le(b"\x01") == 0x01
    assert bin2int2_le(b"\x01\x23") == 0x2301
    assert bin2int4_le(b"\x01\x23\x45\x67") == 0x67452301
    assert bin2int8_le(b"\x01\x23\x45\x67\x89\xab\xcd\xef") == 0xefcdab8967452301

    assert bin2hex1_be(b"\x01") == "01"
    assert bin2hex2_be(b"\x01\x23") == "0123"
    assert bin2hex4_be(b"\x01\x23\x45\x67") == "01234567"
    assert bin2hex8_be(b"\x01\x23\x45\x67\x89\xab\xcd\xef") == "0123456789abcdef"

    assert bin2hex1_le(b"\x01") == "01"
    assert bin2hex2_le(b"\x01\x23") == "2301"
    assert bin2hex4_le(b"\x01\x23\x45\x67") == "67452301"
    assert bin2hex8_le(b"\x01\x23\x45\x67\x89\xab\xcd\xef") == "efcdab8967452301"

    ascii = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f" \
            "\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f" \
            "\x20\x21\x22\x23\x24\x25\x26\x27\x28\x29\x2a\x2b\x2c\x2d\x2e\x2f" \
            "\x30\x31\x32\x33\x34\x35\x36\x37\x38\x39\x3a\x3b\x3c\x3d\x3e\x3f" \
            "\x40\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f" \
            "\x50\x51\x52\x53\x54\x55\x56\x57\x58\x59\x5a\x5b\x5c\x5d\x5e\x5f" \
            "\x60\x61\x62\x63\x64\x65\x66\x67\x68\x69\x6a\x6b\x6c\x6d\x6e\x6f" \
            "\x70\x71\x72\x73\x74\x75\x76\x77\x78\x79\x7a\x7b\x7c\x7d\x7e\x7f"

    russian = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419\u041a" \
              "\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424\u0425\u0426" \
              "\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f\u0430\u0431\u0432" \
              "\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439\u043a\u043b\u043c\u043d" \
              "\u043e\u043f\u0440\u0441\u0442\u0443\u0444\u0445\u0446\u0447\u0448\u0449" \
              "\u044c\u044b\u044a\u044d\u044e\u044f"

    assert b2a_hex(win1251_to_ucs2(ascii)) == \
    b"0000000100020003000400050006000700080009000a000b000c000d000e000f001000110012001300140015" \
    b"0016001700180019001a001b001c001d001e001f0020002100220023002400250026002700280029002a002b" \
    b"002c002d002e002f0030003100320033003400350036003700380039003a003b003c003d003e003f00400041" \
    b"00420043004400450046004700480049004a004b004c004d004e004f00500051005200530054005500560057" \
    b"00580059005a005b005c005d005e005f0060006100620063006400650066006700680069006a006b006c006d" \
    b"006e006f0070007100720073007400750076007700780079007a007b007c007d007e007f"

    assert b2a_hex(win1251_to_ucs2(russian)) == \
    b"04100411041204130414041504150416041704180419041a041b041c041d041e041f04200421042204230424" \
    b"04250426042704280429042c042b042a042d042e042f04300431043204330434043504350436043704380439" \
    b"043a043b043c043d043e043f0440044104420443044404450446044704480449044c044b044a044d044e044f"

    assert quote_bytes(b"") == "\"\""
    assert quote_bytes(b"foo") == "\"foo\""
    assert quote_bytes(b"\x00") == "\"\\x00\""
    assert quote_bytes(bytes(range(256))) == \
    "\"\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09\\x0a\\x0b\\x0c\\x0d\\x0e\\x0f\\x10\\x11\\x12\\x13\\x14\\x15" \
    "\\x16\\x17\\x18\\x19\\x1a\\x1b\\x1c\\x1d\\x1e\\x1f !\\\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOP" \
    "QRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\\x7f\\x80\\x81\\x82\\x83\\x84\\x85\\x86\\x87\\x88\\x89" \
    "\\x8a\\x8b\\x8c\\x8d\\x8e\\x8f\\x90\\x91\\x92\\x93\\x94\\x95\\x96\\x97\\x98\\x99\\x9a\\x9b\\x9c\\x9d\\x9e\\x9f" \
    "\\xa0\\xa1\\xa2\\xa3\\xa4\\xa5\\xa6\\xa7\\xa8\\xa9\\xaa\\xab\\xac\\xad\\xae\\xaf\\xb0\\xb1\\xb2\\xb3\\xb4\\xb5" \
    "\\xb6\\xb7\\xb8\\xb9\\xba\\xbb\\xbc\\xbd\\xbe\\xbf\\xc0\\xc1\\xc2\\xc3\\xc4\\xc5\\xc6\\xc7\\xc8\\xc9\\xca\\xcb" \
    "\\xcc\\xcd\\xce\\xcf\\xd0\\xd1\\xd2\\xd3\\xd4\\xd5\\xd6\\xd7\\xd8\\xd9\\xda\\xdb\\xdc\\xdd\\xde\\xdf\\xe0\\xe1" \
    "\\xe2\\xe3\\xe4\\xe5\\xe6\\xe7\\xe8\\xe9\\xea\\xeb\\xec\\xed\\xee\\xef\\xf0\\xf1\\xf2\\xf3\\xf4\\xf5\\xf6\\xf7" \
    "\\xf8\\xf9\\xfa\\xfb\\xfc\\xfd\\xfe\\xff\""

    d = { 1: { "name": "A", "foo": "bar" }, 2: { "name": "B", "biz": "baz" } }
    e = enum(d)
    assert e.A == 1 and e.B == 2
    assert e[1] == { "name": "A", "foo": "bar" } and e[2] == { "name": "B", "biz": "baz" }
    assert list(e.keys()) == [1, 2]

    assert valid_smpp_time(b"080128151723020+")
    assert valid_smpp_time(b"080128151723020-")
    assert valid_smpp_time(b"080128151723020R")

    assert not valid_smpp_time(b"")
    assert not valid_smpp_time(b"080128151723020")
    assert not valid_smpp_time(b"0801R8151723020R")

    assert encode_gsm7("") == b""
    assert encode_gsm7("A") == b"A"
    assert encode_gsm7("$") == b"\x02"
    assert encode_gsm7("\\") == b"\x1b/"
    assert encode_gsm7("hello world") == b"hello world"

    assert encode_gsm7("", True) == b""
    assert encode_gsm7("A", True) == b"A"
    assert encode_gsm7("$", True) == b"\x02"
    assert encode_gsm7("\\", True) == b"\x9b\x17"
    assert encode_gsm7("hello world", True) == b"\xe82\x9b\xfd\x06\xdd\xdfr6\x19"
    try:
        encode_gsm7("foo\u1234bar")
    except ValueError as e:
        assert str(e) == "GSM7 codec can't encode character \\u1234 in position 3"
    else:
        assert False

    assert encode_ucs2("") == b""
    assert encode_ucs2("foo") == b"\x00f\x00o\x00o"
    assert encode_ucs2(russian) == \
        b"\x04\x10\x04\x11\x04\x12\x04\x13\x04\x14\x04\x15\x04\x01\x04\x16\x04\x17\x04" \
        b"\x18\x04\x19\x04\x1a\x04\x1b\x04\x1c\x04\x1d\x04\x1e\x04\x1f\x04 \x04!\x04\"\x04" \
        b"#\x04$\x04%\x04&\x04\'\x04(\x04)\x04,\x04+\x04*\x04-\x04.\x04/\x040\x041\x042" \
        b"\x043\x044\x045\x04Q\x046\x047\x048\x049\x04:\x04;\x04<\x04=\x04>\x04?\x04@\x04A" \
        b"\x04B\x04C\x04D\x04E\x04F\x04G\x04H\x04I\x04L\x04K\x04J\x04M\x04N\x04O"

    print("ok")

###############################################################################
# EOF
