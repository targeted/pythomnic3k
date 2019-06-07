#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Module smpp_types. Contains SMPP data types definitions.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
#
###############################################################################

__all__ = [ "SMPPType", "SMPPInteger1", "SMPPInteger2", "SMPPInteger4",
            "SMPPCOctetString", "SMPPCOctetStringDecimal", "SMPPCOctetStringHex",
            "SMPPOctetString", "SMPPTLV", "SMPPCompositeType", "SMPPTypeSwitch",
            "SMPPArray", "SMPPGenericAddress", "SMPPSmeDestAddress",
            "SMPPDistributionList", "SMPPDestAddress", "SMPPDestAddresses",
            "SMPPUnsuccessSme", "SMPPUnsuccessSmes", "SMPPUDHElement", "SMPPUDH" ]

###############################################################################

import io; from io import BytesIO
import functools; from functools import reduce

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import *
import smpp34.smpp_tools; from smpp34.smpp_tools import *
import smpp34.smpp_errors; from smpp34.smpp_errors import *

###############################################################################

class SMPPType:

    def __init__(self, value):
        self._value = value

    value = property(lambda self: self._value) # read-only property

    def __eq__(self, other):
        return type(self) is type(other) and self._value == other._value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._value)

    def __repr__(self):
        return "<{0:s} at 0x{1:08x}>".format(self, id(self))

    def __format__(self, spec):
        return format(str(self), spec)

###############################################################################

class SMPPInteger1(SMPPType):

    @typecheck
    def __init__(self, value: byte):
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> byte:
        return 0x00

    def __str__(self):
        return "SMPPInteger1(0x{0:02x})".format(self._value)

    @typecheck
    def serialize(self) -> bytes:
        return int2bin1_be(self._value)

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read"), length: optional(one_of(1)) = 1):
        b = r.read(1)
        if len(b) < 1:
            raise SMPPTypeReadError("{0:s}.read() has encountered unexpected "
                                    "end of stream".format(cls.__name__))
        return cls(bin2int1_be(b))

###############################################################################

class SMPPInteger2(SMPPType):

    @typecheck
    def __init__(self, value: word):
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> word:
        return 0x0000

    def __str__(self):
        return "SMPPInteger2(0x{0:04x})".format(self._value)

    @typecheck
    def serialize(self) -> bytes:
        return int2bin2_be(self._value)

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read"), length: optional(one_of(2)) = 2):
        b2 = r.read(2)
        if len(b2) < 2:
            raise SMPPTypeReadError("{0:s}.read() has encountered unexpected "
                                    "end of stream".format(cls.__name__))
        return cls(bin2int2_be(b2))

###############################################################################

class SMPPInteger4(SMPPType):

    @typecheck
    def __init__(self, value: dword):
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> dword:
        return 0x00000000

    def __str__(self):
        return "SMPPInteger4(0x{0:08x})".format(self._value)

    @typecheck
    def serialize(self) -> bytes:
        return int2bin4_be(self._value)

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read"), length: optional(one_of(4)) = 4):
        b4 = r.read(4)
        if len(b4) < 4:
            raise SMPPTypeReadError("{0:s}.read() has encountered unexpected "
                                    "end of stream".format(cls.__name__))
        return cls(bin2int4_be(b4))

###############################################################################

class SMPPCOctetString(SMPPType):

    _allowed_bytes = set(range(1, 256))

    @typecheck
    def __init__(self, value: bytes):
        assert set(value).issubset(self._allowed_bytes), \
               "invalid byte in {0:s}".format(quote_bytes(value))
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> bytes:
        return b""

    def __str__(self):
        return "{0:s}({1:s})".format(self.__class__.__name__, quote_bytes(self._value))

    def __len__(self):
        return len(self._value)

    @typecheck
    def serialize(self) -> bytes:
        return self._value + b"\x00"

    # note that for c-octet strings null terminator byte is accounted for in
    # size, but is not included in the value

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read"), length: optional(int) = None):
        if length is None:
            length = 0x7fffffff
        value = b""
        b = r.read(1)
        while b != b"\x00":
            if len(value) >= length - 1: # will read no more than N-1 non-NULL bytes
                raise SMPPTypeReadError("{0:s}.read() has exceeded its maximum allowed length of "
                                        "{1:d} byte(s)".format(cls.__name__, length))
            elif len(b) == 0:
                raise SMPPTypeReadError("{0:s}.read() has encountered unexpected "
                                        "end of stream".format(cls.__name__))
            elif ord(b) not in cls._allowed_bytes:
                raise SMPPTypeReadError("{0:s}.read() has encountered invalid byte: "
                                        "{1:s}".format(cls.__name__, quote_bytes(b)))
            value += b
            b = r.read(1)
        return cls(value)

###############################################################################

class SMPPCOctetStringDecimal(SMPPCOctetString):

    _allowed_bytes = set(b"0123456789")

    @typecheck
    def __init__(self, value: bytes):
        SMPPCOctetString.__init__(self, value)

###############################################################################

class SMPPCOctetStringHex(SMPPCOctetString):

    _allowed_bytes = set(b"0123456789abcdefABCDEF")

    @typecheck
    def __init__(self, value: bytes):
        SMPPCOctetString.__init__(self, value)

###############################################################################

class SMPPOctetString(SMPPType):

    @typecheck
    def __init__(self, value: bytes):
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> bytes:
        return b""

    def __str__(self):
        return "{0:s}({1:s})".format(self.__class__.__name__, quote_bytes(self._value))

    def __len__(self):
        return len(self._value)

    @typecheck
    def serialize(self) -> bytes:
        return self._value

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read"), length: int):
        value = r.read(length)
        if len(value) == length:
            return cls(value)
        else:
            raise SMPPTypeReadError("{0:s}.read() has encountered unexpected "
                                    "end of stream".format(cls.__name__))

###############################################################################

class SMPPTLV(SMPPType):

    @typecheck
    def __init__(self, value: (word, bytes)):
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> (word, bytes):
        return (0x0000, b"")

    def __str__(self):
        return "{0:s}(0x{1:04x}, {2:s})".format(self.__class__.__name__, self._value[0],
                                                quote_bytes(self._value[1]))

    @typecheck
    def serialize(self) -> bytes:
        return SMPPInteger2(self._value[0]).serialize() + \
               SMPPInteger2(len(self._value[1])).serialize() + \
               self._value[1]

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read"), length: optional(int) = None):
        tag = SMPPInteger2.read(r).value
        value_length = SMPPInteger2.read(r).value
        assert length in (None, value_length + 4), "length mismatch"
        value = SMPPOctetString.read(r, value_length).value
        return cls((tag, value))

    @classmethod
    @typecheck
    def wrap(cls, tag_instance: (word, SMPPType)):
        tag, instance = tag_instance
        return cls((tag, instance.serialize()))

    @typecheck
    def unwrap(self, cls: type) -> (word, SMPPType):
        return self._value[0], cls.read(BytesIO(self._value[1]))

###############################################################################

class SMPPCompositeType(SMPPType):

    _types = () # ex. (SMPPOctetString, SMPPInteger4)

    @typecheck
    def __init__(self, value: tuple_of(SMPPType)):
        assert len(value) == len(self._types) and \
               reduce(lambda r, t_v: r and isinstance(t_v[1], t_v[0]),
                      zip(self._types, value), True), \
               "type mismatch"
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> tuple_of(SMPPType):
        return tuple(t(t.null()) for t in cls._types)

    def __str__(self):
        return "{0:s}({1:s})".format(self.__class__.__name__, ", ".join(map(str, self._value)))

    def __len__(self):
        return len(self._value)

    def __getitem__(self, i): # supports slices
        return self._value.__getitem__(i)

    @typecheck
    def serialize(self) -> bytes:
        return b"".join(v.serialize() for v in self._value)

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read")):
        return cls(tuple(t.read(r) for t in cls._types))

###############################################################################

class SMPPArray(SMPPType):

    _type = None # ex. SMPPInteger1

    @typecheck
    def __init__(self, value: tuple_of(SMPPType)):
        assert reduce(lambda r, v: r and isinstance(v, self._type),
                      value, True), \
               "type mismatch"
        SMPPType.__init__(self, value)

    @classmethod
    @typecheck
    def null(cls) -> tuple_of(SMPPType):
        return ()

    def __str__(self):
        return "{0:s}({1:s})".format(self.__class__.__name__, ", ".join(map(str, self._value)))

    def __len__(self):
        return len(self._value)

    def __getitem__(self, i): # supports slices
        return self._value.__getitem__(i)

    @typecheck
    def serialize(self) -> bytes:
        return b"".join(item.serialize() for item in self._value)

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read"), items: int):
        return cls(tuple(cls._type.read(r) for i in range(items)))

###############################################################################

class SMPPTypeSwitch(SMPPType):

    _code_type = None  # ex. SMPPInteger1
    _switch = None     # ex. { 0x01: SMPPFirstType, 0x02: SMPPSecondType }

    @typecheck
    def __init__(self, value: (SMPPType, SMPPType)):
        assert isinstance(value[0], self._code_type), "code type mismatch"
        kk = value[0].value
        for k, t in self._switch.items():
            if k == kk:
                assert isinstance(value[1], t), "type mismatch"
                break
        else:
            assert False, "unknown code"
        SMPPType.__init__(self, value)

    def __str__(self):
        return "{0:s}({1[0]:s}, {1[1]:s})".format(self.__class__.__name__, self._value)

    @typecheck
    def serialize(self) -> bytes:
        return self.value[0].serialize() + self.value[1].serialize()

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read")):
        code = cls._code_type.read(r)
        kk = code.value
        for k, t in cls._switch.items():
            if k == kk:
                return cls((code, t.read(r)))
        else:
            raise SMPPTypeReadError("{0:s}.read() has encountered invalid code: "
                                    "{1:s}".format(cls.__name__, code))

###############################################################################

class SMPPGenericAddress: # this is not an SMPP type but a utility class

    @typecheck
    def __init__(self, ton: byte, npi: byte, address: str):
        self._matching = by_regex(address)
        self._ton, self._npi, self._address = ton, npi, address

    ton = property(lambda self: self._ton)
    npi = property(lambda self: self._npi)
    address = property(lambda self: self._address)

    def __eq__(self, other):
        return type(self) is type(other) and self._ton == other._ton and \
               self._npi == other._npi and self._address == other._address

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._ton) ^ hash(self._npi) ^ hash(self._address)

    def __contains__(self, other):
        return self._ton == other._ton and self._npi == other._npi and self._matching(other._address)

###############################################################################

# SubmitMultiPDU requires specific complex subtypes

class SMPPSmeDestAddress(SMPPCompositeType):
    _types = (SMPPInteger1, SMPPInteger1, SMPPCOctetString)

class SMPPDistributionList(SMPPCompositeType):
    _types = (SMPPCOctetString, )

class SMPPDestAddress(SMPPTypeSwitch):
    _code_type = SMPPInteger1
    _switch = { 0x01: SMPPSmeDestAddress, 0x02: SMPPDistributionList }

class SMPPDestAddresses(SMPPArray):
    _type = SMPPDestAddress

###############################################################################

# SubmitMultiRespPDU requires specific complex subtypes

class SMPPUnsuccessSme(SMPPCompositeType):
    _types = (SMPPInteger1, SMPPInteger1, SMPPCOctetString, SMPPInteger4)

class SMPPUnsuccessSmes(SMPPArray):
    _type = SMPPUnsuccessSme

###############################################################################

class SMPPUDHElement: # this is not an SMPP type but a utility class

    @typecheck
    def __init__(self, id: byte, data: bytes):
        self._id, self._data = id, data

    id = property(lambda self: self._id)     # read-only property
    data = property(lambda self: self._data) # read-only property

    def __str__(self):
        return "{0:s}(0x{1:02x}, {2:s})".\
               format(self.__class__.__name__, self._id, quote_bytes(self._data))

    def __repr__(self):
        return "<{0:s} at 0x{1:08x}>".format(self, id(self))

    def __format__(self, spec):
        return format(str(self), spec)

    def __eq__(self, other):
        return type(self) is type(other) and self._id == other._id and self._data == other._data

    def __ne__(self, other):
        return not self.__eq__(other)

    @typecheck
    def serialize(self) -> bytes:
        return SMPPInteger1(self._id).serialize() + \
               SMPPInteger1(len(self._data)).serialize() + \
               self._data

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read")):
        id = SMPPInteger1.read(r).value
        data = SMPPOctetString.read(r, SMPPInteger1.read(r).value).value
        return cls(id, data)

###############################################################################

class SMPPUDH: # this is not an SMPP type but a utility class

    def __init__(self, *elems):
        self._value = tuple(self._init_elem(elem) for elem in elems)

    @typecheck
    def _init_elem(self, elem: either(SMPPUDHElement, (byte, bytes))) -> SMPPUDHElement:
        return elem if isinstance(elem, SMPPUDHElement) else SMPPUDHElement(*elem)

    def __str__(self):
        return "{0:s}({1:s})".format(self.__class__.__name__, ", ".join(str(elem) for elem in self._value))

    def __repr__(self):
        return "<{0:s} at 0x{1:08x}>".format(self, id(self))

    def __format__(self, spec):
        return format(str(self), spec)

    def __eq__(self, other):
        return type(self) is type(other) and self._value == other._value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return len(self._value)

    def __getitem__(self, i): # supports slices
        return self._value.__getitem__(i)

    @typecheck
    def serialize(self) -> bytes:
        udh_b = b"".join(elem.serialize() for elem in self._value)
        return SMPPInteger1(len(udh_b)).serialize() + udh_b

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read")):
        udh_len = SMPPInteger1.read(r).value
        udh_b = SMPPOctetString.read(r, udh_len).value
        udh = BytesIO(udh_b)
        values = []
        while udh.tell() < udh_len:
            values.append(SMPPUDHElement.read(udh))
        return SMPPUDH(*values)

###############################################################################

if __name__ == "__main__":

    print("self-testing module smpp_types.py:")

    from expected import expected

    ###################################

    def f_s(v):
        s = str(v)
        assert "{0}".format(v) == "{0:s}".format(v) == s
        assert repr(v) == "<{0:s} at 0x{1:08x}>".format(s, id(v))
        return s

    ###################################

    def test_SMPPInteger1():

        print("SMPPInteger1: ", end = "")

        assert SMPPInteger1(0x00).value == SMPPInteger1.null()
        assert SMPPInteger1(0x12).value == 0x12

        SMPPInteger1(0xff)
        with expected(InputParameterError):
            SMPPInteger1(-1)
        with expected(InputParameterError):
            SMPPInteger1(0x100)

        assert SMPPInteger1(0x12) == SMPPInteger1(0x12)
        assert SMPPInteger1(0x12) != SMPPInteger1(0x21)
        with expected(TypeError("unorderable types: SMPPInteger1() < SMPPInteger1()")):
            SMPPInteger1(0x12) < SMPPInteger1(0x21)

        assert f_s(SMPPInteger1(0x12)) == "SMPPInteger1(0x12)"

        assert SMPPInteger1(0x12).serialize() == b"\x12"
        r = BytesIO(b"\x12*")
        assert SMPPInteger1.read(r) == SMPPInteger1(0x12) and r.read(1) == b"*"

        with expected(SMPPTypeReadError("SMPPInteger1.read() has encountered unexpected end of stream")):
            SMPPInteger1.read(BytesIO(b""))

        with expected(InputParameterError("read() has got an incompatible value for length: 2")):
            SMPPInteger1.read(BytesIO(b"\x12\x34"), 2)

        assert SMPPInteger1(0xff) in { SMPPInteger1(0xff) }

        print("ok")

    test_SMPPInteger1()

    ###################################

    def test_SMPPInteger2():

        print("SMPPInteger2: ", end = "")

        assert SMPPInteger2(0x0000).value == SMPPInteger2.null()
        assert SMPPInteger2(0x1234).value == 0x1234

        SMPPInteger2(0xffff)
        with expected(InputParameterError):
            SMPPInteger2(-1)
        with expected(InputParameterError):
            SMPPInteger2(0x10000)

        assert SMPPInteger2(0x1234) == SMPPInteger2(0x1234)
        assert SMPPInteger2(0x1234) != SMPPInteger2(0x4321)
        with expected(TypeError("unorderable types: SMPPInteger2() < SMPPInteger2()")):
            SMPPInteger2(0x1234) < SMPPInteger2(0x4321)

        assert f_s(SMPPInteger2(0x1234)) == "SMPPInteger2(0x1234)"

        assert SMPPInteger2(0x1234).serialize() == b"\x12\x34"
        r = BytesIO(b"\x12\x34*")
        assert SMPPInteger2.read(r) == SMPPInteger2(0x1234) and r.read(1) == b"*"

        with expected(SMPPTypeReadError("SMPPInteger2.read() has encountered unexpected end of stream")):
            SMPPInteger2.read(BytesIO(b"\x12"))

        with expected(InputParameterError("read() has got an incompatible value for length: 1")):
            SMPPInteger2.read(BytesIO(b"\x12"), 1)

        assert SMPPInteger2(0xffff) in { SMPPInteger2(0xffff) }

        print("ok")

    test_SMPPInteger2()

    ###################################

    def test_SMPPInteger4():

        print("SMPPInteger4: ", end = "")

        assert SMPPInteger4(0x00000000).value == SMPPInteger4.null()
        assert SMPPInteger4(0x12345678).value == 0x12345678

        SMPPInteger4(0xffffffff)
        with expected(InputParameterError):
            SMPPInteger4(-1)
        with expected(InputParameterError):
            SMPPInteger4(0x100000000)

        assert SMPPInteger4(0x12345678) == SMPPInteger4(0x12345678)
        assert SMPPInteger4(0x12345678) != SMPPInteger4(0x87654321)
        with expected(TypeError("unorderable types: SMPPInteger4() < SMPPInteger4()")):
            SMPPInteger4(0x12345678) < SMPPInteger4(0x87654321)

        assert f_s(SMPPInteger4(0x12345678)) == "SMPPInteger4(0x12345678)"

        assert SMPPInteger4(0x12345678).serialize() == b"\x12\x34\x56\x78"
        r = BytesIO(b"\x12\x34\x56\x78*")
        assert SMPPInteger4.read(r) == SMPPInteger4(0x12345678) and r.read(1) == b"*"

        with expected(SMPPTypeReadError("SMPPInteger4.read() has encountered unexpected end of stream")):
            SMPPInteger4.read(BytesIO(b"\x12\x34\x56"))

        with expected(InputParameterError("read() has got an incompatible value for length: 3")):
            SMPPInteger4.read(BytesIO(b"\x12\x34\x56"), 3)

        assert SMPPInteger4(0xffffffff) in { SMPPInteger4(0xffffffff) }

        print("ok")

    test_SMPPInteger4()

    ###################################

    def test_SMPPCOctetString():

        print("SMPPCOctetString: ", end = "")

        assert SMPPCOctetString(b"").value == SMPPCOctetString.null()
        assert SMPPCOctetString(b"").serialize() == b"\x00"
        assert SMPPCOctetString(b"foo").serialize() == b"foo\x00"
        assert SMPPCOctetString(b"").value == b""
        assert SMPPCOctetString(b"foo").value == b"foo"
        assert len(SMPPCOctetString(b"foo")) == 3
        assert len(SMPPCOctetString(b"")) == 0
        with expected(InputParameterError):
            SMPPCOctetString("foo")
        with expected(AssertionError("invalid byte in \"\\x00foo\"")):
            SMPPCOctetString(b"\x00foo")
        assert SMPPCOctetString(bytes(range(1, 256))).value == bytes(range(1, 256))
        with expected(AssertionError("invalid byte")):
            SMPPCOctetString(bytes(range(1, 256)) + b"\x00")

        assert SMPPCOctetString(b"foo") == SMPPCOctetString(b"foo")
        assert SMPPCOctetString(b"biz") != SMPPCOctetString(b"baz")
        with expected(TypeError("unorderable types: SMPPCOctetString() < SMPPCOctetString()")):
            SMPPCOctetString(b"biz") < SMPPCOctetString(b"baz")

        assert f_s(SMPPCOctetString(b"\x01foo\"\xff")) == "SMPPCOctetString(\"\\x01foo\\\"\\xff\")"

        r = BytesIO(b"foo\x00*")
        assert SMPPCOctetString.read(r) == SMPPCOctetString(b"foo") and r.read(1) == b"*"
        with expected(SMPPTypeReadError("SMPPCOctetString.read() has encountered unexpected end of stream")):
            SMPPCOctetString.read(BytesIO(b"foo"))

        r = BytesIO(b"foo\x00*")
        assert SMPPCOctetString.read(r, 4) == SMPPCOctetString(b"foo") and r.read(1) == b"*"
        with expected(SMPPTypeReadError("SMPPCOctetString.read() has exceeded its maximum allowed length of 3 byte(s)")):
            SMPPCOctetString.read(BytesIO(b"foo\x00"), 3)

        with expected(SMPPTypeReadError("SMPPCOctetString.read() has exceeded its maximum allowed length of 1 byte(s)")):
            SMPPCOctetString.read(BytesIO(b""), 1)

        assert SMPPCOctetString(b"foo") in { SMPPCOctetString(b"foo") }

        print("ok")

    test_SMPPCOctetString()

    ###################################

    def test_SMPPCOctetStringDecimal():

        print("SMPPCOctetStringDecimal: ", end = "")

        assert SMPPCOctetStringDecimal(b"").value == SMPPCOctetStringDecimal.null()
        assert SMPPCOctetStringDecimal(b"").serialize() == b"\x00"
        assert SMPPCOctetStringDecimal(b"0123456789").serialize() == b"0123456789\x00"
        assert SMPPCOctetStringDecimal(b"").value == b""
        assert SMPPCOctetStringDecimal(b"0123456789").value == b"0123456789"
        assert len(SMPPCOctetStringDecimal(b"123")) == 3
        assert len(SMPPCOctetStringDecimal(b"")) == 0
        with expected(InputParameterError):
            SMPPCOctetStringDecimal("0123456789")
        with expected(AssertionError("invalid byte in \"foo\"")):
            SMPPCOctetStringDecimal(b"foo")

        assert SMPPCOctetStringDecimal(b"0123456789") == SMPPCOctetStringDecimal(b"0123456789")
        assert SMPPCOctetStringDecimal(b"0123456789") != SMPPCOctetStringDecimal(b"9876543210")
        with expected(TypeError("unorderable types: SMPPCOctetStringDecimal() < SMPPCOctetStringDecimal()")):
            SMPPCOctetStringDecimal(b"0123456789") < SMPPCOctetStringDecimal(b"9876543210")

        assert f_s(SMPPCOctetStringDecimal(b"0123456789")) == "SMPPCOctetStringDecimal(\"0123456789\")"

        r = BytesIO(b"123\x00*")
        assert SMPPCOctetStringDecimal.read(r) == SMPPCOctetStringDecimal(b"123") and r.read(1) == b"*"
        with expected(SMPPTypeReadError("SMPPCOctetStringDecimal.read() has encountered invalid byte: \"x\"")):
            SMPPCOctetStringDecimal.read(BytesIO(b"12x"))
        with expected(SMPPTypeReadError("SMPPCOctetStringDecimal.read() has encountered unexpected end of stream")):
            SMPPCOctetStringDecimal.read(BytesIO(b"123"))

        with expected(SMPPTypeReadError("SMPPCOctetStringDecimal.read() has exceeded its maximum allowed length of 3 byte(s)")):
            SMPPCOctetStringDecimal.read(BytesIO(b"123\x00"), 3)

        with expected(SMPPTypeReadError("SMPPCOctetStringDecimal.read() has exceeded its maximum allowed length of 1 byte(s)")):
            SMPPCOctetStringDecimal.read(BytesIO(b""), 1)

        assert SMPPCOctetStringDecimal(b"123") in { SMPPCOctetStringDecimal(b"123") }

        print("ok")

    test_SMPPCOctetStringDecimal()

    ###################################

    def test_SMPPCOctetStringHex():

        print("SMPPCOctetStringHex: ", end = "")

        assert SMPPCOctetStringHex(b"").value == SMPPCOctetStringHex.null()
        assert SMPPCOctetStringHex(b"").serialize() == b"\x00"
        assert SMPPCOctetStringHex(b"0123456789ABCDEF").serialize() == b"0123456789ABCDEF\x00"
        assert SMPPCOctetStringHex(b"").value == b""
        assert SMPPCOctetStringHex(b"0123456789ABCDEF").value == b"0123456789ABCDEF"
        assert len(SMPPCOctetStringHex(b"ABC")) == 3
        assert len(SMPPCOctetStringHex(b"")) == 0
        with expected(InputParameterError):
            SMPPCOctetStringHex("0123456789ABCDEF")
        with expected(AssertionError("invalid byte in \"foo\"")):
            SMPPCOctetStringHex(b"foo")

        assert SMPPCOctetStringHex(b"0123456789ABCDEF") == SMPPCOctetStringHex(b"0123456789ABCDEF")
        assert SMPPCOctetStringHex(b"0123456789ABCDEF") != SMPPCOctetStringHex(b"fedcba9876543210")
        with expected(TypeError("unorderable types: SMPPCOctetStringHex() < SMPPCOctetStringHex()")):
            SMPPCOctetStringHex(b"0123456789ABCDEF") < SMPPCOctetStringHex(b"fedcba9876543210")

        assert f_s(SMPPCOctetStringHex(b"0123456789ABCDEF")) == "SMPPCOctetStringHex(\"0123456789ABCDEF\")"

        r = BytesIO(b"ABC\x00*")
        assert SMPPCOctetStringHex.read(r) == SMPPCOctetStringHex(b"ABC") and r.read(1) == b"*"
        with expected(SMPPTypeReadError("SMPPCOctetStringHex.read() has encountered invalid byte: \"G\"")):
            SMPPCOctetStringHex.read(BytesIO(b"ABCDEFG"))
        with expected(SMPPTypeReadError("SMPPCOctetStringHex.read() has encountered unexpected end of stream")):
            SMPPCOctetStringHex.read(BytesIO(b"123"))

        with expected(SMPPTypeReadError("SMPPCOctetStringHex.read() has exceeded its maximum allowed length of 3 byte(s)")):
            SMPPCOctetStringHex.read(BytesIO(b"123\x00"), 3)

        with expected(SMPPTypeReadError("SMPPCOctetStringHex.read() has exceeded its maximum allowed length of 1 byte(s)")):
            SMPPCOctetStringHex.read(BytesIO(b""), 1)

        assert SMPPCOctetStringHex(b"ABC") in { SMPPCOctetStringHex(b"ABC") }

        print("ok")

    test_SMPPCOctetStringHex()

    ###################################

    def test_SMPPOctetString():

        print("SMPPOctetString: ", end = "")

        assert SMPPOctetString(b"").value == SMPPOctetString.null()
        assert SMPPOctetString(b"").serialize() == b""
        assert SMPPOctetString(bytes(range(256))).serialize() == bytes(range(256))
        assert len(SMPPOctetString(b"foo")) == 3
        assert len(SMPPOctetString(b"")) == 0
        with expected(InputParameterError):
            SMPPOctetString("foo")

        assert SMPPOctetString(b"foo") == SMPPOctetString(b"foo")
        assert SMPPOctetString(b"biz") != SMPPOctetString(b"baz")
        with expected(TypeError("unorderable types: SMPPOctetString() < SMPPOctetString()")):
            SMPPOctetString(b"biz") < SMPPOctetString(b"baz")

        assert f_s(SMPPOctetString(b"\x00foo\"\xff")) == "SMPPOctetString(\"\\x00foo\\\"\\xff\")"

        r = BytesIO(b"foo*")
        assert SMPPOctetString.read(r, 3) == SMPPOctetString(b"foo") and r.read(1) == b"*"

        with expected(SMPPTypeReadError("SMPPOctetString.read() has encountered unexpected end of stream")):
            SMPPOctetString.read(BytesIO(b""), 1)

        with expected(SMPPTypeReadError("SMPPOctetString.read() has encountered unexpected end of stream")):
            SMPPOctetString.read(BytesIO(b"12"), 3)

        assert SMPPOctetString(b"\x00") in { SMPPOctetString(b"\x00") }

        print("ok")

    test_SMPPOctetString()

    ###################################

    def test_SMPPTLV():

        print("SMPPTLV: ", end = "")

        assert f_s(SMPPTLV((0, b""))) == "SMPPTLV(0x0000, \"\")"
        assert f_s(SMPPTLV((0xffff, bytes(range(256))))) == "SMPPTLV(0xffff, {0:s})".format(quote_bytes(bytes(range(256))))
        with expected(InputParameterError):
            SMPPTLV((-1, b""))
        with expected(InputParameterError):
            SMPPTLV((0x10000, b""))
        with expected(InputParameterError):
            SMPPTLV((0, ""))

        tlv = SMPPTLV((0x1234, b"foo"))
        assert tlv.value == (0x1234, b"foo")

        assert SMPPTLV(SMPPTLV.null()).serialize() == b"\x00\x00\x00\x00"
        assert SMPPTLV((0x1234, b"foo")).serialize() == b"\x12\x34\x00\x03foo"

        assert SMPPTLV((5, b"foo")) == SMPPTLV((5, b"foo"))
        assert SMPPTLV((5, b"biz")) != SMPPTLV((5, b"baz"))
        assert SMPPTLV((4, b"foo")) != SMPPTLV((5, b"foo"))
        with expected(TypeError("unorderable types: SMPPTLV() < SMPPTLV()")):
            SMPPTLV((5, b"biz")) < SMPPTLV((7, b"baz"))

        r = BytesIO(b"\x12\x34\x00\x03foo*")
        assert SMPPTLV.read(r) == SMPPTLV((0x1234, b"foo")) and r.read(1) == b"*"
        assert SMPPTLV.read(BytesIO(b"\x12\x34\x00\x00")) == SMPPTLV((0x1234, b""))

        with expected(SMPPTypeReadError("SMPPInteger2.read() has encountered unexpected end of stream")):
            SMPPTLV.read(BytesIO(b"\x12"))
        with expected(SMPPTypeReadError("SMPPInteger2.read() has encountered unexpected end of stream")):
            SMPPTLV.read(BytesIO(b"\x12\x34\x00"))
        with expected(SMPPTypeReadError("SMPPOctetString.read() has encountered unexpected end of stream")):
            SMPPTLV.read(BytesIO(b"\x12\x34\x00\x01"))
        with expected(SMPPTypeReadError("SMPPOctetString.read() has encountered unexpected end of stream")):
            SMPPTLV.read(BytesIO(b"\x12\x34\x00\x03fo"))

        assert SMPPTLV.read(BytesIO(b"\xff\xff\x00\x01x"), 5).value == (0xffff, b"x")
        with expected(AssertionError("length mismatch")):
            assert SMPPTLV.read(BytesIO(b"\xff\xff\x00\x01x"), 6)

        assert SMPPTLV.wrap((0x1234, SMPPInteger1(0x12))) == SMPPTLV((0x1234, b"\x12"))
        assert SMPPTLV.wrap((0xffff, SMPPInteger2(0x1234))) == SMPPTLV((0xffff, b"\x12\x34"))
        assert SMPPTLV.wrap((0x55aa, SMPPInteger4(0x12345678))) == SMPPTLV((0x55aa, b"\x12\x34\x56\x78"))
        assert SMPPTLV.wrap((0xaa55, SMPPCOctetString(b"foo"))) == SMPPTLV((0xaa55, b"foo\x00"))
        assert SMPPTLV.wrap((0x0001, SMPPCOctetStringDecimal(b"123"))) == SMPPTLV((0x0001, b"123\x00"))
        assert SMPPTLV.wrap((0x8000, SMPPCOctetStringHex(b"123ABC"))) == SMPPTLV((0x8000, b"123ABC\x00"))
        assert SMPPTLV.wrap((0x00ff, SMPPOctetString(bytes(range(256))))) == SMPPTLV((0x00ff, bytes(range(256))))

        assert SMPPTLV((0x0000, b"\x12")).unwrap(SMPPInteger1) == (0x0000, SMPPInteger1(0x12))
        assert SMPPTLV((0x00ff, b"\x12\x34")).unwrap(SMPPInteger2) == (0x00ff, SMPPInteger2(0x1234))
        assert SMPPTLV((0xff00, b"\x12\x34\x56\x78")).unwrap(SMPPInteger4) == (0xff00, SMPPInteger4(0x12345678))
        assert SMPPTLV((0x5555, b"foo\x00")).unwrap(SMPPCOctetString) == (0x5555, SMPPCOctetString(b"foo"))
        assert SMPPTLV((0xaaaa, b"f\x00o\x00")).unwrap(SMPPCOctetString) == (0xaaaa, SMPPCOctetString(b"f"))
        assert SMPPTLV((0xf00f, b"123\x00")).unwrap(SMPPCOctetStringDecimal) == (0xf00f, SMPPCOctetStringDecimal(b"123"))
        assert SMPPTLV((0x0ff0, b"123ABC\x00")).unwrap(SMPPCOctetStringHex) == (0x0ff0, SMPPCOctetStringHex(b"123ABC"))
        with expected(TypeError): # cannot unwrap a not self-delimiting value
            SMPPTLV((0x0000, bytes(range(256)))).unwrap(SMPPOctetString)

        assert SMPPTLV((0x1234, b"foo")) in { SMPPTLV((0x1234, b"foo")) }

        print("ok")

    test_SMPPTLV()

    ###################################

    def test_SMPPCompositeType():

        print("SMPPCompositeType: ", end = "")

        assert SMPPCompositeType(()).value == ()
        assert SMPPCompositeType(()).serialize() == b""
        assert SMPPCompositeType.read(BytesIO(b"")).value == ()
        assert f_s(SMPPCompositeType(())) == "SMPPCompositeType()"

        class SMPPCompositeTypeSimple(SMPPCompositeType):
            _types = (SMPPInteger1, SMPPInteger2, SMPPInteger4,
                      SMPPCOctetString, SMPPCOctetStringDecimal, SMPPCOctetStringHex)

        p = SMPPCompositeTypeSimple((SMPPInteger1(0x01), SMPPInteger2(0x0203), SMPPInteger4(0x04050607),
                                     SMPPCOctetString(b"foo"), SMPPCOctetStringDecimal(b"123"), SMPPCOctetStringHex(b"123ABC")))

        assert len(p) == 6
        assert p[0] == SMPPInteger1(0x01)
        assert p[1] == SMPPInteger2(0x0203)
        assert p[2] == SMPPInteger4(0x04050607)
        assert p[3] == SMPPCOctetString(b"foo")
        assert p[4] == SMPPCOctetStringDecimal(b"123")
        assert p[5] == SMPPCOctetStringHex(b"123ABC")

        assert f_s(p) == "SMPPCompositeTypeSimple(SMPPInteger1(0x01), SMPPInteger2(0x0203), " \
                                                 "SMPPInteger4(0x04050607), SMPPCOctetString(\"foo\"), " \
                                                 "SMPPCOctetStringDecimal(\"123\"), SMPPCOctetStringHex(\"123ABC\"))"
        ps = p.serialize()
        assert ps == b"\x01\x02\x03\x04\x05\x06\x07foo\x00123\x00123ABC\x00"
        r = BytesIO(ps + b"*")
        assert SMPPCompositeTypeSimple.read(r) == p and r.read(1) == b"*"

        assert SMPPCompositeTypeSimple(SMPPCompositeTypeSimple.null()).serialize() == \
               b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

        # now a composite from composites

        def test_composite(cls):
            null = cls(cls.null())
            assert cls.read(BytesIO(null.serialize())) == null
            with expected(AssertionError("type mismatch")):
                cls((SMPPOctetString(b""), ))
            assert null in { null }

        class SMPPCompositeTypeInt1(SMPPCompositeType):
            _types = (SMPPInteger1, )
        test_composite(SMPPCompositeTypeInt1)

        class SMPPCompositeTypeInt2(SMPPCompositeType):
            _types = (SMPPInteger2, )
        test_composite(SMPPCompositeTypeInt2)

        class SMPPCompositeTypeInt4(SMPPCompositeType):
            _types = (SMPPInteger4, )
        test_composite(SMPPCompositeTypeInt4)

        class SMPPCompositeTypeCOctetString(SMPPCompositeType):
            _types = (SMPPCOctetString, )
        test_composite(SMPPCompositeTypeCOctetString)

        class SMPPCompositeTypeCOctetStringDecimal(SMPPCompositeType):
            _types = (SMPPCOctetStringDecimal, )
        test_composite(SMPPCompositeTypeCOctetStringDecimal)

        class SMPPCompositeTypeCOctetStringHex(SMPPCompositeType):
            _types = (SMPPCOctetStringHex, )
        test_composite(SMPPCompositeTypeCOctetStringHex)

        class SMPPCompositeTypeComposite(SMPPCompositeType):
            _types = (SMPPCompositeTypeInt1, SMPPCompositeTypeInt2, SMPPCompositeTypeInt4,
                      SMPPCompositeTypeCOctetString, SMPPCompositeTypeCOctetStringDecimal, SMPPCompositeTypeCOctetStringHex)

        c = SMPPCompositeTypeComposite((SMPPCompositeTypeInt1((SMPPInteger1(0x01), )),
                                        SMPPCompositeTypeInt2((SMPPInteger2(0x0203), )),
                                        SMPPCompositeTypeInt4((SMPPInteger4(0x04050607), )),
                                        SMPPCompositeTypeCOctetString((SMPPCOctetString(b"foo"), )),
                                        SMPPCompositeTypeCOctetStringDecimal((SMPPCOctetStringDecimal(b"123"), )),
                                        SMPPCompositeTypeCOctetStringHex((SMPPCOctetStringHex(b"123ABC"), ))))

        cs = c.serialize()
        assert cs == b"\x01\x02\x03\x04\x05\x06\x07foo\x00123\x00123ABC\x00"
        r = BytesIO(cs + b"*")
        assert SMPPCompositeTypeComposite.read(r) == c and r.read(1) == b"*"

        # wrapping composite in a TLV and vice versa

        class SMPPCompositeTLV(SMPPCompositeType):
            _types = (SMPPTLV, )

        class SMPPCompositeTLV2(SMPPCompositeType):
            _types = (SMPPCompositeTLV, )

        tlv = SMPPTLV((0x1234, b"\x00"))
        assert f_s(tlv) == "SMPPTLV(0x1234, \"\\x00\")"
        assert tlv.serialize() == b"\x12\x34\x00\x01\x00"
        assert SMPPTLV.read(BytesIO(b"\x12\x34\x00\x01\x00")) == tlv
        wtlv = SMPPTLV.wrap((0x0001, tlv))
        assert wtlv.serialize() == b"\x00\x01\x00\x05\x12\x34\x00\x01\x00"
        assert wtlv.unwrap(SMPPTLV) == (0x0001, tlv)

        c1 = SMPPCompositeTLV((tlv, ))
        assert f_s(c1) == "SMPPCompositeTLV(SMPPTLV(0x1234, \"\\x00\"))"
        assert c1.serialize() == b"\x12\x34\x00\x01\x00"
        assert SMPPCompositeTLV.read(BytesIO(c1.serialize())) == c1
        wc1 = SMPPTLV.wrap((0x0002, c1))
        assert wc1.serialize() == b"\x00\x02\x00\x05\x12\x34\x00\x01\x00"
        assert wc1.unwrap(SMPPCompositeTLV) == (0x0002, c1)
        assert wc1.unwrap(SMPPTLV) == (0x0002, tlv)

        c2 = SMPPCompositeTLV2((c1, ))
        assert f_s(c2) == "SMPPCompositeTLV2(SMPPCompositeTLV(SMPPTLV(0x1234, \"\\x00\")))"
        assert c2.serialize() == b"\x12\x34\x00\x01\x00"
        assert SMPPCompositeTLV2.read(BytesIO(c2.serialize())) == c2
        wc2 = SMPPTLV.wrap((0x0003, c2))
        assert wc2.serialize() == b"\x00\x03\x00\x05\x12\x34\x00\x01\x00"
        assert wc2.unwrap(SMPPCompositeTLV2) == (0x0003, c2)
        assert wc2.unwrap(SMPPCompositeTLV) == (0x0003, c1)
        assert wc2.unwrap(SMPPTLV) == (0x0003, tlv)

        c = SMPPCompositeTypeComposite((SMPPCompositeTypeInt1((SMPPInteger1(0x01), )),
                                        SMPPCompositeTypeInt2((SMPPInteger2(0x0203), )),
                                        SMPPCompositeTypeInt4((SMPPInteger4(0x04050607), )),
                                        SMPPCompositeTypeCOctetString((SMPPCOctetString(b"foo"), )),
                                        SMPPCompositeTypeCOctetStringDecimal((SMPPCOctetStringDecimal(b"123"), )),
                                        SMPPCompositeTypeCOctetStringHex((SMPPCOctetStringHex(b"123ABC"), ))))

        assert SMPPTLV.wrap((0xffff, c)).unwrap(SMPPCompositeTypeComposite)[1] == c

        print("ok")

    test_SMPPCompositeType()

    ###################################

    def test_SMPPArray():

        print("SMPPArray: ", end = "")

        assert SMPPArray(()).serialize() == b""
        assert SMPPArray.read(BytesIO(b""), 0).value == SMPPArray.null()
        assert SMPPArray(SMPPArray.null()).serialize() == b""
        assert f_s(SMPPArray(())) == "SMPPArray()"

        def test_array_of(cls):

            class ArrayType(SMPPArray):
                _type = cls

            cls_null = cls(cls.null())
            a = ArrayType((cls_null, cls_null))
            assert len(a) == 2
            assert a[0] == cls_null
            assert a[1] == cls_null
            assert a[::2] == (cls_null, )
            assert a in { a }
            ss = a.serialize()
            assert ss == cls_null.serialize() * 2
            r = BytesIO(ss + b"*")
            assert ArrayType.read(r, 2) == a and r.read(1) == b"*"
            assert ArrayType.read(BytesIO(ss), 2) == ArrayType((cls_null, cls_null))
            with expected(SMPPTypeReadError):
                ArrayType.read(BytesIO(ss), 3)

            with expected(AssertionError("type mismatch")):
                ArrayType((SMPPOctetString(SMPPOctetString.null()), ))

        test_array_of(SMPPInteger1)
        test_array_of(SMPPInteger2)
        test_array_of(SMPPInteger4)
        test_array_of(SMPPCOctetString)
        test_array_of(SMPPCOctetStringDecimal)
        test_array_of(SMPPCOctetStringHex)
        test_array_of(SMPPTLV)

        class SMPPCompositeTypeInt1(SMPPCompositeType):
            _types = (SMPPInteger1, )

        class SMPPCompositeTypeInt2(SMPPCompositeType):
            _types = (SMPPInteger2, )

        class SMPPCompositeTypeInt4(SMPPCompositeType):
            _types = (SMPPInteger4, )

        class SMPPCompositeTypeCOctetString(SMPPCompositeType):
            _types = (SMPPCOctetString, )

        class SMPPCompositeTypeCOctetStringDecimal(SMPPCompositeType):
            _types = (SMPPCOctetStringDecimal, )

        class SMPPCompositeTypeCOctetStringHex(SMPPCompositeType):
            _types = (SMPPCOctetStringHex, )

        class SMPPCompositeTypeComposite(SMPPCompositeType):
            _types = (SMPPCompositeTypeInt1, SMPPCompositeTypeInt2, SMPPCompositeTypeInt4,
                      SMPPCompositeTypeCOctetString, SMPPCompositeTypeCOctetStringDecimal, SMPPCompositeTypeCOctetStringHex)

        test_array_of(SMPPCompositeTypeComposite)

        print("ok")

    test_SMPPArray()

    ################################### type switch

    def test_SMPPTypeSwitch():

        print("SMPPTypeSwitch: ", end = "")

        class SMPPTypeSwitchInt1(SMPPTypeSwitch):
            _code_type = SMPPInteger1
            _switch = { 0x01: SMPPInteger2, 0x02: SMPPCOctetString }

        s1 = SMPPTypeSwitchInt1((SMPPInteger1(0x01), SMPPInteger2(0x2345)))
        assert f_s(s1) == "SMPPTypeSwitchInt1(SMPPInteger1(0x01), SMPPInteger2(0x2345))"

        s2 = SMPPTypeSwitchInt1((SMPPInteger1(0x02), SMPPCOctetString(b"foo")))
        assert f_s(s2) == "SMPPTypeSwitchInt1(SMPPInteger1(0x02), SMPPCOctetString(\"foo\"))"

        with expected(AssertionError("type mismatch")):
            SMPPTypeSwitchInt1((SMPPInteger1(0x01), SMPPCOctetString(b"foo")))

        with expected(AssertionError("type mismatch")):
            SMPPTypeSwitchInt1((SMPPInteger1(0x02), SMPPInteger2(0x2345)))

        with expected(AssertionError("unknown code")):
            SMPPTypeSwitchInt1((SMPPInteger1(0xff), SMPPInteger4(0x12345678)))

        with expected(AssertionError("code type mismatch")):
            SMPPTypeSwitchInt1((SMPPInteger2(0x01), SMPPInteger2(0x2345)))

        class SMPPAllTypeSwitch(SMPPTypeSwitch):
            _code_type = SMPPInteger2
            _switch = { 0x0001: SMPPInteger1, 0x0002: SMPPInteger2, 0x0004: SMPPInteger4,
                        0x8001: SMPPCOctetString, 0x8002: SMPPCOctetStringDecimal,
                        0x8003: SMPPCOctetStringHex, 0x8004: SMPPOctetString }

        def test_sats(c, v, b, s):
            sats = SMPPAllTypeSwitch((SMPPInteger2(c), v))
            assert f_s(sats) == s
            assert sats.serialize() == b
            r = BytesIO(sats.serialize() + b"*")
            assert SMPPAllTypeSwitch.read(r) == sats and r.read(1) == b"*"
            assert sats in { sats }

        test_sats(0x0001, SMPPInteger1(0x12), b"\x00\x01\x12",
                  "SMPPAllTypeSwitch(SMPPInteger2(0x0001), SMPPInteger1(0x12))")

        test_sats(0x0002, SMPPInteger2(0x1234), b"\x00\x02\x12\x34",
                  "SMPPAllTypeSwitch(SMPPInteger2(0x0002), SMPPInteger2(0x1234))")

        test_sats(0x0004, SMPPInteger4(0x12345678), b"\x00\x04\x12\x34\x56\x78",
                  "SMPPAllTypeSwitch(SMPPInteger2(0x0004), SMPPInteger4(0x12345678))")

        test_sats(0x8001, SMPPCOctetString(b"\xff"), b"\x80\x01\xff\x00",
                  "SMPPAllTypeSwitch(SMPPInteger2(0x8001), SMPPCOctetString(\"\\xff\"))")

        test_sats(0x8002, SMPPCOctetStringDecimal(b"0"), b"\x80\x020\x00",
                  "SMPPAllTypeSwitch(SMPPInteger2(0x8002), SMPPCOctetStringDecimal(\"0\"))")

        test_sats(0x8003, SMPPCOctetStringHex(b"A"), b"\x80\x03A\x00",
                  "SMPPAllTypeSwitch(SMPPInteger2(0x8003), SMPPCOctetStringHex(\"A\"))")

        with expected(TypeError):
            test_sats(0x8004, SMPPOctetString(b"\x00"), b"\x80\x04\x00",
                      "SMPPAllTypeSwitch(SMPPInteger2(0x8004), SMPPOctetString(\"\\x00\"))")

        print("ok")

    test_SMPPTypeSwitch()

    ################################### type combinations

    def test_SMPPTypeArrays():

        print("complex type arrays: ", end = "")

        # we have two complex types - composite and switch

        # array of composite

        class SMPPCompositeTypeX(SMPPCompositeType):
            _types = (SMPPInteger1, SMPPCOctetString)

        class SMPPArrayOfComposite(SMPPArray):
            _type = SMPPCompositeTypeX

        s = SMPPArrayOfComposite((SMPPCompositeTypeX((SMPPInteger1(0x01), SMPPCOctetString(b"foo"))),
                                  SMPPCompositeTypeX((SMPPInteger1(0x02), SMPPCOctetString(b"bar")))))
        assert f_s(s) == "SMPPArrayOfComposite(SMPPCompositeTypeX(SMPPInteger1(0x01), SMPPCOctetString(\"foo\")), " \
                                              "SMPPCompositeTypeX(SMPPInteger1(0x02), SMPPCOctetString(\"bar\")))"
        assert s.serialize() == b"\x01foo\x00\x02bar\x00"
        r = BytesIO(s.serialize() + b"*")
        assert SMPPArrayOfComposite.read(r, 2) == s and r.read(1) == b"*"

        # array of switch

        class SMPPSwitchX(SMPPTypeSwitch):
            _code_type = SMPPInteger1
            _switch = { 0x01: SMPPInteger2, 0x02: SMPPCOctetString }

        class SMPPArrayOfSwitch(SMPPArray):
            _type = SMPPSwitchX

        s = SMPPArrayOfSwitch((SMPPSwitchX((SMPPInteger1(0x01), SMPPInteger2(0x1234))),
                               SMPPSwitchX((SMPPInteger1(0x02), SMPPCOctetString(b"foo")))))
        assert f_s(s) == "SMPPArrayOfSwitch(SMPPSwitchX(SMPPInteger1(0x01), SMPPInteger2(0x1234)), " \
                                           "SMPPSwitchX(SMPPInteger1(0x02), SMPPCOctetString(\"foo\")))"
        assert s.serialize() == b"\x01\x12\x34\x02foo\x00"
        r = BytesIO(s.serialize() + b"*")
        assert SMPPArrayOfSwitch.read(r, 2) == s and r.read(1) == b"*"

        # array of switch of composite (real example)

        class SMPPSmeDestAddress(SMPPCompositeType):
            _types = (SMPPInteger1, SMPPInteger1, SMPPCOctetString)

        class SMPPDistributionList(SMPPCompositeType):
            _types = (SMPPCOctetString, )

        class SMPPDestAddress(SMPPTypeSwitch):
            _code_type = SMPPInteger1
            _switch = { 0x01: SMPPSmeDestAddress, 0x02: SMPPDistributionList }

        class SMPPDestAddresses(SMPPArray):
            _type = SMPPDestAddress

        s = SMPPDestAddresses((SMPPDestAddress((SMPPInteger1(0x01), SMPPSmeDestAddress((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo"))))),
                               SMPPDestAddress((SMPPInteger1(0x02), SMPPDistributionList((SMPPCOctetString(b"bar"), ))))))
        assert f_s(s) == "SMPPDestAddresses(SMPPDestAddress(SMPPInteger1(0x01), SMPPSmeDestAddress(SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(\"foo\"))), " \
                                           "SMPPDestAddress(SMPPInteger1(0x02), SMPPDistributionList(SMPPCOctetString(\"bar\"))))"
        assert s.serialize() == b"\x01\x12\x34foo\x00\x02bar\x00"
        r = BytesIO(s.serialize() + b"*")
        assert SMPPDestAddresses.read(r, 2) == s and r.read(1) == b"*"

        print("ok")

    test_SMPPTypeArrays()

    ################################### SMPPGenericAddress

    def test_SMPPGenericAddress():

        print("SMPPGenericAddress: ", end = "")

        a1 = SMPPGenericAddress(0x01, 0x02, "foo")
        assert a1.ton == 0x01 and a1.npi == 0x02 and a1.address == "foo"
        assert a1 == SMPPGenericAddress(0x01, 0x02, "foo")
        assert a1 != SMPPGenericAddress(0x01, 0x02, "foo!")
        assert a1 != SMPPGenericAddress(0x01, 0x01, "foo")
        assert a1 != SMPPGenericAddress(0x02, 0x02, "foo")

        a2 = SMPPGenericAddress(0x01, 0x02, "-foo-")
        assert a1 in a1 and a2 in a2 and a1 not in a2 and a2 not in a1

        assert a1 in SMPPGenericAddress(0x01, 0x02, "fo+")
        assert a1 not in SMPPGenericAddress(0x02, 0x02, "fo+")
        assert a1 not in SMPPGenericAddress(0x01, 0x01, "fo+")

        assert a1 in { a1 } and a2 in { a2 }

        print("ok")

    test_SMPPGenericAddress()

    ################################### SMPPDestAddresses

    def test_SMPPDestAddresses():

        print("SMPPDestAddresses: ", end = "")

        sda = SMPPSmeDestAddress((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo")))

        dl = SMPPDistributionList((SMPPCOctetString(b"bar"), ))

        da1 = SMPPDestAddress((SMPPInteger1(0x01), sda))
        with expected(AssertionError("type mismatch")):
            SMPPDestAddress((SMPPInteger1(0x02), sda))

        da2 = SMPPDestAddress((SMPPInteger1(0x02), dl))
        with expected(AssertionError("type mismatch")):
            SMPPDestAddress((SMPPInteger1(0x01), dl))

        das = SMPPDestAddresses((da1, da2))
        assert f_s(das) == \
               "SMPPDestAddresses(SMPPDestAddress(SMPPInteger1(0x01), SMPPSmeDestAddress(SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(\"foo\"))), " \
                                 "SMPPDestAddress(SMPPInteger1(0x02), SMPPDistributionList(SMPPCOctetString(\"bar\"))))"

        assert das.serialize() == b"\x01\x12\x34foo\x00\x02bar\x00"
        assert SMPPDestAddresses.read(BytesIO(das.serialize()), 2) == das

        print("ok")

    test_SMPPDestAddresses()

    ################################### SMPPDestAddresses

    def test_SMPPUnsuccessSmes():

        print("SMPPUnsuccessSmes: ", end = "")

        us1 = SMPPUnsuccessSme((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo"), SMPPInteger4(0xffffffff)))
        us2 = SMPPUnsuccessSme((SMPPInteger1(0x56), SMPPInteger1(0x78), SMPPCOctetString(b"bar"), SMPPInteger4(0x00000000)))

        uss = SMPPUnsuccessSmes((us1, us2))
        assert f_s(uss) == \
               "SMPPUnsuccessSmes(SMPPUnsuccessSme(SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(\"foo\"), SMPPInteger4(0xffffffff)), " \
                                 "SMPPUnsuccessSme(SMPPInteger1(0x56), SMPPInteger1(0x78), SMPPCOctetString(\"bar\"), SMPPInteger4(0x00000000)))"

        assert uss.serialize() == b"\x12\x34foo\x00\xff\xff\xff\xff\x56\x78bar\x00\x00\x00\x00\x00"
        assert SMPPUnsuccessSmes.read(BytesIO(uss.serialize()), 2) == uss

        print("ok")

    test_SMPPUnsuccessSmes()

    ################################### SMPPUDHElement

    def test_SMPPUDHElement():

        print("SMPPUDHElement: ", end = "")

        udhe1 = SMPPUDHElement(0x00, b"")
        assert udhe1.serialize() == b"\x00\x00"
        assert SMPPUDHElement.read(BytesIO(udhe1.serialize())) == udhe1

        assert udhe1.id == 0x00
        assert udhe1.data == b""
        assert f_s(udhe1) == "SMPPUDHElement(0x00, \"\")"

        udhe2 = SMPPUDHElement(0xff, b"f\x00o")
        assert udhe2.serialize() == b"\xff\x03f\x00o"
        assert SMPPUDHElement.read(BytesIO(udhe2.serialize())) == udhe2

        assert udhe2.id == 0xff
        assert udhe2.data == b"f\x00o"
        assert f_s(udhe2) == "SMPPUDHElement(0xff, \"f\\x00o\")"

        assert udhe1 != udhe2

        print("ok")

    test_SMPPUDHElement()

    ################################### SMPPUDHElement

    def test_SMPPUDH():

        print("SMPPUDH: ", end = "")

        udh1 = SMPPUDH()
        assert udh1.serialize() == b"\x00"
        assert SMPPUDH.read(BytesIO(udh1.serialize())) == udh1

        assert len(udh1) == 0
        assert f_s(udh1) == "SMPPUDH()"

        udh2 = SMPPUDH((0xff, b"f\x00o"))
        assert udh2.serialize() == b"\x05\xff\x03f\x00o"
        assert SMPPUDH.read(BytesIO(udh2.serialize())) == udh2

        assert len(udh2) == 1
        assert f_s(udh2) == "SMPPUDH(SMPPUDHElement(0xff, \"f\\x00o\"))"

        assert udh1 != udh2

        udh3 = SMPPUDH(SMPPUDHElement(0xff, b"f\x00o"))
        assert SMPPUDH.read(BytesIO(udh3.serialize())) == udh2

        udh4 = SMPPUDH(*((i, bytes([i])) for i in range(256)))
        assert len(udh4) == 256
        assert udh4[0] == SMPPUDHElement(0x00, b"\x00")

        assert SMPPUDH(*udh4[:10]).serialize() == \
        b"\x1e\x00\x01\x00\x01\x01\x01\x02\x01\x02\x03\x01\x03\x04\x01\x04\x05\x01\x05\x06\x01\x06\x07\x01\x07\x08\x01\x08\x09\x01\x09"

        with expected(InputParameterError):
            udh4.serialize()

        udh5 = SMPPUDH((0x01, b"\x01"), (0x01, b"\x01"))
        assert len(udh5) == 2
        assert udh5[0] == udh5[1]

        print("ok")

    test_SMPPUDH()

    ###################################

    print("all ok")

###############################################################################
# EOF