#!/usr/bin/env python
#-*- coding: windows-1251 -*-
###############################################################################
#
# This module implements MongoDB response packet parsing.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "MongoDB_Response", "OP_REPLY" ]

###############################################################################

import struct; from struct import unpack

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import mongodb.bson; from mongodb.bson import parse_stream, \
            BSON_Value, BSON_ParsingError, BSON_Document

###############################################################################

class MongoDB_Response:

    def __init__(self, request_id, response_id):
        self._request_id = request_id
        self._response_id = response_id

    response_id = property(lambda self: self._response_id)

    def __str__(self):
        return "MongoDB_Response"

    def __repr__(self):
        return "<{0:s} # 0x{1:08x} @ 0x{2:08x}>".\
               format(self, self._response_id, id(self))

    def __format__(self, spec):
        return format(str(self), spec)

    @classmethod
    def parse_stream(cls, stream):
        message_length, request_id, response_id, op_code = \
            unpack("<llll", BSON_Value._read(stream, 16))
        if message_length < 16:
            raise BSON_ParsingError("incorrect message length") # tested
        op = cls._ops.get(op_code)
        if not op:
            raise BSON_ParsingError("unknown opcode 0x{0:08x}".format(op_code)) # tested
        op = op(request_id, response_id)
        op._parse_stream(stream)
        return op

    _ops = {}

    @classmethod
    def register_op(cls, op):
        cls._ops[op._op_code] = op

###############################################################################

class OP_REPLY(MongoDB_Response):

    _op_code = 1

    def __str__(self):
        return "OP_REPLY{0:s}{1:s}{2:s}{3:s}{4:s}{5:s}".\
               format(" FAILURE" if self.query_failure else "",
                      " CURSOR NOT FOUND" if self.cursor_not_found else "",
                      " RETURNS {0:d}".format(len(self._documents)) if self._documents else "",
                      " USING CURSOR 0x{0:016x}".format(self._cursor_id) if self._cursor_id > 0 else "",
                      " AWAIT CAPABLE" if self.await_capable else "",
                      " STARTING FROM {0:d}".format(self._starting_from) if self._starting_from > 0 else "")

    def _parse_stream(self, stream):
        self._flags, self._cursor_id, self._starting_from, number_returned = \
            unpack("<lqll", BSON_Value._read(stream, 20))
        self._documents = [ parse_stream(stream) for i in range(number_returned) ]

    cursor_id = property(lambda self: self._cursor_id)
    starting_from = property(lambda self: self._starting_from)
    documents = property(lambda self: self._documents)

    cursor_not_found = property(lambda self: self._flags & 0b0001 != 0)
    query_failure = property(lambda self: self._flags & 0b0010 != 0)
    await_capable = property(lambda self: self._flags & 0b1000 != 0)

MongoDB_Response.register_op(OP_REPLY)

###############################################################################

if __name__ == "__main__":

    print("self-testing module mongodb/response.py:")

    from expected import expected
    from io import BytesIO
    from struct import pack
    from mongodb.bson import serialize_to_bytes
    from typecheck import by_regex

    ###################################

    def f_s(v):
        s = str(v)
        assert "{0}".format(v) == "{0:s}".format(v) == s
        repr_regex = "^<{0:s} # 0x[0-9a-f]{{8}} @ 0x{1:08x}>".\
                     format("".join("[{0:s}]".format(c) for c in s), id(v))
        valid_repr = by_regex(repr_regex)
        assert valid_repr(repr(v))
        return s

    ###################################

    print("OP_REPLY: ", end = "")

    rs_s = BytesIO(pack("<lllllqll", 36, 7, 8, 1, 0b1011, 0x0123456789ABCDEF, 100, 0))
    rs = MongoDB_Response.parse_stream(rs_s)

    assert isinstance(rs, OP_REPLY)
    assert rs.response_id == 8
    assert rs.cursor_id == 0x0123456789ABCDEF
    assert rs.cursor_not_found is True
    assert rs.query_failure is True
    assert rs.await_capable is True
    assert rs.documents == []

    assert f_s(rs) == "OP_REPLY FAILURE CURSOR NOT FOUND USING CURSOR 0x0123456789abcdef AWAIT CAPABLE STARTING FROM 100"

    rs_d = serialize_to_bytes({ "foo": "bar" })
    rs_s = BytesIO(pack("<lllllqll", 36 + len(rs_d), 8, 7, 1, 0b0000, 0, 0, 1) + rs_d)
    rs = MongoDB_Response.parse_stream(rs_s)

    assert isinstance(rs, OP_REPLY)
    assert rs.response_id == 7
    assert rs.cursor_id == 0
    assert rs.cursor_not_found is False
    assert rs.query_failure is False
    assert rs.await_capable is False
    assert rs.documents == [ { "foo": "bar" } ]

    assert f_s(rs) == "OP_REPLY RETURNS 1"

    print("ok")

    ###################################

    print("parsing errors: ", end = "")

    with expected(BSON_ParsingError("unexpected end of stream")):
        MongoDB_Response.parse_stream(BytesIO(b""))

    with expected(BSON_ParsingError("incorrect message length")):
        MongoDB_Response.parse_stream(BytesIO(pack("<llll", 15, 0, 0, 0)))

    with expected(BSON_ParsingError("unknown opcode 0x12345678")):
        MongoDB_Response.parse_stream(BytesIO(pack("<llll", 16, 0, 0, 0x12345678)))

    print("ok")

    ###################################

    print("all ok")

###############################################################################
# EOF
