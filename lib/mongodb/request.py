#!/usr/bin/env python
#-*- coding: windows-1251 -*-
###############################################################################
#
# This module implements MongoDB request packet serialization.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "MongoDB_Request", "OP_UPDATE", "OP_INSERT", "OP_QUERY",
            "OP_GET_MORE", "OP_DELETE", "OP_KILL_CURSORS" ]

###############################################################################

import struct; from struct import pack

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional, list_of, tuple_of
import interlocked_counter; from interlocked_counter import InterlockedCounter
import mongodb.bson; from mongodb.bson import serialize_to_bytes, cstrify, BSON_Value

###############################################################################

class MongoDB_Request:

    _request_id = InterlockedCounter(2**32)

    def __init__(self):
        self._request_id = self._request_id.next()
        self._op_code = self._op_code

    def __str__(self):
        return "MongoDB_Request"

    def __repr__(self):
        return "<{0:s} # 0x{1:08x} @ 0x{2:08x}>".format(self, self._request_id, id(self))

    def __format__(self, spec):
        return format(str(self), spec)

    @staticmethod
    def _bit_flags(*args):
        return sum(2**i for i, arg in enumerate(args) if arg)

    @staticmethod
    def _dump_dict(d):
        return "{{{0:s}}}".format(", ".join("{0:s}: {1:s}".\
                    format(k, str(v) if isinstance(v, BSON_Value)
                              else MongoDB_Request._dump_dict(v) if isinstance(v, dict)
                              else repr(v)) for k, v in d.items()))

    def _with_flags(self, *ns):
        flags = ", ".join(n.upper() for n in ns if getattr(self, "_{0:s}".format(n)))
        return " WITH {0:s}".format(flags) if flags else ""

    request_id = property(lambda self: self._request_id)

    def serialize(self):
        packet = pack("<lll", self._request_id, 0, self._op_code) + self._serialize()
        return pack("<l", len(packet) + 4) + packet

###############################################################################

class OP_UPDATE(MongoDB_Request):

    _op_code = 2001

    @typecheck
    def __init__(self, collection: str, selector: dict, update: dict, *,
                 upsert: optional(bool) = False,
                 multi_update: optional(bool) = False):
        MongoDB_Request.__init__(self)
        self._collection = collection
        self._selector = selector
        self._update = update
        self._upsert = upsert
        self._multi_update = multi_update

    def __str__(self):
        return "OP_UPDATE {0:s} SET {1:s} WHERE {2:s}{3:s}".\
               format(self._collection, self._dump_dict(self._update),
                      self._dump_dict(self._selector),
                      self._with_flags("upsert", "multi_update"))

    def _flags(self):
        return self._bit_flags(self._upsert, self._multi_update)

    def _serialize(self):
        return pack("<l", 0) + \
               cstrify(self._collection) + \
               pack("<l", self._flags()) + \
               serialize_to_bytes(self._selector) + \
               serialize_to_bytes(self._update)

###############################################################################

class OP_INSERT(MongoDB_Request):

    _op_code = 2002

    @typecheck
    def __init__(self, collection: str, documents: list_of(dict)):
        MongoDB_Request.__init__(self)
        self._collection = collection
        self._documents = documents

    def __str__(self):
        return "OP_INSERT INTO {0:s} VALUES ({1:s})".format(
               self._collection, ", ".join(self._dump_dict(document)
                                           for document in self._documents))

    def _serialize(self):
        return pack("<l", 0) + \
               cstrify(self._collection) + \
               b"".join(serialize_to_bytes(document) for document in self._documents)

###############################################################################

class OP_QUERY(MongoDB_Request):

    _op_code = 2004

    @typecheck
    def __init__(self, collection: str, query: dict, fields: optional(tuple_of(str)) = None, *,
                 docs_to_skip: optional(int) = None,
                 docs_to_return: optional(int) = None,
                 tailable_cursor: optional(bool) = False,
                 slave_ok: optional(bool) = False,
                 no_cursor_timeout: optional(bool) = False,
                 await_data: optional(bool) = False,
                 exhaust: optional(bool) = False):
        MongoDB_Request.__init__(self)
        self._collection = collection
        self._query = query
        self._fields = fields
        self._docs_to_skip = docs_to_skip
        self._docs_to_return = docs_to_return
        self._tailable_cursor = tailable_cursor
        self._slave_ok = slave_ok
        self._no_cursor_timeout = no_cursor_timeout
        self._await_data = await_data
        self._exhaust = exhaust

    def __str__(self):
        return "OP_QUERY{0:s} FROM {1:s} WHERE {2:s}{3:s}{4:s}{5:s}".format(
               " *" if self._fields is None else " {0:s}".format(", ".join(self._fields)) if self._fields else "",
               self._collection, self._dump_dict(self._query),
               " SKIP {0:d}".format(self._docs_to_skip) if self._docs_to_skip is not None else "",
               " RETURN {0:d}".format(self._docs_to_return) if self._docs_to_return is not None else "",
               self._with_flags("tailable_cursor", "slave_ok", "no_cursor_timeout", "await_data", "exhaust"))

    def _flags(self):
        return self._bit_flags(False, self._tailable_cursor, self._slave_ok, False,
                               self._no_cursor_timeout, self._await_data, self._exhaust)

    def _serialize(self):
        return pack("<l", self._flags()) + \
               cstrify(self._collection) + \
               pack("<ll", self._docs_to_skip or 0, self._docs_to_return or -1) + \
               serialize_to_bytes(self._query) + \
               (serialize_to_bytes({ f: 1 for f in self._fields })
                if self._fields is not None else b"")

###############################################################################

class OP_GET_MORE(MongoDB_Request):

    _op_code = 2005

    @typecheck
    def __init__(self, collection: str, cursor_id: int, *,
                 docs_to_return: optional(int) = None):
        MongoDB_Request.__init__(self)
        self._collection = collection
        self._cursor_id = cursor_id
        self._docs_to_return = docs_to_return

    def __str__(self):
        return "OP_GET_MORE FROM {0:s} USING CURSOR 0x{1:016x}{2:s}".format(
               self._collection, self._cursor_id,
               " RETURN {0:d}".format(self._docs_to_return) if self._docs_to_return is not None else "")

    def _serialize(self):
        return pack("<l", 0) + \
               cstrify(self._collection) + \
               pack("<lq", self._docs_to_return or -1, self._cursor_id)

###############################################################################

class OP_DELETE(MongoDB_Request):

    _op_code = 2006

    @typecheck
    def __init__(self, collection: str, selector: dict, *,
                 single_remove: optional(bool) = False):
        MongoDB_Request.__init__(self)
        self._collection = collection
        self._selector = selector
        self._single_remove = single_remove

    def __str__(self):
        return "OP_DELETE FROM {0:s} WHERE {1:s}{2:s}".format(
               self._collection, self._dump_dict(self._selector),
               self._with_flags("single_remove"))

    def _flags(self):
        return self._bit_flags(self._single_remove)

    def _serialize(self):
        return pack("<l", 0) + \
               cstrify(self._collection) + \
               pack("<l", self._flags()) + \
               serialize_to_bytes(self._selector)

###############################################################################

class OP_KILL_CURSORS(MongoDB_Request):

    _op_code = 2007

    @typecheck
    def __init__(self, cursor_ids: list_of(int)):
        MongoDB_Request.__init__(self)
        self._cursor_ids = cursor_ids

    def __str__(self):
        return "OP_KILL_CURSORS{0:s}".format(
               " {0:s}".format(", ".join("0x{0:016x}".format(c)
                               for c in self._cursor_ids))
               if self._cursor_ids else "")

    def _serialize(self):
        return pack("<ll" + "q" * len(self._cursor_ids),
                    0, len(self._cursor_ids), *self._cursor_ids)

###############################################################################

if __name__ == "__main__":

    print("self-testing module mongodb/request.py:")

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

    print("utilities:", end = "")

    assert MongoDB_Request._bit_flags() == MongoDB_Request._bit_flags(False) == 0
    assert MongoDB_Request._bit_flags(True) == 1
    assert MongoDB_Request._bit_flags(False, False) == 0
    assert MongoDB_Request._bit_flags(True, False) == 1
    assert MongoDB_Request._bit_flags(False, True) == 2
    assert MongoDB_Request._bit_flags(True, True) == 3
    assert MongoDB_Request._bit_flags(*([False]*31 + [True])) == 2**31
    assert MongoDB_Request._bit_flags(*[True]*32) == 2**32-1

    print("ok")

    ###################################

    print("OP_UPDATE: ", end = "")

    op = OP_UPDATE("foo.bar", {}, {})
    assert f_s(op) == "OP_UPDATE foo.bar SET {} WHERE {}"
    assert op.serialize() == \
        b"\x2a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd1\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x00\x00\x00\x00" \
        b"\x05\x00\x00\x00\x00\x05\x00\x00\x00\x00"

    op = OP_UPDATE("foo.bar", { "v": "bar" }, { "k": "foo" }, upsert = True, multi_update = True)
    assert f_s(op) == "OP_UPDATE foo.bar SET {k: 'foo'} WHERE {v: 'bar'} WITH UPSERT, MULTI_UPDATE"
    assert op.serialize() == \
        b"\x40\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xd1\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x03\x00\x00\x00" \
        b"\x10\x00\x00\x00\x02v\x00\x04\x00\x00\x00bar\x00\x00\x10\x00\x00\x00\x02k\x00\x04\x00\x00\x00foo\x00\x00"

    print("ok")

    ###################################

    print("OP_INSERT: ", end = "")

    op = OP_INSERT("foo.bar", [{}, ])
    assert f_s(op) == "OP_INSERT INTO foo.bar VALUES ({})"
    assert op.serialize() == \
        b"\x21\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\xd2\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x05\x00\x00\x00\x00"

    print("ok")

    ###################################

    print("OP_QUERY: ", end = "")

    op = OP_QUERY("foo.bar", {})
    assert f_s(op) == "OP_QUERY * FROM foo.bar WHERE {}"
    assert op.serialize() == \
        b"\x29\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\xd4\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x00\x00\x00\x00\xff\xff\xff\xff\x05\x00\x00\x00\x00"

    op = OP_QUERY("foo.bar", {}, ())
    assert f_s(op) == "OP_QUERY FROM foo.bar WHERE {}"
    assert op.serialize() == \
        b"\x2e\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\xd4\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x00\x00\x00\x00\xff\xff\xff\xff\x05\x00\x00\x00\x00\x05\x00\x00\x00\x00"

    op = OP_QUERY("foo.bar", { "k": 123 }, ("v", "_id"), docs_to_skip = 3, docs_to_return = 4,
                  tailable_cursor = True, slave_ok = True, no_cursor_timeout = True, await_data = True, exhaust = True)
    assert f_s(op) == \
        "OP_QUERY v, _id FROM foo.bar WHERE {k: 123} SKIP 3 RETURN 4 " \
        "WITH TAILABLE_CURSOR, SLAVE_OK, NO_CURSOR_TIMEOUT, AWAIT_DATA, EXHAUST"
    assert op.serialize() == \
        b"\x45\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\xd4\x07\x00\x00" \
        b"v\x00\x00\x00foo.bar\x00\x03\x00\x00\x00\x04\x00\x00\x00\x0c\x00\x00\x00\x10k\x00{" \
        b"\x00\x00\x00\x00\x15\x00\x00\x00\x10_id\x00\x01\x00\x00\x00\x10v\x00\x01\x00\x00\x00\x00"

    print("ok")

    ###################################

    print("OP_GET_MORE: ", end = "")

    op = OP_GET_MORE("foo.bar", 0)
    assert f_s(op) == "OP_GET_MORE FROM foo.bar USING CURSOR 0x0000000000000000"
    assert op.serialize() == \
        b"\x28\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\xd5\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00"

    op = OP_GET_MORE("foo.bar", 0x1122334455667788, docs_to_return = 1)
    assert f_s(op) == "OP_GET_MORE FROM foo.bar USING CURSOR 0x1122334455667788 RETURN 1"
    assert op.serialize() == \
        b"\x28\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\xd5\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x01\x00\x00\x00\x88wfUD3\"\x11"

    print("ok")

    ###################################

    print("OP_DELETE: ", end = "")

    op = OP_DELETE("foo.bar", {})
    assert f_s(op) == "OP_DELETE FROM foo.bar WHERE {}"
    assert op.serialize() == \
        b"\x25\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\xd6\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00"

    op = OP_DELETE("foo.bar", { "k": "v" }, single_remove = True)
    assert f_s(op) == "OP_DELETE FROM foo.bar WHERE {k: 'v'} WITH SINGLE_REMOVE"
    assert op.serialize() == \
        b"\x2e\x00\x00\x00\t\x00\x00\x00\x00\x00\x00\x00\xd6\x07\x00\x00" \
        b"\x00\x00\x00\x00foo.bar\x00\x01\x00\x00\x00\x0e\x00\x00\x00\x02k\x00\x02\x00\x00\x00v\x00\x00"

    print("ok")

    ###################################

    print("OP_KILL_CURSORS: ", end = "")

    op = OP_KILL_CURSORS([])
    assert f_s(op) == "OP_KILL_CURSORS"
    assert op.serialize() == \
        b"\x18\x00\x00\x00\n\x00\x00\x00\x00\x00\x00\x00\xd7\x07\x00\x00" \
        b"\x00\x00\x00\x00\x00\x00\x00\x00"

    op = OP_KILL_CURSORS([0, 0x7fffffffffffffff])
    assert f_s(op) == "OP_KILL_CURSORS 0x0000000000000000, 0x7fffffffffffffff"

    assert op.serialize() == \
        b"\x28\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\xd7\x07\x00\x00" \
        b"\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\x7f"

    print("ok")

    ###################################

    print("all ok")

###############################################################################
# EOF