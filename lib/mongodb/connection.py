#!/usr/bin/env python
#-*- coding: windows-1251 -*-
###############################################################################
#
# This module implements MongoDB connection with authentication and timeouts.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "MongoDB_Connection", "MongoDB_Error" ]

###############################################################################

import time; from time import time
import socket; from socket import socket, AF_INET, SOCK_STREAM, timeout
import select; from select import select
import hashlib; from hashlib import md5
import collections; from collections import OrderedDict as odict
import io; from io import BytesIO

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional
import pmnc.timeout; from pmnc.timeout import Timeout
import mongodb.request; from mongodb.request import MongoDB_Request, OP_QUERY
import mongodb.response; from mongodb.response import MongoDB_Response
import mongodb.bson; from mongodb.bson import *

###############################################################################

class MongoDB_Error(Exception):
    def __init__(self, message, code = None):
        Exception.__init__(self, message)
        self._code = code
    code = property(lambda self: self._code)

###############################################################################

class MongoDB_Connection:

    @typecheck
    def __init__(self, *,
                 server_address: (str, int),
                 auth_database: str,
                 username: optional(str) = None,
                 password: optional(str) = None):

        self._server_address = server_address
        self._auth_database = auth_database
        self._username = username or ""
        self._password = password or ""

        self._read_offset = 0
        self._read_buffer = BytesIO()
        self._read_ahead = 16384

    ###################################

    def connect(self, t: Timeout):
        self._s = socket(AF_INET, SOCK_STREAM)
        try:
            self._s.settimeout(t.remain or 0.01)
            self._s.connect(self._server_address)
            if self._username and self._password:
                self._authenticate(t)
        except:
            self._s.close()
            raise

    def disconnect(self):
        self._s.close()

    ###################################

    def _authenticate(self, t: Timeout):
        nonce = self._command("getnonce", {}, t).documents[0]["nonce"]
        username_b = self._username.encode("utf-8")
        password_b = self._password.encode("utf-8")
        pwd = md5(username_b + b":mongo:" + password_b).hexdigest()
        pwd_digest = md5(nonce.encode("utf-8") + username_b + pwd.encode("ascii")).hexdigest()
        self._command("authenticate", dict(user = self._username, nonce = nonce, key = pwd_digest), t)

    def _command(self, command: str, params: dict, t: Timeout):
        query = odict({ command: 1 })
        if params: query.update(params)
        rq = OP_QUERY("{0:s}.$cmd".format(self._auth_database), query)
        rs = self.sync_request(rq, t)
        self._command_check(rs)
        return rs

    @staticmethod
    def _command_check(rs):
        if len(rs.documents) != 1:
            raise MongoDB_Error("incorrect response to command")
        if not rs.documents[0].get("ok"):
            raise MongoDB_Error(rs.documents[0].get("errmsg", "undefined error"))

    ###################################

    @typecheck
    def async_request(self, rq: MongoDB_Request, t: Timeout):
        self._send_request(rq, t)

    @typecheck
    def sync_request(self, rq: MongoDB_Request, t: Timeout) -> MongoDB_Response:
        self._send_request(rq, t)
        rs = self._read_response(t)
        if rs.response_id != rq.request_id:
            raise MongoDB_Error("mismatched response")
        return rs

    ###################################
    # timeout-aware send/recv pair

    def _send_request(self, rq: MongoDB_Request, t: Timeout):
        b = rq.serialize()
        while not t.expired:
            if select([], [ self._s ], [], t.remain)[1]:
                n = self._s.send(b)
                if n == len(b):
                    return
                b = b[n:]
        raise timeout("timed out")

    def _read_response(self, t: Timeout) -> MongoDB_Response:
        self._read_timeout = t
        try:
            rs = MongoDB_Response.parse_stream(self)
            if rs.query_failure:
                d = rs.documents[0]
                raise MongoDB_Error(d["$err"], d.get("code"))
            return rs
        finally:
            del self._read_timeout

    ###################################
    # buffering stream reading methods

    @typecheck
    def read(self, n: int) -> bytes:
        b = self._read_buffer.read(n)
        if len(b) == n:
            self._read_offset += n
            return b
        while not self._read_timeout.expired:
            if select([ self._s ], [], [], self._read_timeout.remain)[0]:
                trd = n - len(b)
                rd = self._s.recv(trd + self._read_ahead)
                b += rd[:trd]
                if len(rd) >= trd:
                    self._read_buffer = BytesIO(rd[trd:])
                    self._read_offset += n
                    return b
        raise timeout("timed out")

    @typecheck
    def tell(self) -> int:
        return self._read_offset

###############################################################################

if __name__ == "__main__":

    print("self-testing module mongodb/connection.py:")

    from expected import expected
    from mongodb.request import OP_DELETE, OP_INSERT

    c = MongoDB_Connection(server_address = ("1.2.3.4", 5678), auth_database = "test")
    with expected(timeout("timed out")):
        c.connect(Timeout(0.1))

    c = MongoDB_Connection(server_address = ("127.0.0.1", 27017), auth_database = "test",
                           username = "user", password = "wrong")
    try:
        c.connect(Timeout(3.0))
    except MongoDB_Error as e:
        assert "auth" in str(e)
        assert e.code is None
    else:
        assert False

    c = MongoDB_Connection(server_address = ("127.0.0.1", 27017), auth_database = "test",
                           username = "user", password = "pass")
    c.connect(Timeout(3.0))
    try:

        rq = OP_DELETE("test.foo", {})
        c.async_request(rq, Timeout(3.0))

        rq = OP_INSERT("test.foo", [ { "bar": i } for i in range(10) ])
        c.async_request(rq, Timeout(3.0))

        rq = OP_QUERY("test.foo", { "bar": 3 })
        rs = c.sync_request(rq, Timeout(3.0))

        assert len(rs.documents) == 1 and rs.documents[0]["bar"] == 3

    finally:
        c.disconnect()

    print("ok")

###############################################################################
# EOF
