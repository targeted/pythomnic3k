#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
#################################################################################
#
# This module implements resource to access MongoDB databases.
#
# Sample configuration (config_resource_mongodb_1.py)
#
# config = dict \
# (
# protocol = "mongodb",                       # meta
# server_address = ("db.domain.com", 27017),  # mongodb
# connect_timeout = 3.0,                      # mongodb
# database = "db",                            # mongodb
# username = None,                            # mongodb, optional str
# password = None,                            # mongodb, optional str
# )
#
# Sample usage (anywhere):
#
# 1. Insert data to collection db.foo.bar:
#
# xa = pmnc.transaction.create()
# xa.mongodb_1.foo.bar.insert([ { "k": 1 }, ... ])
# xa.execute()
#
# or if the only transaction participant (similarly for other examples):
#
# pmnc.transaction.mongodb_1.foo.bar.insert([ { "k": 1 }, ... ])
#
# 2. Query collection db.foo.bar:
#
# xa = pmnc.transaction.create()
# xa.mongodb_1.foo.bar.find({ "k": { "$gt": 1 } }, docs_to_return = 100)
# rs = xa.execute()[0]
# for d in rs.documents:
#     assert d["k"] > 1
#
# 3. Fetch data using cursor from collection db.foo.bar:
#
# docs = []
#
# xa = pmnc.transaction.create()
# xa.mongodb_1.foo.bar.find(docs_to_return = 10)
# rs = xa.execute()[0]
# docs.extend(rs.documents)
#
# while rs.cursor_id:
#     xa = pmnc.transaction.create()
#     xa.mongodb_1.foo.bar.get_more(rs.cursor_id, docs_to_return = 10)
#     rs = xa.execute()[0]
#     docs.extend(rs.documents)
#
# for d in docs:
#     ...
#
# 4. Delete data from collection db.foo.bar:
#
# xa = pmnc.transaction.create()
# xa.mongodb_1.foo.bar.remove({ "k": 1 })
# xa.execute()
#
# 5. Drop collection db.foo.bar:
#
# xa = pmnc.transaction.create()
# xa.mongodb_1.foo.bar.drop()
# xa.execute()
#
# 6. Send command to database db:
#
# xa = pmnc.transaction.create()
# xa.mongodb_1.command("collStats", { "scale": 1024 }, collStats = "foo.bar")
# rs = xa.execute()[0]
# docs_count = rs.documents[0]["count"]
#
# 7. Use map/reduce on collection db.foo.bar:
#
# xa = pmnc.transaction.create()
# xa.mongodb_1.foo.bar.map_reduce({ "map": BSON_JavaScript("function () { emit(k, 1); }"),
#                                   "reduce": BSON_JavaScript("function (k, vs) { return 1; }") })
# rs = xa.execute()[0]
# for d in rs.documents:
#     print(d["_id"], d["value"])
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
#################################################################################

__all__ = [ "Resource" ]

###############################################################################

import collections; from collections import OrderedDict as odict
import os; from os import urandom
import binascii; from binascii import b2a_hex

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, optional, list_of, by_regex, either, callable
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, ResourceError
import mongodb.connection; from mongodb.connection import *
import mongodb.request; from mongodb.request import *
import mongodb.response; from mongodb.response import *
import mongodb.bson; from mongodb.bson import *

###############################################################################

valid_database = by_regex("^[A-Za-z0-9_]+$")
valid_collection = by_regex("^[A-Za-z0-9_]+(?:\\.[A-Za-z0-9_]+)*$")
empty_string = by_regex("^$")

###############################################################################

class Resource(TransactionalResource): # MongoDB resource

    @typecheck
    def __init__(self, name: str, *,
                 server_address: (str, int),
                 connect_timeout: float,
                 database: valid_database,
                 username: optional(str) = None,
                 password: optional(str) = None):

        TransactionalResource.__init__(self, name)
        self._server_address = server_address
        self._connect_timeout = connect_timeout
        self._database = database
        self._username = username
        self._connection = MongoDB_Connection(server_address = server_address,
            auth_database = database, username = username, password = password)

    ###################################

    def connect(self):
        TransactionalResource.connect(self)
        connect_timeout = Timeout(min(self._connect_timeout, pmnc.request.remain))
        self._connection.connect(connect_timeout)
        self._attrs = []

    def disconnect(self):
        try:
            self._connection.disconnect()
        finally:
            TransactionalResource.disconnect(self)

    ###################################

    def __getattr__(self, name):
        self._attrs.append(name)
        return self

    def __call__(self, *args, **kwargs):
        attrs, self._attrs = self._attrs, []
        return self._commands[attrs[-1]](self, ".".join(attrs[:-1]), *args, **kwargs)

    ###################################

    def _async_request(self, rq: MongoDB_Request):
        pmnc.log.info(">> {0:s}".format(rq))
        try:
            self._connection.async_request(rq, Timeout(pmnc.request.remain))
            gle_rq = OP_QUERY("{0:s}.$cmd".format(self._database), { "getlasterror": 1 })
            gle_rs = self._connection.sync_request(gle_rq, Timeout(pmnc.request.remain))
            MongoDB_Connection._command_check(gle_rs)
            d = gle_rs.documents[0]
            err = d.get("err")
            if err: raise MongoDB_Error(err, d.get("code"))
        except MongoDB_Error as e:
            pmnc.log.warning("<< {0:s} !! {1:s}".format(rq, exc_string()))
            ResourceError.rethrow(code = e.code, description = str(e),
                                  terminal = e.code is not None)
        except Exception as e:
            pmnc.log.warning("<< {0:s} !! {1:s}".format(rq, exc_string()))
            ResourceError.rethrow(description = str(e))
        else:
            pmnc.log.info("<< OK")

    def _sync_request(self, rq: MongoDB_Request, check: optional(callable) = None) -> MongoDB_Response:
        pmnc.log.info(">> {0:s}".format(rq))
        try:
            rs = self._connection.sync_request(rq, Timeout(pmnc.request.remain))
            if rs.cursor_not_found:
                raise MongoDB_Error("cursor not found")
            if check: check(rs)
        except MongoDB_Error as e:
            pmnc.log.warning("<< {0:s} !! {1:s}".format(rq, exc_string()))
            ResourceError.rethrow(code = e.code, description = str(e),
                                  terminal = e.code is not None)
        except Exception as e:
            pmnc.log.warning("<< {0:s} !! {1:s}".format(rq, exc_string()))
            ResourceError.rethrow(description = str(e))
        else:
            pmnc.log.info("<< OK, {0:s}".format(rs))
            return rs

    ###################################

    _commands = {}

    ###################################

    @typecheck
    def _insert(self, collection: valid_collection, documents: list_of(dict)):
        collection = "{0:s}.{1:s}".format(self._database, collection)
        self._async_request(OP_INSERT(collection, documents))

    _commands["insert"] = _insert

    ###################################

    @typecheck
    def _find(self, collection: valid_collection, query: optional(dict) = {}, **kwargs) -> MongoDB_Response:
        collection = "{0:s}.{1:s}".format(self._database, collection)
        return self._sync_request(OP_QUERY(collection, query, **kwargs))

    _commands["find"] = _find

    ###################################

    @typecheck
    def _remove(self, collection: valid_collection, selector: optional(dict) = {}, **kwargs):
        collection = "{0:s}.{1:s}".format(self._database, collection)
        self._async_request(OP_DELETE(collection, selector, **kwargs))

    _commands["remove"] = _remove

    ###################################

    @typecheck
    def _get_more(self, collection: valid_collection, cursor_id: int, **kwargs) -> MongoDB_Response:
        collection = "{0:s}.{1:s}".format(self._database, collection)
        return self._sync_request(OP_GET_MORE(collection, cursor_id, **kwargs))

    _commands["get_more"] = _get_more

    ###################################

    @typecheck
    def _kill_cursors(self, collection: empty_string, cursor_ids: list_of(int), **kwargs):
        self._async_request(OP_KILL_CURSORS(cursor_ids, **kwargs))

    _commands["kill_cursors"] = _kill_cursors

    ###################################

    @typecheck
    def _command(self, collection: either(valid_collection, empty_string),
                 command: str, params: optional(dict) = {}, **kwargs) -> MongoDB_Response:
        cmd = "{0:s}.$cmd".format(self._database)
        query = odict({ command: kwargs.pop(command, 1) })
        query.update(params)
        rq = OP_QUERY(cmd, query, **kwargs)
        return self._sync_request(rq, MongoDB_Connection._command_check)

    _commands["command"] = _command

    ###################################

    @typecheck
    def _drop(self, collection: valid_collection) -> MongoDB_Response:
        return self._command("", "drop", {}, drop = collection)

    _commands["drop"] = _drop

    ###################################

    @typecheck
    def _map_reduce(self, collection: valid_collection, params: dict, **kwargs) -> MongoDB_Response:
        params_ = odict({ "mapreduce": collection })
        try:
            params_.update(map = params.pop("map"))
            params_.update(reduce = params.pop("reduce"))
        except KeyError:
            ResourceError.rethrow(description = "map and reduce functions must be specified")
        query = params.pop("having", {})
        params_.update(params)
        out = "{0:s}.mr_{1:s}".format(collection, b2a_hex(urandom(8)).decode("ascii"))
        params_["out"] = out
        rs = self._command("", "mapreduce", params_, **kwargs)
        try:
            counts = rs.documents[0]["counts"]
            if pmnc.log.debug:
                pmnc.log.debug("-- MAP/REDUCE: IN={0[input]:d}, EMIT={0[emit]:d}, "
                               "OUT={0[output]:d}".format(counts))
            return self._find(out, query, docs_to_return = -counts["output"], exhaust = True)
        finally:
            self._drop(out)

    _commands["map_reduce"] = _map_reduce

###############################################################################

def self_test():

    from pmnc.request import fake_request
    from expected import expected

    ###################################

    def test_populate():

        fake_request(10.0)

        pmnc.transaction.mongodb_1.test.remove()
        pmnc.transaction.mongodb_1.test.insert([ { "k": i } for i in range(100) ])

    test_populate()

    ###################################

    def test_query_cursor():

        fake_request(10.0)

        rs = pmnc.transaction.mongodb_1.test.find()
        assert len(rs.documents) == 1 and rs.cursor_id == 0

        rs = pmnc.transaction.mongodb_1.test.find(docs_to_return = 50)
        assert len(rs.documents) == 50 and rs.cursor_id != 0

        xa = pmnc.transaction.create()
        xa.mongodb_1.test.get_more(rs.cursor_id, docs_to_return = 25)
        xa.mongodb_1.test.get_more(rs.cursor_id, docs_to_return = 25)
        rs1, rs2 = xa.execute()
        assert len(rs1.documents) == 25 and len(rs2.documents) == 25

        ds = [ d["k"] for d in (rs.documents + rs1.documents + rs2.documents) ]
        ds.sort()
        assert ds == list(range(100))

    test_query_cursor()

    ###################################

    def test_close_cursor():

        fake_request(10.0)

        rs = pmnc.transaction.mongodb_1.test.find(docs_to_return = 2)
        assert len(rs.documents) == 2 and rs.cursor_id != 0

        pmnc.transaction.mongodb_1.kill_cursors([rs.cursor_id])

        with expected(ResourceError, "cursor not found"):
            pmnc.transaction.mongodb_1.test.get_more(rs.cursor_id)

    test_close_cursor()

    ###################################

    def test_complex_search():

        fake_request(10.0)

        rs = pmnc.transaction.mongodb_1.test.find({ "$query": { "k": { "$gt": 90 } } }, docs_to_return = -10)
        assert len(rs.documents) == 9 and rs.cursor_id == 0

        rs = pmnc.transaction.mongodb_1.test.find({ "$where": BSON_JavaScript("function() { return this.k % 3 == 0; }") }, docs_to_return = -100)
        ds = [ d["k"] for d in rs.documents ]; ds.sort()
        assert ds == list(range(0, 100, 3))

    test_complex_search()

    ###################################

    def test_command():

        fake_request(10.0)

        with expected(ResourceError, "no such cmd.*"):
            pmnc.transaction.mongodb_1.command("no-such-cmd")

        # implicit { command: 1 }

        pmnc.transaction.mongodb_1.command("ping")

        # explicit { command: value }

        rs = pmnc.transaction.mongodb_1.command("isMaster", isMaster = 1.0)
        assert rs.documents[0]["ismaster"] in (True, False)

        # with extra params

        rs = pmnc.transaction.mongodb_1.command("collStats", { "scale": 1024 }, collStats = "test")
        assert rs.documents[0]["count"] == 100

        rs = pmnc.transaction.mongodb_1.command("distinct", { "key": "k", "query": {} }, distinct = "test")
        vs = rs.documents[0]["values"]
        vs.sort()
        assert vs == list(range(100))

    test_command()

    ###################################

    def test_map_reduce():

        fake_request(10.0)

        rs = pmnc.transaction.\
            mongodb_1.test.map_reduce({ "map": BSON_JavaScript("function () { for (var i = 2; i < this.k; i++) { emit(this.k, this.k % i); } }"),
                                        "reduce": BSON_JavaScript("function (k, vs) { for (var i = 0; i < vs.length; i++) if (vs[i] == 0) return 0; return 1; }"),
                                        "having": { "value": 1.0 } })

        primes = [ int(d["_id"]) for d in rs.documents ]
        assert primes == [3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97]

    test_map_reduce()

    ###################################

    def test_getlasterror():

        fake_request(10.0)

        try:
            pmnc.transaction.mongodb_1.capped.drop()
        except ResourceError as e:
            if e.description == "ns not found":
                pass
            else:
                raise

        pmnc.transaction.mongodb_1.command("create", { "capped": True, "size": 10 }, create = "capped")

        with expected(ResourceError, "10101.*remove from a capped collection.*"):
            pmnc.transaction.mongodb_1.capped.remove({ "foo": "bar" })

    test_getlasterror()

    ###################################

    def test_drop():

        fake_request(10.0)

        pmnc.transaction.mongodb_1.test.drop()

        with expected(ResourceError, ".*not found.*"):
            pmnc.transaction.mongodb_1.command("collStats", {}, collStats = "test")

    test_drop()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
