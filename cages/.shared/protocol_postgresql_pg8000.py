#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
################################################################################
#
# This module implements resource to access PostgreSQL databases,
# uses pg8000 library (version 1.07) written in native Python.
#
# Sample configuration (config_resource_postgresql_1.py)
#
# config = dict \
# (
# protocol = "postgresql_pg8000",             # meta
# decimal_precision = (10, 2),                # sql
# server_address = ("db.domain.com", 5432),   # postgresql
# connect_timeout = 3.0,                      # postgresql
# database = "database",                      # postgresql
# username = "user",                          # postgresql
# password = "pass",                          # postgresql
# server_encoding = None,                     # postgresql, optional str
# )
#
# Sample usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.postgresql_1.execute("INSERT INTO t (id, name) VALUES ({id}, {name})", # query 1
#                         "SELECT name FROM t WHERE id = {id}",             # query 2
#                         id = 123, name = "foo")                           # parameters
# insert_records, select_records = xa.execute()[0]
# assert insert_records == []
# for record in select_records:
#     print(record["name"])
#
# or if the only transaction participant:
#
# insert_records, select_records = pmnc.transaction.postgresql_1.execute(...)
#
# Pythomnic3k project
# (c) 2005-2015, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

import datetime; from datetime import datetime, timedelta
import os; from os import urandom
import binascii; from binascii import b2a_hex
import pg8000; from pg8000 import DBAPI, ProgrammingError; DBAPI.paramstyle = "qmark"
import pg8000.types; from pg8000.types import Bytea, Interval, pg_to_py_encodings

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string, trace_string
import typecheck; from typecheck import typecheck, optional
import pmnc.resource_pool; from pmnc.resource_pool import SQLResource, \
                                ResourceError, SQLResourceError

###############################################################################

class Resource(SQLResource): # PostgreSQL resource

    @typecheck
    def __init__(self, name: str, *,
                 decimal_precision: (int, int),
                 server_address: (str, int),
                 connect_timeout: float,
                 database: str,
                 username: str,
                 password: str,
                 server_encoding: optional(str) = None):

        SQLResource.__init__(self, name, decimal_precision = decimal_precision)

        self._server_address = server_address
        self._connect_timeout = connect_timeout
        self._database = database
        self._username = username
        self._password = password
        self._server_encoding = server_encoding

        self._random_separator = "|{0:s}|".format(b2a_hex(urandom(8)).decode("ascii"))

    ###################################

    def connect(self):
        SQLResource.connect(self)
        self._connection = \
            DBAPI.connect(host = self._server_address[0],
                          port = self._server_address[1],
                          socket_timeout = self._connect_timeout,
                          database = self._database,
                          user = self._username,
                          password = self._password)
        try:
            self._cursor = self._connection.cursor()
            if not self._server_encoding:
                self._cursor.execute("SHOW SERVER_ENCODING")
                server_encoding = self._cursor.fetchall()[0][0].lower()
                self._server_encoding = \
                    pg_to_py_encodings.get(server_encoding, server_encoding)
        except:
            self._connection.close()
            raise

    ###################################

    # WARNING: pg8000 begins transactions implicitly,
    # which results in the following strangeness

    # this is in fact (rollback+begin) pair used to detect
    # possible connection problems after idle timeout

    def begin_transaction(self, *args, **kwargs):
        SQLResource.begin_transaction(self, *args, **kwargs)
        self._connection.rollback()

    ###################################

    def _execute_sql(self, sql, params):

        try:
            sql, params = self._convert_to_qmarks(sql, params)
            param_list = ", ".join(map(lambda x: isinstance(x, str)
                                                 and "'{0:s}'".format(x)
                                                 or str(x), params))
        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

        pmnc.log.info(">> {0:s}".format(sql))
        if param_list:
            if pmnc.log.debug:
                pmnc.log.debug("-- {0:s} -- ({1:s})".format(sql, param_list))

        records = []
        try:

            self._cursor.execute(sql, params)

            rowcount = self._cursor.rowcount
            if rowcount >= 0:
                pmnc.log.info("<< OK, {0:d} record(s)".format(rowcount))
                if self._cursor.description and rowcount > 0:
                    column_names = [ t[0].decode("ascii") for t in self._cursor.description ]
                    for record in self._cursor.fetchall():
                        records.append(dict(zip(column_names, record)))
            else:
                pmnc.log.info("<< OK")

        except ProgrammingError as e:
            try:
                level, state, message = map(self._decode_message, e.args)
            except:
                state, message = "PG800", str(e)
            state = state.upper()
            pmnc.log.warning("<< {0:s}{1:s} !! PostgreSQL_Error(\"[{2:s}] {3:s}\") in {4:s}".\
                             format(sql, " -- ({0:s})".format(param_list)
                                    if param_list else "", state, message, trace_string()))
            SQLResourceError.rethrow(
                    state = state, description = message, # note that there is no code
                    recoverable = True, terminal = state[:2] not in self._safe_states)
        except:
            pmnc.log.warning("<< {0:s}{1:s} !! {2:s}".\
                             format(sql, " -- ({0:s})".format(param_list)
                                    if param_list else "", exc_string()))
            ResourceError.rethrow(recoverable = True)
        else:
            return records

    ###################################

    # errors with the following sql state classes, will not cause
    # connection termination, this may not be an exhaustive list
    # of errors after which it is safe to keep the connection,
    # but still hopefully an optimization

    _safe_states = {
                       "00", # success
                       "01", # warning
                       "02", # no data
                       "21", # cardinality violation
                       "22", # data exception
                       "23", # integrity constraint violation
                       "42", # syntax error or access rule violation
                       "44", # with check option violation
                       "P0", # pl/pgsql error
                   }

    ###################################

    # this is in fact (commit+begin) pair and there is
    # no way to tell begin failure from commit failure

    def commit(self):
        self._connection.commit()

    ###################################

    # this is in fact (rollback+begin) pair and there is
    # no way to tell begin failure from rollback failure

    def rollback(self):
        self._connection.rollback()

    ###################################

    def disconnect(self):
        try:
            try:
                self._cursor.close()
            finally:
                self._connection.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            SQLResource.disconnect(self)

    ###################################

    # this method takes "SELECT {foo} FROM {bar}", { "foo": 1, "bar": 2 }
    # and returns "SELECT ? FROM ?", (1, 2)

    def _convert_to_qmarks(self, sql, params):

        params = tuple(params.items()) # this fixes the order

        n2i = { n: "{0:s}{1:d}{0:s}".format(self._random_separator, i)
                for i, (n, v) in enumerate(params) }
        split_sql = sql.format(**n2i).split(self._random_separator)

        return "?".join(split_sql[::2]), tuple(params[int(s)][1] for s in split_sql[1::2])

    ###################################

    _supported_types = SQLResource._supported_types | { float, timedelta, list }

    def _py_to_sql_bytes(self, v):
        return Bytea(SQLResource._py_to_sql_bytes(self, v))

    def _py_to_sql_Decimal(self, v):
        if v == 0:
            return SQLResource._py_to_sql_int(self, 0) # pg8000 fails to marshal Decimal zero
        else:
            return SQLResource._py_to_sql_Decimal(self, v)

    def _py_to_sql_float(self, v):
        return v

    def _py_to_sql_timedelta(self, v):
        return Interval(days = v.days, microseconds = (v.seconds * 1000000) + v.microseconds)

    def _py_to_sql_Interval(self, v):
        return v

    def _py_to_sql_list(self, el):
        return [ self._py_to_sql(v) for v in el ]

    ###################################

    def _sql_to_py_Bytea(self, v):
        return bytes(v)

    def _sql_to_py_date(self, v):
        return datetime(v.year, v.month, v.day)

    def _sql_to_py_float(self, v):
        return v

    def _sql_to_py_timedelta(self, v):
        return v

    def _sql_to_py_Interval(self, v):
        if not v.months:
            return timedelta(days = v.days,
                             seconds = v.microseconds // 1000000,
                             microseconds = v.microseconds % 1000000)
        else:
            raise ValueError("month intervals cannot be converted")

    def _sql_to_py_list(self, el):
        pel = []
        same = True
        for v in el:
            pv = self._sql_to_py(v)
            pel.append(pv)
            same = same and pv is v
        return el if same else pel

    ###################################

    def _decode_message(self, b):
        try:
            return b.decode(self._server_encoding)
        except UnicodeDecodeError:
            return "?"

###############################################################################

def self_test():

    from expected import expected
    from pmnc.timeout import Timeout
    from random import randint
    from decimal import Decimal
    from time import time
    from threading import Thread
    from pmnc.request import fake_request
    from interlocked_counter import InterlockedCounter

    ###################################

    rus = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"

    random_string = lambda n: b2a_hex(urandom(n))[:n].decode("ascii")

    ###################################

    def test_qmarks():

        db_config = pmnc.config_resource_postgresql_1.copy()
        assert db_config.pop("protocol") == "postgresql_pg8000"
        r = Resource("test", **db_config)

        assert r._convert_to_qmarks("", {}) == ("", ())
        assert r._convert_to_qmarks("{i}", dict(i = 123)) == ("?", (123,))
        assert r._convert_to_qmarks("{{i}}{i}{{i}}", dict(i = 123)) == ("{i}?{i}", (123,))
        assert r._convert_to_qmarks("{i}{i}", dict(i = 123)) == ("??", (123, 123))
        assert r._convert_to_qmarks("{i}{{i}}{i}", dict(i = 123)) == ("?{i}?", (123, 123))
        assert r._convert_to_qmarks("{i}{{i{i}i}}{i}", dict(i = 123)) == ("?{i?i}?", (123, 123, 123))

        assert r._convert_to_qmarks("", dict(i = 123, s = "foo")) == ("", ())
        assert r._convert_to_qmarks("{i}", dict(i = 123, s = "foo")) == ("?", (123,))
        assert r._convert_to_qmarks("{s}", dict(i = 123, s = "foo")) == ("?", ("foo",))
        assert r._convert_to_qmarks("{i}{s}", dict(i = 123, s = "foo")) == ("??", (123, "foo"))
        assert r._convert_to_qmarks("{s}{i}", dict(i = 123, s = "foo")) == ("??", ("foo", 123))
        assert r._convert_to_qmarks("{i}{s}{i}", dict(i = 123, s = "foo")) == ("???", (123, "foo", 123))
        assert r._convert_to_qmarks("{s}{i}{s}", dict(i = 123, s = "foo")) == ("???", ("foo", 123, "foo"))
        assert r._convert_to_qmarks("|{s}|{i}|{c}|", dict(i = 123, s = "foo", c = b"")) == ("|?|?|?|", ("foo", 123, b""))

        assert r._convert_to_qmarks("{x} ? {y} ? {z} " * 10000, dict(x = None, y = 123, z = "foo")) == \
                                   ("? ? ? ? ? " * 10000, (None, 123, "foo") * 10000)

        with expected(KeyError("missing")):
            r._convert_to_qmarks("{missing}", {})

        with expected(ValueError("Unknown ")):
            r._convert_to_qmarks("{i:d}", dict(i = 123))

    test_qmarks()

    ###################################

    def use_standalone_resource(database, f, *args):

        db_config = pmnc.config_resource_postgresql_1.copy()
        assert db_config.pop("protocol") == "postgresql_pg8000"
        db_config["database"] = database

        r = Resource(database, **db_config)
        r.connect()
        try:
            r.begin_transaction("xid", source_module_name = "", transaction_options = {},
                                resource_args = (), resource_kwargs = {})
            try:
                result = f(r, *args)
            except:
                r.rollback()
                raise
            else:
                r.commit()
        finally:
            r.disconnect()

        return result

    ###################################

    def test_db_encoding():

        def test_encoding(database, database_encoding):

            def _verify_encoding(r):
                assert r.execute("SHOW server_encoding") == ([{'server_encoding': database_encoding}], )

            use_standalone_resource(database, _verify_encoding)

            def _test_literal_encoding(r, sample):
                assert r.execute("SELECT {sample} AS sample, length({sample}) AS len", sample = sample) == \
                       ([{'sample': sample, 'len': len(sample)}], )
                t = "table_{0:s}".format(random_string(8))
                assert r.execute("CREATE TABLE {0:s} (s varchar({1:d}))".format(t, len(sample)),
                                 "INSERT INTO {0:s} VALUES ({{sample}})".format(t),
                                 "SELECT * FROM {0:s}".format(t),
                                 "UPDATE {0:s} SET s = {{empty}} WHERE s = {{sample}}".format(t),
                                 "SELECT * FROM {0:s}".format(t),
                                 "DROP TABLE {0:s}".format(t),
                                 sample = sample, empty = "") == ([], [], [{'s': sample}], [], [{'s': ''}], [])

            use_standalone_resource(database, _test_literal_encoding, rus)

        # the following databases need to exist, each in its own encoding

        test_encoding("test_db_win1251", "WIN1251")
        #test_encoding("test_db_koi8r", "KOI8")
        #test_encoding("test_db_utf8", "UTF8")

    test_db_encoding()

    ###################################

    def test_supported_types():

        def test_value(typename, value):
            fake_request(10.0)
            t = "table_{0:s}".format(random_string(8))
            return pmnc.transaction.\
                postgresql_1.execute("SELECT ({{value}})::{typename} AS value".format(typename = typename),
                                     "CREATE TABLE {t} (value {typename})".format(t = t, typename = typename),
                                     "INSERT INTO {t} (value) VALUES (({{value}})::{typename})".format(t = t, typename = typename),
                                     "SELECT value FROM {t}".format(t = t),
                                     "DROP TABLE {t}".format(t = t),
                                     value = value)

        # None

        def test_none():

            assert test_value("integer", None) == ([{'value': None}], [], [], [{'value': None}], [])
            assert test_value("char(1)", None) == ([{'value': None}], [], [], [{'value': None}], [])

        test_none()

        # int

        def test_int():

            assert test_value("integer", 0) == ([{'value': 0}], [], [], [{'value': 0}], [])

            assert test_value("int4", 2**31-1) == ([{'value': 2**31-1}], [], [], [{'value': 2**31-1}], [])
            try:
                test_value("int4", 2**31)
            except SQLResourceError as e:
                assert e.code is None and e.state == "22003" and e.description == "integer out of range"
                assert e.recoverable and not e.terminal
            else:
                assert False

            assert test_value("int4", -2**31) == ([{'value': -2**31}], [], [], [{'value': -2**31}], [])
            try:
                test_value("int4", -2**31-1)
            except SQLResourceError as e:
                assert e.code is None and e.state == "22003" and e.description == "integer out of range"
                assert e.recoverable and not e.terminal
            else:
                assert False

            assert test_value("int8", 2**63-1) == ([{'value': 2**63-1}], [], [], [{'value': 2**63-1}], [])
            assert test_value("int8", -2**63) == ([{'value': -2**63}], [], [], [{'value': -2**63}], [])

        test_int()

        # bool

        def test_bool():

            assert test_value("boolean", False) == ([{'value': False}], [], [], [{'value': False}], [])
            assert test_value("boolean", True) == ([{'value': True}], [], [], [{'value': True}], [])

        test_bool()

        # datetime

        def test_datetime():

            dt = datetime.now()
            assert test_value("timestamp without time zone", dt) == ([{'value': dt}], [], [], [{'value': dt}], [])

            dt = datetime(year = 1900, month = 1, day = 1)
            assert test_value("timestamp without time zone", dt) == ([{'value': dt}], [], [], [{'value': dt}], [])

            dt = datetime(2001, 2, 3)
            assert test_value("date", "2001-02-03") == ([{'value': dt}], [], [], [{'value': dt}], [])

        test_datetime()

        # timedelta/interval

        def test_timedelta():

            td = timedelta(minutes = 10)
            assert test_value("interval", td) == ([{'value': td}], [], [], [{'value': td}], [])

            td = timedelta(microseconds = 1)
            assert test_value("interval", td) == ([{'value': td}], [], [], [{'value': td}], [])

            td = timedelta(days = 3650, hours = 23, minutes = 59, seconds = 59, microseconds = 999999)
            assert test_value("interval", td) == ([{'value': td}], [], [], [{'value': td}], [])

            with expected(ResourceError, "month intervals cannot be converted"):
                pmnc.transaction.postgresql_1.execute("SELECT '1 month'::interval AS td")

        test_timedelta()

        # decimal

        def test_decimal():

            numeric_type = "numeric({0[0]:d}, {0[1]:d})".format(pmnc.config_resource_postgresql_1.get("decimal_precision"))

            d = Decimal("0.0")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

            d = Decimal("1.0")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

            d = Decimal("-1.0")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

            d = Decimal("0.01")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

            d = Decimal("-0.01")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

            d = Decimal("98765432.1")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

            d = Decimal("-98765432.1")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

            d = Decimal("123.45")
            assert test_value(numeric_type, d) == ([{'value': d}], [], [], [{'value': d}], [])

        test_decimal()

        # float

        def test_float():

            def eq(v1, v2, d):
                return abs(v1 - v2) < d

            f = 0.0
            v1, v2 = map(lambda d: d[0]["value"], test_value("double precision", f)[::3])
            assert eq(v1, f, 1e-9) and eq(v2, f, 1e-9)

            f = 1.234567891
            v1, v2 = map(lambda d: d[0]["value"], test_value("float", f)[::3])
            assert eq(v1, f, 1e-9) and eq(v2, f, 1e-9)

            f = -1.234567891
            v1, v2 = map(lambda d: d[0]["value"], test_value("real", f)[::3])
            assert not eq(v1, f, 1e-9) and not eq(v2, f, 1e-9)
            assert eq(v1, f, 1e-7) and eq(v2, f, 1e-7)

            f = 1e308
            v1, v2 = map(lambda d: d[0]["value"], test_value("double precision", f)[::3])
            assert eq(v1, f, 1e-3) and eq(v2, f, 1e-3)

        test_float()

        # str

        def test_str():

            assert test_value("char(1)", "") == ([{'value': " "}], [], [], [{'value': " "}], [])
            assert test_value("varchar(1)", "") == ([{'value': ""}], [], [], [{'value': ""}], [])
            assert test_value("character varying(66)", rus) == ([{'value': rus}], [], [], [{'value': rus}], [])

        test_str()

        # bytes

        def test_bytes():

            assert test_value("bytea", b"") == ([{'value': b""}], [], [], [{'value': b""}], [])
            r = rus.encode("windows-1251")
            assert test_value("bytea", r) == ([{'value': r}], [], [], [{'value': r}], [])
            r = rus.encode("utf-8")
            assert test_value("bytea", r) == ([{'value': r}], [], [], [{'value': r}], [])

        test_bytes()

        # list

        def test_list():

            assert test_value("int[]", None) == ([{'value': None}], [], [], [{'value': None}], [])
            assert test_value("varchar[]", [ "foo" ]) == ([{'value': ['foo']}], [], [], [{'value': ['foo']}], [])

            try:
                test_value("varchar[]", [])
            except SQLResourceError as e:
                assert e.code is None and e.state == "PG800" and e.description == "array has no values"
                assert e.recoverable and e.terminal
            else:
                assert False

            try:
                test_value("varchar[]", [ "foo", 123 ])
            except SQLResourceError as e:
                assert e.code is None and e.state == "PG800" and e.description == "not all array elements are of type <class 'str'>"
                assert e.recoverable and e.terminal
            else:
                assert False

            # doesn't seem to support multidimensional arrays

        test_list()

    test_supported_types()

    ###################################

    def test_errors():

        fake_request(10.0)
        try:
            pmnc.transaction.postgresql_1.execute("SELECT 1/0")
        except SQLResourceError as e:
            assert e.code is None and e.state == "22012" and e.description == "division by zero"
            assert e.recoverable and not e.terminal
        else:
            assert False

        t = "table_{0:s}".format(random_string(8))

        fake_request(10.0)
        try:
            pmnc.transaction.postgresql_1.execute(
                "CREATE TABLE {t} (id int PRIMARY KEY)".format(t = t),
                "INSERT INTO {t} VALUES ({{id}})".format(t = t),
                "INSERT INTO {t} VALUES ({{id}})".format(t = t),
                id = 123)
        except SQLResourceError as e:
            assert e.code is None and e.state == "23505"
            assert e.description.startswith("duplicate key value violates unique constraint")
            assert e.recoverable and not e.terminal
        else:
            assert False

        fake_request(10.0)
        try:
            pmnc.transaction.postgresql_1.execute("DROP TABLE {t}".format(t = t))
        except SQLResourceError as e:
            assert e.code is None and e.state == "42P01"
            assert e.description == "table \"{0:s}\" does not exist".format(t)
            assert e.recoverable and not e.terminal
        else:
            assert False

    test_errors()

    ###################################

    def test_unsupported_types():

        def test_type(typename, value):
            fake_request(10.0)
            return pmnc.transaction.postgresql_1.execute(
                "SELECT ({{value}})::{typename} AS value".format(typename = typename),
                value = value)

        try:
            test_type("time", "01:23:45")
        except ResourceError as e:
            assert e.code is None and e.description == "type time cannot be converted"
            assert not e.recoverable and e.terminal
        else:
            assert False

    test_unsupported_types()

    ###################################

    def test_performance():

        pmnc.log("begin performance test")

        # create table

        t = "table_{0:s}".format(random_string(8))

        fake_request(10.0)
        pmnc.transaction.postgresql_1.execute(
            "CREATE TABLE {t} (id int PRIMARY KEY, key char(8) UNIQUE NOT NULL, "
                              "pad varchar(200) NOT NULL)".format(t = t))

        # populate table

        start = time()

        for i in range(10):
            fake_request(10.0)
            sqls, params = [], {}
            for j in range(100):
                id = i * 100 + j
                params["id{0:d}".format(id)] = id
                params["key{0:d}".format(id)] = str(id)
                params["pad{0:d}".format(id)] = random_string(200)
                sql = "INSERT INTO {t} VALUES ({{id{id}}}, {{key{id}}}, {{pad{id}}})".format(t = t, id = id)
                sqls.append(sql)
            pmnc.transaction.postgresql_1.execute(*sqls, **params)

        pmnc.log("{0:.01f} insert(s)/sec".format(1000 / (time() - start)))

        # query table

        stop = Timeout(10.0)
        count = InterlockedCounter()

        def th_proc():
            while not stop.expired:
                fake_request(10.0)
                key = randint(0, 999)
                rs = pmnc.transaction.postgresql_1.execute(
                    "SELECT id FROM {t} WHERE key = {{key}}".format(t = t), key = str(key))
                assert rs[0][0]["id"] == key
                count.next()

        ths = [ Thread(target = th_proc) for i in range(5) ]
        for th in ths: th.start()
        for th in ths: th.join()

        pmnc.log("{0:.01f} select(s)/sec".format(count.next() / 10.0))

        # drop table

        fake_request(10.0)
        pmnc.transaction.postgresql_1.execute("DROP TABLE {t}".format(t = t))

        pmnc.log("end performance test")

    pmnc._loader.set_log_level("LOG")
    try:
        test_performance()
    finally:
        pmnc._loader.set_log_level("DEBUG")

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
