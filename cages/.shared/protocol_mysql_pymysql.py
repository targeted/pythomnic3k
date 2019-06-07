#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
################################################################################
#
# This module implements resource to access MySQL databases,
# uses PyMySQL3 library written in native Python.
#
# Sample configuration (config_resource_mysql_1.py)
#
# config = dict \
# (
# protocol = "mysql_pymysql",                 # meta
# decimal_precision = (10, 2),                # sql
# server_address = ("db.domain.com", 3306),   # mysql
# connect_timeout = 3.0,                      # mysql
# database = "database",                      # mysql
# username = "user",                          # mysql
# password = "pass",                          # mysql
# sql_mode = None,                            # mysql, optional str
# charset = None,                             # mysql, optional str
# )
#
# Sample usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.mysql_1.execute("INSERT INTO t (id, name) VALUES ({id}, {name})", # query 1
#                    "SELECT name FROM t WHERE id = {id}",             # query 2
#                    id = 123, name = "foo")                           # parameters
# insert_records, select_records = xa.execute()[0]
# assert insert_records == []
# for record in select_records:
#     print(record["name"])
#
# or if the only transaction participant:
#
# insert_records, select_records = pmnc.transaction.mysql_1.execute(...)
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

import binascii; from binascii import b2a_hex
import decimal; from decimal import Decimal
import datetime; from datetime import datetime, date, time
import pymysql; from pymysql import Connect
import pymysql.err; from pymysql.err import Error as MySQL_Error

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string, trace_string
import typecheck; from typecheck import typecheck, optional
import pmnc.resource_pool; from pmnc.resource_pool import SQLResource, \
                                ResourceError, SQLResourceError

###############################################################################

class Resource(SQLResource): # MySQL resource

    @typecheck
    def __init__(self, name: str, *,
                 decimal_precision: (int, int),
                 server_address: (str, int),
                 connect_timeout: float,
                 database: str,
                 username: str,
                 password: str,
                 sql_mode: optional(str),
                 charset: optional(str)):

        SQLResource.__init__(self, name, decimal_precision = decimal_precision)

        self._host, self._port = server_address
        self._connect_timeout = connect_timeout
        self._database = database
        self._username = username
        self._password = password
        self._sql_mode = sql_mode
        self._charset = charset or "utf8"

    ###################################

    def connect(self):
        SQLResource.connect(self)
        self._connection = Connect(
                host = self._host, port = self._port, connect_timeout = self._connect_timeout,
                db = self._database, user = self._username, passwd = self._password,
                sql_mode = self._sql_mode, charset = self._charset)
        try:
            self._connection.autocommit(False)
        except:
            self._connection.close()
            raise

    ###################################

    def _execute_sql(self, sql, params):

        try:
            param_list = ", ".join("@{0:s} = {1:s}".format(n, v) for n, v in params.items())
            at_params = { n: "@{0:s}".format(n) for n in params.keys() }
            sql = sql.format(**at_params)
        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

        cursor = self._connection.cursor()
        try:

            for n, v in params.items():
                cursor.execute("SET @{0:s}={1:s}".format(n, v))

            pmnc.log.info(">> {0:s}".format(sql))
            if param_list:
                if pmnc.log.debug:
                    pmnc.log.debug("-- {0:s} -- ({1:s})".format(sql, param_list))

            records = []
            try:

                cursor.execute(sql)
                rowcount = cursor.rowcount

                if rowcount >= 0:
                    pmnc.log.info("<< OK, {0:d} record(s)".format(rowcount))
                    if rowcount > 0 and cursor.description:
                        column_names = [ t[0] for t in cursor.description ]
                        for record in cursor.fetchall():
                            records.append(dict(zip(column_names, record)))
                else:
                    pmnc.log.info("<< OK")

            except MySQL_Error as e:
                code, message = e.args[0].args
                pmnc.log.warning("<< {0:s}{1:s} !! MySQL_Error(\"[{2:d}] {3:s}\") in {4:s}".\
                                 format(sql, " -- ({0:s})".format(param_list)
                                        if param_list else "", code, message, trace_string()))
                SQLResourceError.rethrow(recoverable = True,
                        code = code, description = message) # note that there is no state
            except Exception:
                pmnc.log.warning("<< {0:s}{1:s} !! {2:s}".\
                                 format(sql, " -- ({0:s})".format(param_list)
                                        if param_list else "", exc_string()))
                ResourceError.rethrow(recoverable = True)
            else:
                return records

        finally:
            cursor.close()

    ###################################

    def commit(self):
        self._connection.commit()

    ###################################

    def rollback(self):
        self._connection.rollback()

    ###################################

    def disconnect(self):
        try:
            self._connection.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            SQLResource.disconnect(self)

    ###################################

    _supported_types = SQLResource._supported_types | { float, set, date, time }

    def _py_to_sql_NoneType(self, v):
        return self._connection.escape(SQLResource._py_to_sql_NoneType(self, v))

    def _py_to_sql_int(self, v):
        return self._connection.escape(SQLResource._py_to_sql_int(self, v))

    def _py_to_sql_Decimal(self, v):
        return self._connection.escape(SQLResource._py_to_sql_Decimal(self, v))

    def _py_to_sql_bool(self, v):
        return self._connection.escape(SQLResource._py_to_sql_bool(self, v))

    def _py_to_sql_datetime(self, v):
        return self._connection.escape(SQLResource._py_to_sql_datetime(self, v))

    def _py_to_sql_str(self, v):
        return self._connection.escape(SQLResource._py_to_sql_str(self, v))

    def _py_to_sql_bytes(self, v):
        return "X'{0:s}'".format(b2a_hex(v).decode("ascii"))

    def _py_to_sql_float(self, v):
        return self._connection.escape(v)

    def _py_to_sql_set(self, v):
        return "'{0:s}'".format(",".join(self._connection.escape(s)[1:-1]
                                         for s in v if isinstance(s, str)))

    def _py_to_sql_date(self, v):
        return self._connection.escape(v)

    def _py_to_sql_time(self, v):
        return self._connection.escape(v)

    ###################################

    def _sql_to_py_float(self, v):
        return v

    def _sql_to_py_set(self, v):
        return v

    def _sql_to_py_date(self, v):
        return v

    def _sql_to_py_time(self, v):
        return v

    def _sql_to_py_timedelta(self, v): # PyMySQL represents TIME as timedelta
        return time(hour = v.seconds // 3600,
                    minute = (v.seconds % 3600) // 60,
                    second = v.seconds % 60)

###############################################################################

def self_test():

    from expected import expected
    from os import urandom
    from pmnc.request import fake_request
    from pmnc.resource_pool import TransactionExecutionError

    def random_name():
        alph = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        return "".join(alph[b % len(alph)] for b in urandom(8))

    ###################################

    def test_data_types():

        fake_request(10.0)

        tn = random_name()

        rs = pmnc.transaction.mysql_1.execute(
"""\
CREATE TABLE {0:s}
(
f_DECIMAL DECIMAL(10, 2), f_NUMERIC NUMERIC(10, 2), f_TINYINT TINYINT, f_SMALLINT SMALLINT,
f_MEDIUMINT MEDIUMINT, f_INT INT, f_BIGINT BIGINT, f_FLOAT FLOAT, f_DOUBLE DOUBLE,
f_BIT BIT(2), f_CHAR CHAR(2), f_VARCHAR VARCHAR(2), f_BINARY BINARY(2), f_VARBINARY VARBINARY(256),
f_BLOB BLOB, f_TEXT TEXT, f_ENUM ENUM('a', 'b'), f_SET SET('a', 'b'),
f_DATE DATE, f_TIME TIME, f_DATETIME DATETIME, f_TIMESTAMP TIMESTAMP, f_YEAR YEAR
)
""".format(tn),
"""\
INSERT INTO {0:s}
(f_DECIMAL, f_NUMERIC, f_TINYINT, f_SMALLINT, f_MEDIUMINT, f_INT, f_BIGINT,
 f_FLOAT, f_DOUBLE, f_BIT, f_CHAR, f_VARCHAR, f_BINARY, f_VARBINARY, f_BLOB, f_TEXT,
 f_ENUM, f_SET, f_DATE, f_TIME, f_DATETIME, f_TIMESTAMP, f_YEAR)
VALUES
(1.25, 1.999, 127, 32767, 8388607, 2147483647, 9223372036854775807,
 3.4E-38, 1.7E308, b'10', 'x', 'y', X'01', X'02', 0x01020304, 'TEXT',
 'a', 'a,b', '2001-01-01', '23:59:59', '2001-01-01 23:59:59', '2010-12-31 23:59:59', '1999')
""".format(tn),
"""\
SELECT * FROM {0:s}
""".format(tn))

        assert rs == ([], [], [dict(
f_DECIMAL = Decimal("1.25"), f_NUMERIC = Decimal("2.00"), f_TINYINT = 127, f_SMALLINT = 32767,
f_MEDIUMINT = 8388607, f_INT = 2147483647, f_BIGINT = 9223372036854775807, f_FLOAT = 3.4E-38,
f_DOUBLE = 1.7E308, f_BIT = b"\x02", f_CHAR = "x", f_VARCHAR = "y", f_BINARY = b"\x01\x00",
f_VARBINARY = b"\x02", f_BLOB = b"\x01\x02\x03\x04", f_TEXT = "TEXT", f_ENUM = "a", f_SET = { "a", "b" },
f_DATE = date(2001, 1, 1), f_TIME = time(23, 59, 59), f_DATETIME = datetime(2001, 1, 1, 23, 59, 59),
f_TIMESTAMP = datetime(2010, 12, 31, 23, 59, 59), f_YEAR = 1999,
)])

        bytes256 = bytes(list(range(256)))

        rs = pmnc.transaction.mysql_1.execute(
"""\
TRUNCATE TABLE {0:s}
""".format(tn),
"""\
INSERT INTO {0:s}
(
f_DECIMAL, f_NUMERIC, f_TINYINT, f_SMALLINT, f_MEDIUMINT, f_INT, f_BIGINT,
f_FLOAT, f_DOUBLE, f_BIT, f_CHAR, f_VARCHAR, f_BINARY, f_VARBINARY,
f_BLOB, f_TEXT, f_ENUM, f_SET, f_DATE, f_TIME, f_DATETIME, f_TIMESTAMP, f_YEAR
)
VALUES
(
{{f_DECIMAL}}, {{f_NUMERIC}}, {{f_TINYINT}}, {{f_SMALLINT}}, {{f_MEDIUMINT}}, {{f_INT}}, {{f_BIGINT}},
{{f_FLOAT}}, {{f_DOUBLE}}, {{f_BIT}}, {{f_CHAR}}, {{f_VARCHAR}}, {{f_BINARY}}, {{f_VARBINARY}},
{{f_BLOB}}, {{f_TEXT}}, {{f_ENUM}}, {{f_SET}}, {{f_DATE}}, {{f_TIME}}, {{f_DATETIME}}, {{f_TIMESTAMP}}, {{f_YEAR}}
)
""".format(tn),
"""\
SELECT * FROM {0:s}
""".format(tn),
"""\
DROP TABLE {0:s}
""".format(tn),
f_DECIMAL = Decimal("1.99"), f_NUMERIC = Decimal("1.0099"), f_TINYINT = -128,
f_SMALLINT = -32768, f_MEDIUMINT = -8388608, f_INT = -2147483648, f_BIGINT = -9223372036854775808,
f_FLOAT = 3.4E38, f_DOUBLE = -1.7E308, f_BIT = b"\x03",
f_CHAR = "xx", f_VARCHAR = "yy", f_BINARY = b"\x00\x00", f_VARBINARY = bytes256,
f_BLOB = b"DATA" * 100, f_TEXT = "TEXT" * 100, f_ENUM = "b", f_SET = { "a", "b" },
f_DATE = date(2000, 1, 1), f_TIME = time(12, 34, 56), f_DATETIME = datetime(2000, 1, 1, 12, 34, 56),
f_TIMESTAMP = datetime(2010, 11, 30, 12, 30, 30), f_YEAR = 2010)

        assert rs == ([], [], [dict(
f_YEAR = 2010, f_BIT = b"\x03", f_BINARY = b"\x00\x00", f_BLOB = b"DATA" * 100, f_SMALLINT = -32768, f_BIGINT = -9223372036854775808,
f_FLOAT = 3.4e+38, f_DOUBLE = -1.7e+308, f_TINYINT = -128, f_VARBINARY = bytes256, f_SET = {"a", "b"}, f_DECIMAL = Decimal("1.99"),
f_INT = -2147483648, f_MEDIUMINT = -8388608, f_TIMESTAMP = datetime(2010, 11, 30, 12, 30, 30), f_VARCHAR = "yy", f_TIME = time(12, 34, 56),
f_TEXT = "TEXT" * 100, f_DATE = date(2000, 1, 1), f_NUMERIC = Decimal("1.01"), f_ENUM = "b", f_CHAR = "xx", f_DATETIME = datetime(2000, 1, 1, 12, 34, 56)
)], [])

    test_data_types()

    ###################################

    def test_international():

        fake_request(10.0)

        russian = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"
        assert pmnc.transaction.mysql_1.execute("SELECT {s} AS s", s = russian) == \
               ([{ "s": russian }],)

        # assuming CIS collation in WHERE clause

        tn = random_name()

        rs = pmnc.transaction.mysql_1.execute(
                "CREATE TABLE {0:s} (s VARCHAR(66))".format(tn),
                "INSERT INTO {0:s} (s) VALUES ({{s}})".format(tn),
                "SELECT s FROM {0:s} WHERE s = {{s}}".format(tn),
                "SELECT LOWER(s) AS sl FROM {0:s} WHERE s = {{sl}}".format(tn),
                "DROP TABLE {0:s}".format(tn), s = russian, sl = russian.lower())

        assert rs == ([], [], [{ "s": russian }], [{ "sl": russian.lower() }], [])

    test_international()

    ###################################

    def test_transaction():

        fake_request(10.0)

        tn = random_name()

        try:
            pmnc.transaction.mysql_1.execute(
                "CREATE TABLE {0:s} (i INT)".format(tn),
                "SELECT * FROM {0:s}".format(random_name()))
        except SQLResourceError as e:
            assert e.code == 1146
        else:
            assert False

        # the table persists across transactions

        try:
            pmnc.transaction.mysql_1.execute(
                "INSERT INTO {0:s} (i) VALUES (0)".format(tn),
                "SELECT * FROM {0:s}".format(random_name()))
        except SQLResourceError as e:
            assert e.code == 1146
        else:
            assert False

        # whereas the data does not

        assert pmnc.transaction.mysql_1.execute(
                "SELECT * FROM {0:s}".format(tn),
                "DROP TABLE {0:s}".format(tn)) == ([], [])

    test_transaction()

    ###################################

    def test_deadlock():

        fake_request(10.0)

        tn = random_name()

        pmnc.transaction.mysql_1.execute(
            "CREATE TABLE {0:s} (i INT, PRIMARY KEY (i))".format(tn),
            "INSERT INTO {0:s} (i) VALUES ({{i}})".format(tn), i = 0)

        xa = pmnc.transaction.create()
        xa.mysql_1.execute("SELECT * FROM {0:s} WHERE i = {{zero}} FOR UPDATE".format(tn), zero = 0)
        xa.mysql_1.execute("SELECT * FROM {0:s} WHERE i = {{zero}} FOR UPDATE".format(tn), zero = 0)
        with expected(TransactionExecutionError, "request deadline waiting for intermediate result.*"):
            xa.execute()

    test_deadlock()

    ###################################

    def test_sp():

        fake_request(10.0)

        tn = random_name()

        rs = pmnc.transaction.mysql_1.execute("""\
CREATE PROCEDURE {0:s}(x int)
BEGIN
SELECT x AS x;
END
""".format(tn),
"CALL {0:s}({{one}})".format(tn),
"CALL {0:s}({{null}})".format(tn),
"DROP PROCEDURE {0:s}".format(tn),
one = 1, null = None)

        # this is awkward but SELECT NULL returns nothing

        assert rs == ([], [{ "x": 1 }], [], [])

    test_sp()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
