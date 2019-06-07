#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
################################################################################
#
# This module implements resource to access Oracle databases,
# uses cx_Oracle library.
#
# Sample configuration (config_resource_oracle_1.py)
#
# config = dict \
# (
# protocol = "oracle_cx",                     # meta
# decimal_precision = (10, 2),                # sql
# server_address = ("db.domain.com", 1521),   # oracle
# database = "database",                      # oracle
# username = "user",                          # oracle
# password = "pass",                          # oracle
# )
#
# Sample usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.oracle_1.execute("INSERT INTO t (id, name) VALUES ({id}, {name})", # query 1
#                     "SELECT name FROM t WHERE id = {id}",             # query 2
#                     id = 123, name = "foo")                           # parameters
# insert_result, select_result = xa.execute()[0]
# assert insert_result == []
# assert select_result == [{"name": "foo"}]
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

import os; from os import urandom, environ
import binascii; from binascii import b2a_hex
import decimal; from decimal import Decimal
import datetime; from datetime import datetime, timedelta
import re; from re import compile as regex
import cx_Oracle; from cx_Oracle import makedsn, connect, TIMESTAMP, INTERVAL, \
                       Error as Oracle_Error, _Error as _Oracle_Error

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string, trace_string
import typecheck; from typecheck import typecheck
import pmnc.resource_pool; from pmnc.resource_pool import SQLResource, \
                                ResourceError, SQLResourceError

###############################################################################

environ["NLS_LANG"] = "AMERICAN_AMERICA.UTF8"

###############################################################################

class Resource(SQLResource): # Oracle resource

    _supported_types = SQLResource._supported_types | { float, timedelta } - { bool }
    _execute_parser = regex("^\\s*[Ee][Xx][Ee][Cc][Uu][Tt][Ee]\\s+([A-Za-z0-9_$#.@]+)\\s*\\(.*")

    @typecheck
    def __init__(self, name: str, *,
                 decimal_precision: (int, int),
                 server_address: (str, int),
                 database: str,
                 username: str,
                 password: str):

        SQLResource.__init__(self, name, decimal_precision = decimal_precision)

        self._server_host, self._server_port = server_address
        self._database = database
        self._username = username
        self._password = password

        self._random_separator = "|{0:s}|".format(b2a_hex(urandom(8)).decode("ascii"))

    ###################################

    def connect(self):
        SQLResource.connect(self)
        dsn = makedsn(self._server_host, self._server_port, self._database)
        self._connection = connect(user = self._username, password = self._password,
                                   dsn = dsn, threaded = 1) # 1 = OCI_THREADED
        try:
            self._connection.autocommit = False
        except:
            self._connection.close()
            raise

    ###################################

    def begin_transaction(self, *args, **kwargs):
        SQLResource.begin_transaction(self, *args, **kwargs)
        self._connection.begin()

    ###################################

    def _execute_sql(self, sql, params):

        try:
            sql, params = self._convert_to_named(sql, params)
            param_list = ", ".join(map(lambda n_v: "{0:s} = {1:s}".\
                                       format(n_v[0], isinstance(n_v[1], str) and
                                                      "'{0:s}'".format(n_v[1]) or str(n_v[1])),
                                       params.items()))
        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

        pmnc.log.info(">> {0:s}".format(sql))
        if param_list:
            if pmnc.log.debug:
                pmnc.log.debug("-- {0:s} -- ({1:s})".format(sql, param_list))

        try:

            cursor = self._connection.cursor()
            try:

                # avoid using floats with numbers

                cursor.numbersAsStrings = True

                # avoid truncation of timestamps and intervals

                param_sizes = {}
                for n, v in params.items():
                    if isinstance(v, datetime):
                        param_sizes[n] = TIMESTAMP
                    elif isinstance(v, timedelta):
                        param_sizes[n] = INTERVAL
                if param_sizes:
                    cursor.setinputsizes(**param_sizes)

                # execute the query, making stored procedure a special case

                execute = self._execute_parser.match(sql)
                if execute:
                    proc_name = execute.group(1)
                    result = self._connection.cursor()
                    try:
                        params.update(result = result) # providing out sys_refcursor parameter result
                        cursor.callproc(proc_name, keywordParameters = params)
                    finally:
                        try:
                            cursor.close()
                        finally:
                            cursor = result
                else:
                    cursor.execute(sql, **params)

                # extract the result

                if cursor.description is not None:
                    description = tuple(dict(name = t[0], type_name = t[1].__name__) for t in cursor.description)
                    records = [ { field["name"]: self.cx_TYPE(field["type_name"], value)
                                  for field, value in zip(description, record) } for record in cursor ]
                else:
                    records = [] # not a SELECT query

                records_affected = cursor.rowcount

            finally:
                cursor.close()

            if records_affected > 0:
                pmnc.log.info("<< OK, {0:d} record(s)".format(records_affected))
            else:
                pmnc.log.info("<< OK")

        except Oracle_Error as e:
            e = e.args[0]
            if isinstance(e, _Oracle_Error):
                code, message = e.code, e.message
            else:
                code, message = -1, "cx_Oracle error: {0:s}".format(str(e))
            pmnc.log.warning("<< {0:s}{1:s} !! Oracle_Error(\"{2:d}: {3:s}\") in {4:s}".\
                             format(sql, " -- ({0:s})".format(param_list)
                                    if param_list else "", code, message, trace_string()))
            SQLResourceError.rethrow(
                    code = code, description = message, recoverable = True) # note that there is no state
        except:
            pmnc.log.warning("<< {0:s}{1:s} !! {2:s}".\
                             format(sql, " -- ({0:s})".format(param_list)
                                    if param_list else "", exc_string()))
            ResourceError.rethrow(recoverable = True)
        else:
            return records

    ###################################

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def disconnect(self):
        try:
            self._connection.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            SQLResource.disconnect(self)

    ###################################

    # this method takes "SELECT {foo} FROM {bar}", { "foo": 1, "bar": 2, "biz": "baz" }
    # and returns "SELECT :foo FROM :bar", { "foo": 1, "bar": 2 }

    def _convert_to_named(self, sql, params):
        sql = sql.format(**{ n: "{0:s}{1:s}{0:s}".format(self._random_separator, n)
                             for n in params.keys() })
        sql_parts = sql.split(self._random_separator)
        return "".join(i % 2 == 1 and ":{0:s}".format(s) or s for i, s in enumerate(sql_parts)), \
               { n: params[n] for n in sql_parts[1::2] }

    ###################################

    def _py_to_sql_float(self, v):
        return v

    def _sql_to_py_float(self, v):
        return v

    def _py_to_sql_timedelta(self, v):
        return v

    def _sql_to_py_timedelta(self, v):
        return v

    ###################################

    class cx_TYPE:
        def __init__(self, type_name, value):
            self.type_name, self.value = type_name, value

    def _sql_to_py_cx_TYPE(self, v):
        return getattr(self, "_cx_to_py_{0:s}".format(v.type_name))(v.value)

    def _cx_to_py_STRING(self, v):
        if v is None:
            return None
        return str(v)

    _cx_to_py_FIXED_CHAR = _cx_to_py_LONG_STRING = \
    _cx_to_py_CLOB = _cx_to_py_NCLOB = _cx_to_py_STRING

    def _cx_to_py_NUMBER(self, v):

        if v is None:
            return None
        elif isinstance(v, int):
            return v
        else:
            return Decimal(v)

    def _cx_to_py_NATIVE_FLOAT(self, v):
        return v

    def _cx_to_py_BINARY(self, v):
        return v

    _cx_to_py_LONG_BINARY = _cx_to_py_BINARY

    def _cx_to_py_BLOB(self, v):
        if v is None:
            return None
        return v.read()

    def _cx_to_py_ROWID(self, v):
        return v

    def _cx_to_py_DATETIME(self, v):
        return v

    _cx_to_py_TIMESTAMP = _cx_to_py_DATETIME

    def _cx_to_py_INTERVAL(self, v):
        return v

###############################################################################

def self_test():

    from expected import expected
    from pmnc.request import fake_request
    from math import log10

    ###################################

    rus = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"

    random_string = lambda n: b2a_hex(urandom(n))[:n].decode("ascii")

    ###################################

    def test_convert_to_named():

        db_config = pmnc.config_resource_oracle_1.copy()
        assert db_config.pop("protocol") == "oracle_cx"
        r = Resource("test", **db_config)

        assert r._convert_to_named("", {}) == ("", {})
        assert r._convert_to_named("", { "foo": 1 }) == ("", {})
        assert r._convert_to_named("{foo}", { "foo": 1 }) == (":foo", { "foo": 1 })
        with expected(KeyError("foo")):
            r._convert_to_named("{foo}", {})

        assert r._convert_to_named("{{{foo}}}", { "foo": 1, "bar": 2 }) == ("{:foo}", { "foo": 1 })
        assert r._convert_to_named("{foo}{foo}", { "foo": 1 }) == (":foo:foo", { "foo": 1 })
        assert r._convert_to_named("{foo}:foo:{{foo}}:foo{foo}", { "foo": 1 }) == (":foo:foo:{foo}:foo:foo", { "foo": 1 })
        with expected(KeyError("foo{{foo}}")):
            r._convert_to_named("{{{foo{{foo}}}}}", { "foo": 1 })
        assert r._convert_to_named("{{{foo}{{{foo}}}}}", { "foo": 1, "bar": None }) == ("{:foo{:foo}}", { "foo": 1 })

        assert r._convert_to_named("SELECT {foo} FROM {bar}", { "foo": 1, "bar": 2, "biz": "baz" }) == \
               ("SELECT :foo FROM :bar", { "foo": 1, "bar": 2 })

    test_convert_to_named()

    ###################################

    def test_session_params():

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER = 'NLS_NUMERIC_CHARACTERS'")
        record = xa.execute()[0][0][0]
        assert record["VALUE"][0] == record["value"][0] == "." # this is required, set NLS_LANG properly

    test_session_params()

    ###################################

    def test_transaction_isolation():

        fake_request(10.0)

        tn = "table_{0:s}".format(random_string(8))

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("CREATE TABLE {0:s} (ID NUMBER(8) NOT NULL PRIMARY KEY)".format(tn))
        xa.execute()

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn),
                            "SELECT ID FROM {0:s}".format(tn), id = 1)
        xa.oracle_1.execute("INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn),
                            "SELECT ID FROM {0:s}".format(tn), id = 2)
        assert xa.execute() == (([], [{ "ID": 1 }]), ([], [{ "ID": 2 }]))

        fake_request(5.0)

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn), id = 3) # causes a deadlock
        xa.oracle_1.execute("INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn), id = 3)
        with expected(Exception("request deadline")):
            xa.execute()

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("SELECT id FROM {0:s} ORDER BY id".format(tn),
                            "DROP TABLE {0:s}".format(tn))
        assert xa.execute()[0] == ([{ "ID": 1 }, { "ID": 2 }], [])

    test_transaction_isolation()

    ###################################

    def test_ddl_transactions():

        def test_sequential():

            fake_request(10.0)

            tn = "table_{0:s}".format(random_string(8))

            xa = pmnc.transaction.create()
            xa.oracle_1.execute("CREATE TABLE {0:s} (ID NUMBER(8))".format(tn),
                                "INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn),
                                "THIS SHOULD FAIL", id = 1)
            try:
                xa.execute()
            except SQLResourceError as e:
                assert e.code == 900 and e.description.startswith("ORA-00900: ") # invalid SQL statement
                assert e.recoverable and e.terminal # note that the transaction had unrecoverable side effects
            else:
                assert False

            # because in Oracle DDL commits, the empty table remains

            xa = pmnc.transaction.create()
            xa.oracle_1.execute("SELECT COUNT(*) AS c FROM {0:s}".format(tn),
                                "DROP TABLE {0:s}".format(tn))
            assert xa.execute()[0][0][0]["c"] == 0

            # see if data inserted before the DDL stays

            tn1 = "table_{0:s}".format(random_string(8))
            tn2 = "table_{0:s}".format(random_string(8))

            xa = pmnc.transaction.create()
            xa.oracle_1.execute("CREATE TABLE {0:s} (ID NUMBER(8))".format(tn1),
                                "INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn1),
                                "CREATE TABLE {0:s} (ID NUMBER(8))".format(tn2),
                                "INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn2),
                                "THIS SHOULD FAIL", id = 1)
            try:
                xa.execute()
            except SQLResourceError as e:
                assert e.code == 900 and e.description.startswith("ORA-00900: ") # invalid SQL statement
                assert e.recoverable and e.terminal # note that the transaction had unrecoverable side effects
            else:
                assert False

            xa = pmnc.transaction.create()
            xa.oracle_1.execute("SELECT COUNT(*) AS c FROM {0:s}".format(tn1),
                                "DROP TABLE {0:s}".format(tn1),
                                "SELECT COUNT(*) AS c FROM {0:s}".format(tn2),
                                "DROP TABLE {0:s}".format(tn2))
            assert xa.execute()[0] == ([{ "C": 1 }], [], [{ "C": 0 }], [])

        test_sequential()

        def test_parallel():

            tn1 = "table_{0:s}".format(random_string(8))
            tn2 = "table_{0:s}".format(random_string(8))

            # when executed in separate connections, the transactions only commit up to the DDL

            xa = pmnc.transaction.create()
            xa.oracle_1.execute("CREATE TABLE {0:s} (ID NUMBER(8))".format(tn1),
                                "INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn1), id = 1)
            xa.oracle_1.execute("CREATE TABLE {0:s} (ID NUMBER(8))".format(tn2),
                                "INSERT INTO {0:s} (ID) VALUES ({{id}})".format(tn2), id = 2)
            xa.oracle_1.execute("THIS SHOULD FAIL")
            try:
                xa.execute()
            except SQLResourceError as e:
                assert e.code == 900 and e.description.startswith("ORA-00900: ") # invalid SQL statement
                assert e.recoverable and e.terminal # note that the transaction had unrecoverable side effects
            else:
                assert False

            xa = pmnc.transaction.create()
            xa.oracle_1.execute("SELECT COUNT(*) AS c FROM {0:s}".format(tn1),
                                "DROP TABLE {0:s}".format(tn1),
                                "SELECT COUNT(*) AS c FROM {0:s}".format(tn2),
                                "DROP TABLE {0:s}".format(tn2))
            assert xa.execute()[0] == ([{ "C": 0 }], [], [{ "C": 0 }], [])

        test_parallel()

    test_ddl_transactions()

    ###################################

    def test_data_types():

        def test_data_type(t, vs):

            tn = "table_{0:s}".format(random_string(8))

            sqls = [ "CREATE TABLE {0:s} (i number(8), v {1:s})".format(tn, t) ]
            params = {}
            for i, v in enumerate(vs):
                sqls.append("INSERT INTO {0:s} (i, v) VALUES ({1:d}, {{v{1:d}}})".format(tn, i))
                params["v{0:d}".format(i)] = v
            sqls.append("SELECT v FROM {0:s} ORDER BY i".format(tn))
            sqls.append("DROP TABLE {0:s}".format(tn))

            fake_request(30.0)

            xa = pmnc.transaction.create()
            xa.oracle_1.execute(*sqls, **params)
            records = xa.execute()[0][-2]
            result = [ r["V"] for r in records ] # note that field name is capitalized
            return result

        # char

        assert test_data_type("char", [ None, "", "1" ]) == [ None, None, "1" ]
        assert test_data_type("char(2)", [ "1" ]) == [ "1 " ]
        try:
            test_data_type("char(10000)", [])
        except SQLResourceError as e:
            assert e.code == 910 and e.description.startswith("ORA-00910: ") # specified length too long for its datatype
            assert e.recoverable and e.terminal
        else:
            assert False

        # nchar

        assert test_data_type("nchar", [ None, "", "1", rus[0] ]) == [ None, None, "1", rus[0] ]
        assert test_data_type("nchar(2)", [ "1" ]) == [ "1 " ]
        assert test_data_type("nchar(66)", [ rus ]) == [ rus ]
        try:
            test_data_type("nchar(1)", [ rus[0:2] ])
        except SQLResourceError as e:
            assert e.code == 12899 and e.description.startswith("ORA-12899: value too large for column ")
            assert e.recoverable and e.terminal
        else:
            assert False
        try:
            test_data_type("nchar(10000)", [])
        except SQLResourceError as e:
            assert e.code == 910 and e.description.startswith("ORA-00910: ") # specified length too long for its datatype
            assert e.recoverable and e.terminal
        else:
            assert False

        # varchar2

        assert test_data_type("varchar2(1)", [ None, "", "1" ]) == [ None, None, "1" ]
        assert test_data_type("varchar2(2)", [ "1" ]) == [ "1" ]
        try:
            test_data_type("varchar2(10000)", [])
        except SQLResourceError as e:
            assert e.code == 910 and e.description.startswith("ORA-00910: ") # specified length too long for its datatype
            assert e.recoverable and e.terminal
        else:
            assert False

        # nvarchar2

        assert test_data_type("nvarchar2(1)", [ None, "", "1", rus[0] ]) == [ None, None, "1", rus[0] ]
        assert test_data_type("nvarchar2(2)", [ "1" ]) == [ "1" ]
        assert test_data_type("nvarchar2(66)", [ rus ]) == [ rus ]
        try:
            test_data_type("nvarchar2(1)", [ rus[0:2] ])
        except SQLResourceError as e:
            assert e.code == 12899 and e.description.startswith("ORA-12899: value too large for column ")
            assert e.recoverable and e.terminal
        else:
            assert False
        try:
            test_data_type("nvarchar2(10000)", [])
        except SQLResourceError as e:
            assert e.code == 910 and e.description.startswith("ORA-00910: ") # specified length too long for its datatype
            assert e.recoverable and e.terminal
        else:
            assert False

        # long

        assert test_data_type("long", [ None, "", "x" * 10000 ]) == [ None, None, "x" * 10000 ]

        # clob

        assert test_data_type("clob", [ None, "", "x" * 10000 ]) == [ None, None, "x" * 10000 ]

        # nclob

        assert test_data_type("nclob", [ None, "", rus * 150 ]) == [ None, None, rus * 150 ]

        # number

        ints = test_data_type("number(2)", [ None, 0, 99, -99, Decimal("0"), Decimal("99"), Decimal("-99"), 0.0, 99.0, -99.0 ])
        assert ints == [ None, 0, 99, -99, 0, 99, -99, 0, 99, -99 ]
        assert list(map(type, ints[1:])) == [ Decimal ] * (len(ints) - 1) # note that integers are returned as Decimals

        assert test_data_type("number(2)", [ Decimal("98.99"), Decimal("-98.99"), 98.99, -98.99 ]) == [ 99, -99, 99, -99 ]
        try:
            test_data_type("number(2)", [ "x" ])
        except SQLResourceError as e:
            assert e.code == 1722 and e.description.startswith("ORA-01722: ") # invalid number
            assert e.recoverable and e.terminal
        else:
            assert False
        try:
            test_data_type("number(2)", [ 100 ])
        except SQLResourceError as e:
            assert e.code == 1438 and e.description.startswith("ORA-01438: ") # value larger than specified precision allowed for this column
            assert e.recoverable and e.terminal
        else:
            assert False
        assert test_data_type("number(2)", [ Decimal("0.1") ]) == [ 0 ]
        assert test_data_type("number(2)", [ 0.1 ]) == [ 0 ]

        assert test_data_type("number(4, 2)", [ None, 0, 99, -99, Decimal("0.0"), Decimal("99.99"), Decimal("-99.99"), Decimal("0.01"), Decimal("-0.01"), 0.0, 99.99, -99.99, 0.01, -0.01 ]) == \
               [ None, Decimal("0.00"), Decimal("99.00"), Decimal("-99.00"), Decimal("0.00"), Decimal("99.99"), Decimal("-99.99"), Decimal("0.01"), Decimal("-0.01"), Decimal("0.00"), Decimal("99.99"), Decimal("-99.99"), Decimal("0.01"), Decimal("-0.01") ]
        assert test_data_type("number(4, 2)", [ Decimal("0.0001"), Decimal("-99.9899"), Decimal("99.9899") ]) == \
               [ Decimal("0.00"), Decimal("-99.9900"), Decimal("99.9900") ]
        try:
            test_data_type("number(4, 2)", [ "0,0" ])
        except SQLResourceError as e:
            assert e.code == 1722 and e.description.startswith("ORA-01722: ") # invalid number
            assert e.recoverable and e.terminal
        else:
            assert False
        try:
            test_data_type("number(4, 2)", [ Decimal("100.00") ])
        except SQLResourceError as e:
            assert e.code == 1438 and e.description.startswith("ORA-01438: ") # value larger than specified precision allowed for this column
            assert e.recoverable and e.terminal
        else:
            assert False

        assert test_data_type("number(7, 4)", [ None, 999, -999, Decimal("999.9999"), Decimal("-999.9999"), Decimal("0.0001"), Decimal("-0.0001"), 999.9999, -999.9999, 0.0001, -0.0001 ]) == \
               [ None, Decimal("999.0000"), Decimal("-999.0000"), Decimal("999.9999"), Decimal("-999.9999"), Decimal("0.0001"), Decimal("-0.0001"), Decimal("999.9999"), Decimal("-999.9999"), Decimal("0.0001"), Decimal("-0.0001") ]
        try:
            test_data_type("number(7, 4)", [ 1000 ])
        except SQLResourceError as e:
            assert e.code == 1438 and e.description.startswith("ORA-01438: ") # value larger than specified precision allowed for this column
            assert e.recoverable and e.terminal
        else:
            assert False
        try:
            test_data_type("number(7, 4)", [ 1000.0 ])
        except SQLResourceError as e:
            assert e.code == 1438 and e.description.startswith("ORA-01438: ") # value larger than specified precision allowed for this column
            assert e.recoverable and e.terminal
        else:
            assert False

        try:
            test_data_type("number(7, 4)", [ Decimal("0.00001") ])
        except ResourceError as e:
            assert e.description == "decimal value too precise"
            assert e.recoverable and not e.terminal
        else:
            assert False
        try:
            test_data_type("number(7, 4)", [ Decimal("10000000") ])
        except ResourceError as e:
            assert e.description == "decimal value too large"
            assert e.recoverable and not e.terminal
        else:
            assert False

        assert test_data_type("number(7, 4)", [ 0.00001 ]) == [ Decimal("0") ]

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("SELECT 0.1 AS X1, 0.01 AS X2, 0.001 AS X3, 0.0001 AS X4 FROM dual")
        assert xa.execute()[0][0] == [ dict(X1 = Decimal("0.1"), X2 = Decimal("0.01"), X3 = Decimal("0.001"), X4 = Decimal("0.0001")) ]

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("SELECT 0.00001 AS X5 FROM dual")
        try:
            xa.execute()
        except ResourceError as e:
            assert e.description == "decimal value too precise"
            assert not e.recoverable and e.terminal
        else:
            assert False

        # float

        def loose_compare(a, b):
            assert len(a) == len(b)
            for aa, bb in zip(a, b):
                if (aa is None and bb is None) or (aa == float("nan") and bb == float("nan")) or \
                   (aa == float("inf") and bb == float("inf")) or (aa == float("-inf") and bb == float("-inf")):
                    continue
                if abs(aa) < 1e-6 and abs(bb) < 1e-6:
                    continue
                if (aa < 0.0 and bb >= 0.0) or (bb < 0.0 and aa >= 0.0):
                    return False
                if abs(log10(abs(aa)) - log10(abs(bb))) > 1e-6:
                    return False
            else:
                return True

        assert loose_compare(test_data_type("binary_float", [ None, 0, 1000000, -1000000, Decimal("0.0"), Decimal("999.9999"), Decimal("-999.9999"), 0.0, 1000.0, -1000.0 ]),
                             [None, 0.0, 1000000.0, -1000000.0, 0.0, 1000.0, -1000.0, 0.0, 1000.0, -1000.0])

        assert loose_compare(test_data_type("binary_float", [ -3.5e+38, -3.4e+38, 3.4e+38, 3.5e+38 ]),
                             [ float("-inf"), -3.40e+38, 3.40e+38, float("inf") ])

        assert loose_compare(test_data_type("binary_double", [ None, 0, 1000000, -1000000, Decimal("0.0"), Decimal("999.9999"), Decimal("-999.9999"), 0.0, 1000.0, -1000.0 ]),
                             [None, 0.0, 1000000.0, -1000000.0, 0.0, 1000.0, -1000.0, 0.0, 1000.0, -1000.0])

        assert loose_compare(test_data_type("binary_double", [ -1e+126, -1e+125, 1e+125, 1e+126 ]),
                             [ float("-inf"), -1e+125, 1e+125, float("inf") ])

        # binary

        assert test_data_type("raw(1)", [ None, b"", b"\x00" ]) == [ None, None, b"\x00" ]
        assert test_data_type("raw(2000)", [ b"\xff" * 2000 ]) == [ b"\xff" * 2000 ]
        try:
            test_data_type("raw(10000)", [])
        except SQLResourceError as e:
            assert e.code == 910 and e.description.startswith("ORA-00910: ") # specified length too long for its datatype
            assert e.recoverable and e.terminal
        else:
            assert False

        test_data_type("blob", [ None, b"", b"\x00" ]) == [ None, b"", b"\x00" ]
        assert test_data_type("blob", [ b"\xff" * 10000 ]) == [ b"\xff" * 10000 ]

        test_data_type("long raw", [ None, b"", b"\x00" ])
        assert test_data_type("long raw", [ b"\xff" * 10000 ]) == [ b"\xff" * 10000 ]

        # rowid

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("SELECT rowid AS RID, ROWIDTOCHAR(rowid) AS RIDC FROM dual")
        r = xa.execute()[0][0][0]
        assert r["RID"] == r["RIDC"]

        # date

        test_data_type("date", [ None, datetime(2009, 12, 31), datetime(2009, 12, 31, 23, 59, 59), datetime(2009, 12, 31, 23, 59, 59, 999999) ]) == \
                               [ None, datetime(2009, 12, 31), datetime(2009, 12, 31, 23, 59, 59), datetime(2009, 12, 31, 23, 59, 59) ]

        # timestamp

        assert test_data_type("timestamp", [ None, datetime(2009, 12, 31), datetime(2009, 12, 31, 23, 59, 59), datetime(2009, 12, 31, 23, 59, 59, 999999) ]) == \
               [ None, datetime(2009, 12, 31), datetime(2009, 12, 31, 23, 59, 59), datetime(2009, 12, 31, 23, 59, 59, 999999) ]

        assert test_data_type("timestamp(3)", [ None, datetime(2009, 12, 31), datetime(2009, 12, 31, 23, 59, 59), datetime(2009, 12, 31, 23, 59, 59, 123456) ]) == \
               [ None, datetime(2009, 12, 31), datetime(2009, 12, 31, 23, 59, 59), datetime(2009, 12, 31, 23, 59, 59, 123000) ]

        # interval (day to second)

        assert test_data_type("interval day to second", [ None, timedelta(seconds = 1) ]) == [ None, timedelta(seconds = 1) ]

    test_data_types()

    ###################################

    def test_udf():

        fake_request(10.0)

        fn = "func_{0:s}".format(random_string(8))

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("""
CREATE OR REPLACE FUNCTION {0:s} (I NUMBER) RETURN NUMBER IS
BEGIN
    RETURN I * I;
END;
""".format(fn),
                            "SELECT {0:s}({{param}}) AS RESULT FROM dual".format(fn),
                            "DROP FUNCTION {0:s}".format(fn),
                            param = 10, unused_param = 20)
        assert xa.execute()[0] == ([], [ { "RESULT": 100 } ], [])

    test_udf()

    ###################################

    def test_sp():

        fake_request(10.0)

        pn = "proc_{0:s}".format(random_string(8))

        xa = pmnc.transaction.create()
        xa.oracle_1.execute("""
CREATE OR REPLACE PROCEDURE {0:s} (result OUT SYS_REFCURSOR, param INT) IS
BEGIN
   OPEN result FOR SELECT param * param AS param FROM dual;
END {0:s};
""".format(pn),
                            "EXECUTE {0:s}({{param}})".format(pn),
                            "DROP PROCEDURE {0:s}".format(pn),
                            param = 10, unused_param = 20)
        assert xa.execute()[0] == ([], [ { "PARAM": 100 } ], [])

    test_sp()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
