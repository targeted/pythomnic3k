#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This module implements resource to access MS SQL Server databases via ADODB.
#
# Sample configuration (config_resource_sqlserver_1.py)
#
# config = dict \
# (
# protocol = "sqlserver_adodb",                        # meta
# decimal_precision = (10, 2),                         # sql
# connection_string = "Provider=SQLOLEDB.1;" \
#                     "Integrated Security=SSPI;" \
#                     "Persist Security Info=False;" \
#                     "Initial Catalog=database;" \
#                     "Data Source=.",                 # sqlserver
# )
#
# Sample usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.sqlserver_1.execute("INSERT INTO t (id, name) VALUES ({id}, {name})", # query 1
#                        "SELECT name FROM t WHERE id = {id}",             # query 2
#                        id = 123, name = "foo")                           # parameters
# insert_records, select_records = xa.execute()[0]
# assert insert_records == []
# for record in select_records:
#     print(record["name"])
#
# or if the only transaction participant:
#
# insert_records, select_records = pmnc.transaction.sqlserver_1.execute(...)
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

import os; from os import urandom
import sys; from sys import exc_info
import binascii; from binascii import b2a_hex
import win32com; from win32com import client as com_client
import pywintypes; from pywintypes import com_error
import datetime; from datetime import datetime, date, timedelta
import decimal; from decimal import Decimal

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string, trace_string
import typecheck; from typecheck import typecheck, typecheck_with_exceptions
import pmnc.resource_pool; from pmnc.resource_pool import SQLResource, \
                                ResourceError, SQLResourceError

###############################################################################

ADODataTypes = dict(adBigInt = 20, adBinary = 128, adBoolean = 11, adBSTR = 8,
                    adChapter = 136, adChar = 129, adCurrency = 6, adDate = 7,
                    adDBDate = 133, adDBTime = 134, adDBTimeStamp = 135, adDecimal = 14,
                    adDouble = 5, adEmpty = 0, adError = 10, adFileTime = 64,
                    adGUID = 72, adIDispatch = 9, adInteger = 3, adIUnknown = 13,
                    adLongVarBinary = 205, adLongVarChar = 201, adLongVarWChar = 203,
                    adNumeric = 131, adPropVariant = 138, adSingle = 4, adSmallInt = 2,
                    adTinyInt = 16, adUnsignedBigInt = 21, adUnsignedInt = 19,
                    adUnsignedSmallInt = 18, adUnsignedTinyInt = 17, adUserDefined = 132,
                    adVarBinary = 204, adVarChar = 200, adVariant = 12, adVarNumeric = 139,
                    adVarWChar = 202, adWChar = 130)

ADODataTypeNames = { v: k for k, v in ADODataTypes.items() }

class TypeValueWrapper(tuple): pass # named alias for a (2-)tuple

###############################################################################

class Resource(SQLResource): # SQL server resource

    _supported_types = SQLResource._supported_types | { float }

    @typecheck
    def __init__(self, name: str, *,
                 decimal_precision: (int, int),
                 connection_string: str):

        SQLResource.__init__(self, name, decimal_precision = decimal_precision)
        self._connection_string = connection_string

        self._connection = None
        self._random_separator = "|{0:s}|".format(b2a_hex(urandom(8)).decode("ascii"))
        self._18991231 = date(1899, 12, 31).toordinal()
        self._decimal_bytes = 5 if self.precision <= 9 else 9 if self.precision <= 19 else \
                              13 if self.precision <= 28 else 17

    ###################################

    def connect(self):
        SQLResource.connect(self)
        try:
            self._connection = com_client.Dispatch("ADODB.Connection")
            self._connection.Open(self._connection_string)
        except com_error:
            self._rethrow_adodb_error()

    ###################################

    def begin_transaction(self, *args, **kwargs):
        SQLResource.begin_transaction(self, *args, **kwargs)
        try:
            self._connection.BeginTrans()
        except com_error:
            self._rethrow_adodb_error()

    ###################################

    def _execute_sql(self, sql, params):

        try:
            sql, params = self._convert_to_qmarks(sql, params)
            param_list = ", ".join(map(lambda t_s_v: "{0:s}({1:s})".\
                                       format(t_s_v[0], isinstance(t_s_v[2], str)
                                                        and "'{0:s}'".format(t_s_v[2])
                                                        or str(t_s_v[2])), params))
        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

        pmnc.log.info(">> {0:s}".format(sql))
        if param_list:
            if pmnc.log.debug:
                pmnc.log.debug("-- {0:s} -- ({1:s})".format(sql, param_list))

        records = []
        try:

            command = com_client.Dispatch("ADODB.Command")
            command.CommandText = sql
            command.CommandType = 1 # adCmdText

            for i, (param_type, param_size, param_value) in enumerate(params):
                param = command.CreateParameter(None, ADODataTypes[param_type], 1, # adParamInput
                                                param_size, param_value)
                if param_type in ("adDecimal", "adNumeric"):
                    param.Precision = self.precision
                    param.NumericScale = self.scale
                command.Parameters.Append(param)

            command.ActiveConnection = self._connection
            command.Prepared = True

            recordset = command.Execute()[0]

            if recordset.State == 1: # adStateOpen
                try:
                    while not recordset.EOF:
                        records.append({ field.Name: TypeValueWrapper((field.Type, field.Value))
                                         for field in recordset.Fields })
                        recordset.MoveNext()
                finally:
                    recordset.Close()
                pmnc.log.info("<< OK, {0:d} record(s)".format(len(records)))
            else:
                pmnc.log.info("<< OK")

        except com_error:
            tb, code, state, description = self._extract_adodb_error()
            state_brace = " [{0:s}]".format(state) if state is not None else ""
            pmnc.log.warning("<< {0:s}{1:s} !! ADODB_Error(\"{2:d}:{3:s} {4:s}\") in {5:s}".\
                             format(sql, " -- ({0:s})".format(param_list) if param_list else "",
                                    code, state_brace, description, trace_string()))
            SQLResourceError.rethrow(
                    code = code, state = state, description = description,
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
                       "22", # data exception
                       "23", # integrity constraint violation
                       "44", # with check option violation
                   }

    ###################################

    def commit(self):
        try:
            self._connection.CommitTrans()
        except com_error:
            self._rethrow_adodb_error()

    ###################################

    def rollback(self):
        try:
            self._connection.RollbackTrans()
        except com_error:
            self._rethrow_adodb_error()

    ###################################

    def disconnect(self):
        try:
            try:
                self._connection.Close()
            except com_error:
                self._rethrow_adodb_error()
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            SQLResource.disconnect(self)

    ###################################

    def _extract_adodb_error(self):
        t, v, tb = exc_info()[:3]
        if self._connection:
            for error in self._connection.errors:
                if error.Number < 0 and error.SQLState[:2] != "00":
                    return tb, error.Number, error.SQLState.upper(), error.Description
        return tb, v.args[0], None, (v.args[2] or [None, None, v.args[1]])[2]

    ###################################

    def _rethrow_adodb_error(self, **kwargs):
        tb, code, state, description = self._extract_adodb_error()
        state_brace = " [{0:s}]".format(state) if state is not None else ""
        raise Exception("{0:d}:{1:s} {2:s}".format(code, state_brace, description)).with_traceback(tb)

    ###################################

    # this method takes "SELECT {foo} FROM {bar}", { "foo": 1, "bar": 2 }
    # and returns "SELECT ? FROM ?", (1, 2)
    # if a value is None its ? placeholder is replaced with literal NULL,
    # this is unfortunate because it may poison the ad-hoc prepared cache
    # but there seems no other way of passing a context-free NULL parameter

    def _convert_to_qmarks(self, sql, params):

        params = tuple(params.items()) # this fixes the order

        n2i = { n: "{0:s}{1:d}{0:s}".format(self._random_separator, i)
                for i, (n, v) in enumerate(params) }
        split_sql = sql.format(**n2i).split(self._random_separator)

        qmark_sql, qmark_params = split_sql[0], []
        for i in range(1, len(split_sql), 2):
            value = params[int(split_sql[i])][1]
            if value is not None:
                qmark_sql += "?" + split_sql[i+1]
                qmark_params.append(value)
            else:
                qmark_sql += "NULL" + split_sql[i+1]

        return qmark_sql, tuple(qmark_params)

    ###################################

    def _py_to_sql_NoneType(self, v):
        return v

    def _py_to_sql_int(self, v):
        if -2147483648 <= v <= 2147483647:
            return "adInteger", 4, v
        elif -9223372036854775808 <= v <= 9223372036854775807:
            return "adBigInt", 8, v
        else:
            raise ResourceError(description = "integer value too large",
                                recoverable = True, terminal = False)

    def _py_to_sql_Decimal(self, v):
        return "adDecimal", self._decimal_bytes, \
               self._ensure_decimal_in_range(v,
                    lambda description: ResourceError(description = description,
                                                      recoverable = True, terminal = False))

    def _py_to_sql_float(self, v):
        return "adDouble", 8, v

    def _py_to_sql_bool(self, v):
        return "adBoolean", 1, v

    def _py_to_sql_datetime(self, v):
        t = v.timetuple()
        dt = date(*t[:3]).toordinal() - self._18991231 + 1 + \
             ((((t[3] * 60 + t[4]) * 60) + t[5]) * 1000 + t[6]) / 86400000.0
        return "adDBTimeStamp", 8, dt

    def _py_to_sql_str(self, v):
        return "adVarWChar", (len(v) or 1), v

    def _py_to_sql_bytes(self, v):
        return "adVarBinary", (len(v) or 1), v

    ###################################

    def _sql_to_py_float(self, v):
        return v

    def _sql_to_py_TypeValueWrapper(self, v):
        if v[1] is None:
            return None
        ad_type_name = ADODataTypeNames.get(v[0], "adEmpty")
        return getattr(self, "_adodb_to_py_{0:s}".format(ad_type_name))(v[1])

    def _adodb_to_py_adBigInt(self, v):
        return int(v)

    _adodb_to_py_adInteger = _adodb_to_py_adSmallInt = \
    _adodb_to_py_adTinyInt = _adodb_to_py_adUnsignedBigInt = \
    _adodb_to_py_adUnsignedInt = _adodb_to_py_adUnsignedSmallInt = \
    _adodb_to_py_adUnsignedTinyInt = _adodb_to_py_adBigInt

    def _adodb_to_py_adBoolean(self, v):
        return bool(v)

    def _adodb_to_py_adEmpty(self, v):
        return None

    _adodb_to_py_adChapter = _adodb_to_py_adError = \
    _adodb_to_py_adFileTime = _adodb_to_py_adIDispatch = \
    _adodb_to_py_adIUnknown = _adodb_to_py_adPropVariant = \
    _adodb_to_py_adUserDefined = _adodb_to_py_adVariant = \
    _adodb_to_py_adVarNumeric = _adodb_to_py_adEmpty

    def _adodb_to_py_adVarChar(self, v):
        return str(v)

    _adodb_to_py_adBSTR = _adodb_to_py_adChar = \
    _adodb_to_py_adGUID = _adodb_to_py_adLongVarChar = \
    _adodb_to_py_adLongVarWChar = _adodb_to_py_adVarWChar = \
    _adodb_to_py_adWChar = _adodb_to_py_adVarChar

    def _adodb_to_py_adVarBinary(self, v):
        return bytes(v)

    _adodb_to_py_adBinary = adLongVarBinary = \
    _adodb_to_py_adLongVarBinary = _adodb_to_py_adVarBinary

    def _adodb_to_py_adDecimal(self, v):
        if isinstance(v, str): v = v.replace(",", ".")
        return self._ensure_decimal_in_range(Decimal(v),
                    lambda description: ResourceError(description = description,
                                                      recoverable = True, terminal = True))

    _adodb_to_py_adNumeric = _adodb_to_py_adCurrency = _adodb_to_py_adDecimal

    def _adodb_to_py_adDouble(self, v):
        return float(v)

    _adodb_to_py_adSingle = _adodb_to_py_adDouble

    def _adodb_to_py_adDBTimeStamp(self, v):
        return v.replace(tzinfo = None) + timedelta()

###############################################################################

def self_test():

    from expected import expected
    from functools import reduce
    from math import log10
    from pmnc.request import fake_request
    from random import shuffle
    from time import time

    ###################################

    # assuming test database has collation Cyrillic_General_CI_AS

    rus = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419" \
          "\u041a\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424" \
          "\u0425\u0426\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f" \
          "\u0430\u0431\u0432\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439" \
          "\u043a\u043b\u043c\u043d\u043e\u043f\u0440\u0441\u0442\u0443\u0444" \
          "\u0445\u0446\u0447\u0448\u0449\u044c\u044b\u044a\u044d\u044e\u044f"

    heb = "\u05d0\u05d1\u05d2\u05d3\u05d4\u05d5\u05d6\u05d7\u05d8\u05d9\u05da" \
          "\u05db\u05dc\u05dd\u05de\u05df\u05e0\u05e1\u05e2\u05e3\u05e4\u05e5" \
          "\u05e6\u05e7\u05e8\u05e9\u05ea\u05f0\u05f1\u05f2\u05f3\u05f4"

    bin = bytes(list(range(256)))

    ###################################

    def test_qmarks():

        db_config = pmnc.config_resource_sqlserver_1.copy()
        assert db_config.pop("protocol") == "sqlserver_adodb"
        r = Resource("test", **db_config)

        assert r._convert_to_qmarks("", {}) == ("", ())
        assert r._convert_to_qmarks("{i}", dict(i = 123)) == ("?", (123,))
        assert r._convert_to_qmarks("{i}", dict(i = None)) == ("NULL", ())
        assert r._convert_to_qmarks("{{i}}{i}{{i}}", dict(i = 123)) == ("{i}?{i}", (123,))
        assert r._convert_to_qmarks("{i}{i}", dict(i = 123)) == ("??", (123, 123))
        assert r._convert_to_qmarks("{i}{i}", dict(i = None)) == ("NULLNULL", ())
        assert r._convert_to_qmarks("{i}{{i}}{i}", dict(i = 123)) == ("?{i}?", (123, 123))
        assert r._convert_to_qmarks("{i}{{i{i}i}}{i}", dict(i = 123)) == ("?{i?i}?", (123, 123, 123))
        assert r._convert_to_qmarks("{i}{{i{i}i}}{i}", dict(i = None)) == ("NULL{iNULLi}NULL", ())

        assert r._convert_to_qmarks("", dict(i = 123, s = "foo")) == ("", ())
        assert r._convert_to_qmarks("{i}", dict(i = 123, s = "foo")) == ("?", (123,))
        assert r._convert_to_qmarks("{s}", dict(i = 123, s = "foo")) == ("?", ("foo",))
        assert r._convert_to_qmarks("{i}{s}", dict(i = 123, s = "foo")) == ("??", (123, "foo"))
        assert r._convert_to_qmarks("{s}{i}", dict(i = 123, s = "foo")) == ("??", ("foo", 123))
        assert r._convert_to_qmarks("{i}{s}{i}", dict(i = 123, s = "foo")) == ("???", (123, "foo", 123))
        assert r._convert_to_qmarks("{s}{i}{s}", dict(i = 123, s = "foo")) == ("???", ("foo", 123, "foo"))
        assert r._convert_to_qmarks("|{i}|{s}|{c}|", dict(i = 123, s = "foo", c = b"")) == ("|?|?|?|", (123, "foo", b""))
        assert r._convert_to_qmarks("|{i}|{s}|{c}|", dict(i = None, s = "foo", c = b"")) == ("|NULL|?|?|", ("foo", b""))
        assert r._convert_to_qmarks("|{i}|{s}|{c}|", dict(i = 123, s = None, c = b"")) == ("|?|NULL|?|", (123, b""))
        assert r._convert_to_qmarks("|{i}|{s}|{c}|", dict(i = 123, s = "foo", c = None)) == ("|?|?|NULL|", (123, "foo"))

        assert r._convert_to_qmarks("? {x} ? {y} ? {z} ? " * 10000, dict(x = None, y = 123, z = "foo")) == \
                                   ("? NULL ? ? ? ? ? " * 10000, (123, "foo") * 10000)

        with expected(KeyError("missing")):
            r._convert_to_qmarks("{missing}", {})

        with expected(ValueError("Unknown ")):
            r._convert_to_qmarks("{i:d}", dict(i = 123))

    test_qmarks()

    ###################################

    def random_name(prefix):
        return "{0:s}_{1:s}".format(prefix, b2a_hex(urandom(4)).decode("ascii"))

    ###################################

    def exec_res(f, *args, **kwargs):
        db_config = pmnc.config_resource_sqlserver_1.copy()
        assert db_config.pop("protocol") == "sqlserver_adodb"
        r = Resource("test", **db_config)
        r.connect()
        try:
            r.begin_transaction("xid", source_module_name = "", transaction_options = {},
                                resource_args = (), resource_kwargs = {})
            try:
                result = f(r, *args, **kwargs)
            except:
                r.rollback()
                raise
            else:
                r.commit()
        finally:
            r.disconnect()
        return result

    ###################################

    def get_version():

        def execute(r, *args, **kwargs):
            return r.execute(*args, **kwargs)

        product_version = exec_res(execute,
            "SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(128)) "
            "AS version")[0][0]["version"]

        return int(product_version.split(".")[0])

    version = get_version()

    ###################################

    def test_data_types():

        def test_data_type(r, type_name, values):
            table_name = random_name("t")
            qs = [ "CREATE TABLE {0:s} (value {1:s})".format(table_name, type_name) ]
            params = {}
            for i, value in enumerate(values):
                param_name = "p_{0:02d}".format(i)
                params[param_name] = value
                qs.append("INSERT INTO {0:s} (value) VALUES ({{{1:s}}})".format(table_name, param_name))
            qs.append("SELECT value FROM {0:s}".format(table_name))
            qs.append("DROP TABLE {0:s}".format(table_name))
            results = list(r.execute(*qs, **params))
            retvalues = results[-2]; del results[-2]
            assert results == [[]] * len(results)
            return [ retvalue["value"] for retvalue in retvalues ]

        # exact numerics

        assert exec_res(test_data_type, "bit", [ None, True, False ]) == [ None, True, False ]
        assert exec_res(test_data_type, "int", [ None, -2**31, 0, 2**31-1 ]) == [ None, -2**31, 0, 2**31-1 ]
        assert exec_res(test_data_type, "bigint", [ None, -2**63, -2**31, 0, 2**31-1, 2**63-1 ]) == [ None, -2**63, -2**31, 0, 2**31-1, 2**63-1 ]
        assert exec_res(test_data_type, "smallint", [ None, -2**15, 0, 2**15-1 ]) == [ None, -2**15, 0, 2**15-1 ]
        assert exec_res(test_data_type, "tinyint", [ None, 0, 255 ]) == [ None, 0, 255 ]
        #assert exec_res(test_data_type, "numeric(4, 2)", [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]) == [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]
        #assert exec_res(test_data_type, "numeric(10, 2)", [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]) == [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]
        #assert exec_res(test_data_type, "numeric(38, 16)", [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]) == [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]
        #assert exec_res(test_data_type, "decimal(4, 2)", [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]) == [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]
        #assert exec_res(test_data_type, "decimal(10, 2)", [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]) == [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]
        #assert exec_res(test_data_type, "decimal(38, 16)", [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]) == [ None, Decimal("0.0"), Decimal("0.01"), Decimal("99.99"), Decimal("-0.01"), Decimal("-99.99") ]
        #assert exec_res(test_data_type, "money", [ None, Decimal("0.0"), Decimal("-922337203685477.5808"), Decimal("922337203685477.5807") ]) == [ None, Decimal("0.0"), Decimal("-922337203685477.5808"), Decimal("922337203685477.5807") ]
        #assert exec_res(test_data_type, "smallmoney", [ None, Decimal("0.0"), Decimal("-214748.3648"), Decimal("214748.3647") ]) == [ None, Decimal("0.0"), Decimal("-214748.3648"), Decimal("214748.3647") ]

        # approximate numerics

        def loose_compare(a, b):
            assert len(a) == len(b)
            for aa, bb in zip(a, b):
                if aa is None and bb is None:
                    continue
                if abs(aa) < 1e-6 and abs(bb) < 1e-6:
                    continue
                if (aa < 0.0 and bb >= 0.0) or (bb < 0.0 and aa >= 0.0):
                    return False
                if abs(log10(abs(aa)) - log10(abs(bb))) > 1e-6:
                    return False
            else:
                return True

        assert loose_compare(exec_res(test_data_type, "float(53)", [ None, 0.0, -1.79e308, -2.23e-308, 2.23e-308, 1.79e+308 ]), [ None, 0.0, -1.79e308, -2.23e-308, 2.23e-308, 1.79e+308 ])
        assert loose_compare(exec_res(test_data_type, "float(24)", [ None, 0.0, -3.40e+38, -1.18e-38, 1.18e-38, 3.40e+38 ]), [ None, 0.0, -3.40e+38, -1.18e-38, 1.18e-38, 3.40e+38 ])
        assert loose_compare(exec_res(test_data_type, "real", [ None, 0.0, -3.40e+38, -1.18e-38, 1.18e-38, 3.40e+38 ]), [ None, 0.0, -3.40e+38, -1.18e-38, 1.18e-38, 3.40e+38 ])

        # date and time

        now = datetime.now().replace(microsecond = 0)
        now_min = datetime(now.year, now.month, now.day, now.hour, now.minute + ((now.second >= 30) and 1 or 0))
        assert exec_res(test_data_type, "datetime", [ None, now, datetime(1753, 1, 1), datetime(9999, 12, 31, 23, 59, 59) ]) == [ None, now, datetime(1753, 1, 1), datetime(9999, 12, 31, 23, 59, 59) ]
        assert exec_res(test_data_type, "smalldatetime", [ None, now, datetime(1900, 1, 1), datetime(2079, 6, 6, 23, 59, 29), datetime(2079, 6, 6, 23, 58, 30) ]) == [ None, now_min, datetime(1900, 1, 1), datetime(2079, 6, 6, 23, 59), datetime(2079, 6, 6, 23, 59) ]
        if version >= 10:
            assert exec_res(test_data_type, "date", [ None, datetime(1753, 1, 1), datetime(9999, 12, 31) ]) == [ None, "1753-01-01", "9999-12-31" ]
            assert exec_res(test_data_type, "time", [ None, datetime(1753, 1, 1, 0, 0, 0), datetime(9999, 12, 31, 23, 59, 59) ]) == [ None, "00:00:00.0000000", "23:59:59.0000000" ]
            assert exec_res(test_data_type, "datetimeoffset", [ None, datetime(1753, 1, 1, 0, 0, 0), datetime(9999, 12, 31, 23, 59, 59) ]) == [ None, "1753-01-01 00:00:00.0000000 +00:00", "9999-12-31 23:59:59.0000000 +00:00" ]
            assert exec_res(test_data_type, "datetime2", [ None, datetime(1753, 1, 1, 0, 0, 0), datetime(9999, 12, 31, 23, 59, 59) ]) == [ None, "1753-01-01 00:00:00.0000000", "9999-12-31 23:59:59.0000000" ]

        # character strings

        assert exec_res(test_data_type, "char(1)", [ None, "", "x", rus[0] ]) == [ None, " ", "x", rus[0] ]
        assert exec_res(test_data_type, "char(66)", [ None, "", rus ]) == [ None, " " * 66, rus ]
        assert exec_res(test_data_type, "varchar(66)", [ None, "", "x", "x" * 66, rus, heb ]) == [ None, "", "x", "x" * 66, rus, "?" * len(heb) ]
        assert exec_res(test_data_type, "text", [ None, "", rus, "x" * 16384, heb ]) == [ None, "", rus, "x" * 16384, "?" * len(heb) ]

        # unicode character strings

        assert exec_res(test_data_type, "nchar", [ None, "", "x", rus[0] ]) == [ None, " ", "x", rus[0] ]
        assert exec_res(test_data_type, "nchar(66)", [ None, "", rus ]) == [ None, " " * 66, rus ]
        assert exec_res(test_data_type, "nvarchar(66)", [ None, "", "x", "x" * 66, rus, heb ]) == [ None, "", "x", "x" * 66, rus, heb ]
        assert exec_res(test_data_type, "ntext", [ None, "", rus, "x" * 16384, heb ]) == [ None, "", rus, "x" * 16384, heb ]

        # binary strings

        assert exec_res(test_data_type, "binary(1)", [ None, b"" ]) == [ None, b"\x00" ]
        assert exec_res(test_data_type, "binary(256)", [ None, b"", bin ]) == [ None, b"\x00" * 256, bin ]
        assert exec_res(test_data_type, "varbinary(64)", [ None, b"", bin[:64] ]) == [ None, b"", bin[:64] ]
        assert exec_res(test_data_type, "image", [ None, b"", bin ]) == [ None, b"", bin ]

    test_data_types()

    ###################################

    def test_data_types_errors():

        def test_data_type_errors(r, type_name, values):
            for value in values:
                try:
                    table_name = random_name("t")
                    retvalue = r.execute("CREATE TABLE {0:s} (value {1:s})".format(table_name, type_name),
                                         "INSERT INTO {0:s} (value) VALUES ({{value}})".format(table_name),
                                         "SELECT value FROM {0:s}".format(table_name),
                                         value = value)[2][0]["value"]
                except SQLResourceError as e:
                    assert e.recoverable and ((not e.terminal and e.state[:2] == "22") or
                                              (e.terminal and e.state[:2] == "42"))
                except ResourceError as e:
                    pass
                else:
                    assert False, "{0:s}({1:s})".format(retvalue.__class__.__name__,
                                                        isinstance(retvalue, str) and
                                                        "'{0:s}'".format(retvalue) or str(retvalue))

        exec_res(test_data_type_errors, "int", [ 2**31, -2**31-1, "foo" ])
        exec_res(test_data_type_errors, "bigint", [ 2**63, -2**63-1, "foo" ])
        exec_res(test_data_type_errors, "smallint", [ 2**31, -2**31-1, "foo" ])
        exec_res(test_data_type_errors, "tinyint", [ -1, 256, "foo" ])
        exec_res(test_data_type_errors, "numeric(2, 1)", [ Decimal("12.3"), "0,0", "foo" ])
        exec_res(test_data_type_errors, "decimal(2, 1)", [ Decimal("12.3"), "0,0", "foo" ])
        exec_res(test_data_type_errors, "money", [ Decimal("-922337203685477.5809"), Decimal("922337203685477.5808"), "foo" ])
        exec_res(test_data_type_errors, "smallmoney", [ Decimal("-214748.3649"), Decimal("214748.3648"), "foo" ])
        exec_res(test_data_type_errors, "bit", [ "foo" ])

        exec_res(test_data_type_errors, "float(53)", [ -1.80e308, 1.80e+308, "0,0", "foo" ])
        exec_res(test_data_type_errors, "float(24)", [ -3.41e+38, 3.41e+38, "0,0", "foo" ])
        exec_res(test_data_type_errors, "real", [ -3.41e+38, 3.41e+38, "0,0", "foo" ])

        exec_res(test_data_type_errors, "datetime", [ datetime(1752, 12, 31), "foo" ])
        exec_res(test_data_type_errors, "smalldatetime", [ datetime(1899, 12, 31, 23, 59, 29), datetime(2079, 6, 7), "foo" ])
        exec_res(test_data_type_errors, "date", [ datetime(1752, 12, 31), "foo" ])
        exec_res(test_data_type_errors, "datetimeoffset", [ datetime(1752, 12, 31), "foo" ])
        exec_res(test_data_type_errors, "datetime2", [ datetime(1752, 12, 31), "foo" ])

        exec_res(test_data_type_errors, "text", [ bin ])
        exec_res(test_data_type_errors, "ntext", [ bin ])
        exec_res(test_data_type_errors, "binary(66)", [ rus ])
        exec_res(test_data_type_errors, "varbinary(66)", [ rus ])
        exec_res(test_data_type_errors, "image", [ rus ])

    test_data_types_errors()

    ###################################

    def test_resource_success():

        fake_request(10.0)

        rs = pmnc.transaction.sqlserver_1.execute("SELECT 1 AS C")[0]

        assert len(rs) == 1 and rs[0]["c"] == 1

    test_resource_success()

    ###################################

    def test_resource_failure():

        fake_request(10.0)

        try:
            pmnc.transaction.sqlserver_1.execute("SYNTAX ERROR")
        except SQLResourceError as e:
            assert e.state == "42000"
        else:
            assert False

    test_resource_failure()

    ###################################

    def test_performance():

        fake_request(30.0)

        N = 250

        table_name = random_name("t")
        qs = [ "CREATE TABLE {0:s} (id integer PRIMARY KEY)".format(table_name) ]
        ids = list(range(N)); shuffle(ids); ii = {}
        for i, id in enumerate(ids):
            n = format("id_{0:d}".format(i))
            ii[n] = id
            qs.append("INSERT INTO {0:s} (id) VALUES ({{{1:s}}})".format(table_name, n))
        qs.append("SELECT id FROM {0:s} ORDER BY newid()".format(table_name))
        qs.append("DROP TABLE {0:s}".format(table_name))

        start = time()
        rs = pmnc.transaction.sqlserver_1.execute(*qs, **ii)[-2]
        ids = list(map(lambda r: r["id"], rs))
        stop = time()

        assert sorted(ids) == list(range(N))
        pmnc.log("performance: {0:d} insert(s)/sec".format(int(N / (stop - start))))

    pmnc._loader.set_log_level("LOG")
    try:
        test_performance()
    finally:
        pmnc._loader.set_log_level("DEBUG")

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF