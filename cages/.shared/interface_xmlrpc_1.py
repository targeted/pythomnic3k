#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This is a sample request processing module for the sample xmlrpc interface
# (xmlrpc_1), it is exactly this file (or its copy) that you need to edit for
# your own application-specific processing.
#
# Specifically for xmlrpc, add methods below and decorate them with
# @xmlrpc_method decorator to ensure they are accessible via xmlrpc.
# See examples below.
#
###############################################################################

__all__ = [ "process_request" ]

###############################################################################
# imports section

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import typecheck

###############################################################################

xmlrpc_methods = {}
xmlrpc_method = lambda f: xmlrpc_methods.update({f.__name__: typecheck(f)})

###############################################################################

# XMLRPC-accessible methods:

@xmlrpc_method
def Ping(x: str) -> str:
    return x

@xmlrpc_method
def Math__Sum(*args):
    return sum(args)

# todo: add more methods here

###############################################################################
# request processing method, no need to edit it, just add more methods above

def process_request(request: dict, response: dict):

    # request contains "method" and "args"
    # response already contains default "result" of None

    method = request["method"].replace(".", "__")

    try:
        xmlrpc_method = xmlrpc_methods[method]
    except KeyError:
        raise Exception("method is not supported")

    response["result"] = xmlrpc_method(*request["args"])

###############################################################################

def self_test():

    # the following tests simulate what the interface does when invoking
    # process_request to test this module without launching interface

    from expected import expected
    from typecheck import InputParameterError

    ###################################

    def _xmlrpc_call(method, *args):

        request = dict(method = method, args = args)
        response = dict(result = None)

        pmnc.__getattr__(__name__).process_request(request, response)

        return response["result"]

    ###################################

    def test_ping():

        assert _xmlrpc_call("Ping", "123") == "123"

    test_ping()

    ###################################

    def test_invalid_arg():

        with expected(InputParameterError("Ping() has got an incompatible value for x: 123")):
            _xmlrpc_call("Ping", 123)

    test_invalid_arg()

    ###################################

    def test_no_direct_call():

        with expected(TypeError("'NoneType' object is not callable")):
            Ping("123")

    test_no_direct_call()

    ###################################

    def test_dotted_name():

        assert _xmlrpc_call("Math.Sum", 1, 2, 3) == 6

    test_dotted_name()

    ###################################

    def test_invalid_method():

        with expected(Exception("method is not supported")):
            _xmlrpc_call("Foo")

    test_invalid_method()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF