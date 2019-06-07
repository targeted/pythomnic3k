#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This is a sample request processing module for the sample file interface
# (file_1), it is exactly this file (or its copy) that you need to edit for
# your own application-specific processing.
#
###############################################################################

__all__ = [ "process_request" ]

###############################################################################
# imports section

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

###############################################################################
# request processing method, this is an entry point to your application

def process_request(request: dict, response: dict):

    # request contains "file_name" of type str and contains full name of the
    # file to be processed, there is no need to remove the file afterwards;
    # response is and should remain empty

    file_name = request["file_name"]

    with open(file_name, "rb") as f:
        data = f.read()

    pmnc.log("processing {0:s}: {1:s}".format(file_name, data.decode("ascii")))

###############################################################################

def self_test():

    # the following tests simulate what the interface does when invoking
    # process_request to test this module without launching interface

    from os import path as os_path, urandom, remove
    from binascii import b2a_hex

    ###################################

    def test_process_request():

        file_name = os_path.join("/tmp", b2a_hex(urandom(4)).decode("ascii") + ".msg")
        with open(file_name, "wb") as f:
            f.write(b"TEST DATA")

        request = dict(file_name = file_name)
        response = dict()
        pmnc.__getattr__(__name__).process_request(request, response)
        assert not response

        assert os_path.isfile(file_name)
        remove(file_name)

    test_process_request()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF