#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This is a sample request processing module for the sample http interface
# (http_1), it is exactly this file (or its copy) that you need to edit for
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

    # request contains "method", "url", "headers" and "content"
    # response already contains default "status_code", "headers" and "content"
    # response content should be str (with content-type "text/*") or bytes otherwise

    url = request["url"]
    response["headers"]["content-type"] = "text/html"
    response["content"] = "<html><head><title>Pythomnic3k HTTP server</title></head>" \
                          "<body>Pythomnic3k HTTP server is up and running:<br/>URL: {0:s}</body>" \
                          "</html>".format(url)

###############################################################################

def self_test():

    # the following tests simulate what the interface does when invoking
    # process_request to test this module without launching interface

    ###################################

    def test_process_request():

        request = dict(method = "GET", url = "/", headers = { "host": "127.0.0.1" }, content = b"")
        response = dict(status_code = 200, headers = { "content-type": "application/octet-stream" }, content = b"")

        pmnc.__getattr__(__name__).process_request(request, response)

        assert "up and running" in response["content"]

    test_process_request()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF