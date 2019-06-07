#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This is a sample request processing module for the sample e-mail interface
# (email_1), it is exactly this file (or its copy) that you need to edit for
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

    # request contains "message_id" of type str and "message" of type
    # email.message.Message, response is and should remain empty

    message_id = request["message_id"]
    message = request["message"]
    subject = message["Subject"]

###############################################################################

def self_test():

    # the following tests simulate what the interface does when invoking
    # process_request to test this module without launching interface

    from email.message import Message

    ###################################

    def test_process_request():

        request = dict(message_id = "message_id", message = Message())
        response = dict()

        pmnc.__getattr__(__name__).process_request(request, response)

        assert not response

    test_process_request()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF