#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This is a sample request processing module for the sample smpp interface
# (smpp_1), it is exactly this file (or its copy) that you need to edit for
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

import smpp34.smpp_errors; from smpp34.smpp_errors import error_codes
import smpp34.smpp_pdus; from smpp34.smpp_pdus import *

###############################################################################
# request processing method, this is an entry point to your application

def process_request(request: dict, response: dict):

    # request contains "pdu" of type RequestPDU, response contains "pdu"
    # of type ResponsePDU, a GenericNack by default, but it should be
    # replaced by your application code

    req = request["pdu"]
    if isinstance(req, QuerySmPDU):
        response["pdu"] = req.create_response(message_id = req.message_id.value,
                                              final_date = b"",
                                              message_state = 0x03,
                                              error_code = 0x00)

###############################################################################

def self_test():

    # the following tests simulate what the interface does when invoking
    # process_request to test this module without launching interface

    from pmnc.request import fake_request

    ###################################

    def test_process_request():

        fake_request(10.0)

        req_pdu = QuerySmPDU.create(message_id = b"RECEIPT",
                                    source_addr_ton = 0x00,
                                    source_addr_npi = 0x01,
                                    source_addr = b"SENDER")

        req = dict(pdu = req_pdu)
        resp = dict(pdu = req_pdu.create_nack(error_codes.ESME_RUNKNOWNERR))

        pmnc.__getattr__(__name__).process_request(req, resp)

        resp_pdu = resp["pdu"]
        assert isinstance(resp_pdu, QuerySmRespPDU)

    test_process_request()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF