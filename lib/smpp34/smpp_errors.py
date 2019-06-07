#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Module smpp_errors. Contains exception classes.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
#
###############################################################################

__all__ = [ "SMPPError", "SMPPTypeReadError", "SMPPParameterError",
            "SMPPPDUReadError", "SMPPPDUCreateError", "SMPPConnectionError",
            "SMPPResponseError", "error_codes" ]

###############################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import smpp34.smpp_tools; from smpp34.smpp_tools import *

###############################################################################

class SMPPError(Exception): pass
class SMPPTypeReadError(SMPPError): pass
class SMPPParameterError(SMPPError): pass
class SMPPPDUReadError(SMPPError): pass
class SMPPPDUCreateError(SMPPError): pass
class SMPPConnectionError(SMPPError): pass
class SMPPResponseError(SMPPError): pass

###############################################################################

error_code_table = \
{
0x00000000: { "name": "ESME_ROK", "description": "No Error" },
0x00000001: { "name": "ESME_RINVMSGLEN", "description": "Message Length is invalid" },
0x00000002: { "name": "ESME_RINVCMDLEN", "description": "Command Length is invalid" },
0x00000003: { "name": "ESME_RINVCMDID", "description": "Invalid Command ID" },
0x00000004: { "name": "ESME_RINVBNDSTS", "description": "Incorrect BIND Status for given command" },
0x00000005: { "name": "ESME_RALYBND", "description": "ESME Already in Bound State" },
0x00000006: { "name": "ESME_RINVPRTFLG", "description": "Invalid Priority Flag" },
0x00000007: { "name": "ESME_RINVREGDLVFLG", "description": "Invalid Registered Delivery Flag" },
0x00000008: { "name": "ESME_RSYSERR", "description": "System Error" },
0x0000000A: { "name": "ESME_RINVSRCADR", "description": "Invalid Source Address" },
0x0000000B: { "name": "ESME_RINVDSTADR", "description": "Invalid Dest Addr" },
0x0000000C: { "name": "ESME_RINVMSGID", "description": "Message ID is invalid" },
0x0000000D: { "name": "ESME_RBINDFAIL", "description": "Bind Failed" },
0x0000000E: { "name": "ESME_RINVPASWD", "description": "Invalid Password" },
0x0000000F: { "name": "ESME_RINVSYSID", "description": "Invalid System ID" },
0x00000011: { "name": "ESME_RCANCELFAIL", "description": "Cancel SM Failed" },
0x00000013: { "name": "ESME_RREPLACEFAIL", "description": "Replace SM Failed" },
0x00000014: { "name": "ESME_RMSGQFUL", "description": "Message Queue Full" },
0x00000015: { "name": "ESME_RINVSERTYP", "description": "Invalid Service Type" },
0x00000033: { "name": "ESME_RINVNUMDESTS", "description": "Invalid number of destinations" },
0x00000034: { "name": "ESME_RINVDLNAME", "description": "Invalid Distribution List name" },
0x00000040: { "name": "ESME_RINVDESTFLAG", "description": "Destination flag is invalid (submit_multi)" },
0x00000042: { "name": "ESME_RINVSUBREP", "description": "Invalid ‘submit with replace’ request (i.e. submit_sm with replace_if_present_flag set)" },
0x00000043: { "name": "ESME_RINVESMCLASS", "description": "Invalid esm_class field data" },
0x00000044: { "name": "ESME_RCNTSUBDL", "description": "Cannot Submit to Distribution List" },
0x00000045: { "name": "ESME_RSUBMITFAIL", "description": "submit_sm or submit_multi failed" },
0x00000048: { "name": "ESME_RINVSRCTON", "description": "Invalid Source address TON" },
0x00000049: { "name": "ESME_RINVSRCNPI", "description": "Invalid Source address NPI" },
0x00000050: { "name": "ESME_RINVDSTTON", "description": "Invalid Destination address TON" },
0x00000051: { "name": "ESME_RINVDSTNPI", "description": "Invalid Destination address NPI" },
0x00000053: { "name": "ESME_RINVSYSTYP", "description": "Invalid system_type field" },
0x00000054: { "name": "ESME_RINVREPFLAG", "description": "Invalid replace_if_present flag" },
0x00000055: { "name": "ESME_RINVNUMMSGS", "description": "Invalid number of messages" },
0x00000058: { "name": "ESME_RTHROTTLED", "description": "Throttling error (ESME has exceeded allowed message limits)" },
0x00000061: { "name": "ESME_RINVSCHED", "description": "Invalid Scheduled Delivery Time" },
0x00000062: { "name": "ESME_RINVEXPIRY", "description": "Invalid message validity period (Expiry time)" },
0x00000063: { "name": "ESME_RINVDFTMSGID", "description": "Predefined Message Invalid or Not Found" },
0x00000064: { "name": "ESME_RX_T_APPN", "description": "ESME Receiver Temporary App Error Code" },
0x00000065: { "name": "ESME_RX_P_APPN", "description": "ESME Receiver Permanent App Error Code" },
0x00000066: { "name": "ESME_RX_R_APPN", "description": "ESME Receiver Reject Message Error Code" },
0x00000067: { "name": "ESME_RQUERYFAIL", "description": "query_sm request failed" },
0x000000C0: { "name": "ESME_RINVOPTPARSTREAM", "description": "Error in the optional part of the PDU Body" },
0x000000C1: { "name": "ESME_ROPTPARNOTALLWD", "description": "Optional Parameter not allowed" },
0x000000C2: { "name": "ESME_RINVPARLEN", "description": "Invalid Parameter Length" },
0x000000C3: { "name": "ESME_RMISSINGOPTPARAM", "description": "Expected Optional Parameter missing" },
0x000000C4: { "name": "ESME_RINVOPTPARAMVAL", "description": "Invalid Optional Parameter Value" },
0x000000FE: { "name": "ESME_RDELIVERYFAILURE", "description": "Delivery Failure (used for data_sm_resp)" },
0x000000FF: { "name": "ESME_RUNKNOWNERR", "description": "Unknown Error" },
}

error_codes = enum(error_code_table)

###############################################################################

if __name__ == "__main__":

    print("self-testing module smpp_errors.py:")

    from expected import expected

    with expected(SMPPError("foo")):
        raise SMPPError("foo")

    with expected(SMPPTypeReadError("foo")):
        raise SMPPTypeReadError("foo")

    with expected(SMPPParameterError("foo")):
        raise SMPPParameterError("foo")

    with expected(SMPPPDUReadError("foo")):
        raise SMPPPDUReadError("foo")

    with expected(SMPPPDUCreateError("foo")):
        raise SMPPPDUCreateError("foo")

    with expected(SMPPConnectionError("foo")):
        raise SMPPConnectionError("foo")

    with expected(SMPPResponseError("foo")):
        raise SMPPResponseError("foo")

    assert error_codes.ESME_RQUERYFAIL == 0x00000067
    assert error_codes[0x00000067]["description"] == "query_sm request failed"

    print("ok")

###############################################################################
# EOF
