#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Module smpp_pdu_param_table. Contains reference tables of both mandatory
# and optional PDU parameters defined by the specification.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
#
###############################################################################

__all__ = [ "mandatory_pdu_parameters", "optional_pdu_parameters_by_code",
            "optional_pdu_parameters" ]

###############################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import smpp34.smpp_tools; from smpp34.smpp_tools import *
import smpp34.smpp_pdu_param; from smpp34.smpp_pdu_param import MandatoryPDUParameter as _MP, OptionalPDUParameter as _OP
import smpp34.smpp_types; from smpp34.smpp_types import *

###############################################################################
# mandatory parameters are listed under 5.2 plus some extra in 4.x

_ton_values = (0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06) # 5.2.5
_npi_values = (0x00, 0x01, 0x03, 0x04, 0x06, 0x08, 0x09, 0x0a, 0x0e, 0x12) # 5.2.6

mandatory_pdu_parameters_table = \
{
_MP( name = "addr_npi",                type = SMPPInteger1,       check = lambda v: v.value in _npi_values                   ), # 5.2.6
_MP( name = "addr_ton",                type = SMPPInteger1,       check = lambda v: v.value in _ton_values                   ), # 5.2.5
_MP( name = "address_range",           type = SMPPCOctetString,   check = lambda v: len(v) < 41                              ), # 5.2.7
_MP( name = "data_coding",             type = SMPPInteger1                                                                   ), # 5.2.19
_MP( name = "dest_addr_npi",           type = SMPPInteger1,       check = lambda v: v.value in _npi_values                   ), # 5.2.6
_MP( name = "dest_addr_ton",           type = SMPPInteger1,       check = lambda v: v.value in _ton_values                   ), # 5.2.5
_MP( name = "dest_addresses",          type = SMPPDestAddresses,  check = lambda v: len(v) > 0                               ), # 4.5.1 *** WARNING *** spec says dest_address(es)
_MP( name = "destination_addr",        type = SMPPCOctetString,   check = lambda v: len(v) < 21                              ), # 5.2.9
_MP( name = "error_code",              type = SMPPInteger1                                                                   ), # 4.8.2
_MP( name = "esm_class",               type = SMPPInteger1                                                                   ), # 5.2.12
_MP( name = "esme_addr",               type = SMPPCOctetString,   check = lambda v: len(v) < 65                              ), # 5.2.10
_MP( name = "esme_addr_npi",           type = SMPPInteger1,       check = lambda v: v.value in _npi_values                   ), # 5.2.6
_MP( name = "esme_addr_ton",           type = SMPPInteger1,       check = lambda v: v.value in _ton_values                   ), # 5.2.5
_MP( name = "final_date",              type = SMPPCOctetString,   check = lambda v: len(v) == 0 or valid_smpp_time(v.value)  ), # 4.8.2
_MP( name = "interface_version",       type = SMPPInteger1,       check = lambda v: 0 <= v.value <= 0x34                     ), # 5.2.4
_MP( name = "message_id",              type = SMPPCOctetString,   check = lambda v: len(v) < 65                              ), # 5.2.23
_MP( name = "message_id_null",         type = SMPPCOctetString,   check = lambda v: len(v) < 1                               ), # 5.2.23 + 4.6.2
_MP( name = "message_state",           type = SMPPInteger1,       check = lambda v: 1 <= v.value <= 8                        ), # 5.2.28
_MP( name = "no_unsuccess",            type = SMPPInteger1,       check = lambda v: 1 <= v.value <= 254                      ), # 5.2.26
_MP( name = "number_of_dests",         type = SMPPInteger1,       check = lambda v: 1 <= v.value <= 254                      ), # 5.2.24
_MP( name = "password",                type = SMPPCOctetString,   check = lambda v: len(v) < 9                               ), # 5.2.2
_MP( name = "priority_flag",           type = SMPPInteger1,       check = lambda v: 0 <= v.value <= 0x03                     ), # 5.2.14
_MP( name = "protocol_id",             type = SMPPInteger1                                                                   ), # 5.2.13
_MP( name = "registered_delivery",     type = SMPPInteger1,       check = lambda v: v.value & 0xe0 == 0                      ), # 5.2.17
_MP( name = "replace_if_present_flag", type = SMPPInteger1,       check = lambda v: 0 <= v.value <= 1                        ), # 5.2.18
_MP( name = "schedule_delivery_time",  type = SMPPCOctetString,   check = lambda v: len(v) == 0 or valid_smpp_time(v.value)  ), # 5.2.15
_MP( name = "service_type",            type = SMPPCOctetString,   check = lambda v: len(v) < 6                               ), # 5.2.11
_MP( name = "short_message",           type = SMPPOctetString,    check = lambda v: len(v) <= 254                            ), # 5.2.22
_MP( name = "sm_default_msg_id",       type = SMPPInteger1,                                                                  ), # 5.2.20 *** WARNING *** spec says values 0 and 255 are "reserved"
_MP( name = "sm_length",               type = SMPPInteger1,       check = lambda v: 0 <= v.value <= 254                      ), # 5.2.21
_MP( name = "source_addr",             type = SMPPCOctetString,   check = lambda v: len(v) < 21                              ), # 5.2.8
_MP( name = "source_addr_alert",       type = SMPPCOctetString,   check = lambda v: len(v) < 65                              ), # 5.2.8 + 4.12.1
_MP( name = "source_addr_npi",         type = SMPPInteger1,       check = lambda v: v.value in _npi_values                   ), # 5.2.6
_MP( name = "source_addr_ton",         type = SMPPInteger1,       check = lambda v: v.value in _ton_values                   ), # 5.2.5
_MP( name = "system_id",               type = SMPPCOctetString,   check = lambda v: len(v) < 16                              ), # 5.2.1
_MP( name = "system_type",             type = SMPPCOctetString,   check = lambda v: len(v) < 13                              ), # 5.2.3
_MP( name = "unsuccess_smes",          type = SMPPUnsuccessSmes,  check = lambda v: len(v) > 0                               ), # 4.5.2 # *** WARNING *** spec says unsuccess_sme(s)
_MP( name = "validity_period",         type = SMPPCOctetString,   check = lambda v: len(v) == 0 or valid_smpp_time(v.value)  ), # 5.2.16
}

mandatory_pdu_parameters = { mp._name: mp for mp in mandatory_pdu_parameters_table }

###############################################################################
# optional parameters are listed under 5.3.2

optional_pdu_parameters_table = \
{
_OP(   code = 0x0005,    name = "dest_addr_subunit",           type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 4      ), # 5.3.2.1
_OP(   code = 0x0006,    name = "dest_network_type",           type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 8      ), # 5.3.2.3
_OP(   code = 0x0007,    name = "dest_bearer_type",            type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 8      ), # 5.3.2.5
_OP(   code = 0x0008,    name = "dest_telematics_id",          type = SMPPInteger2                                                ), # 5.3.2.7  *** WARNING *** spec says source_telematics_id is 1 byte
_OP(   code = 0x000D,    name = "source_addr_subunit",         type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 4      ), # 5.3.2.2
_OP(   code = 0x000E,    name = "source_network_type",         type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 8      ), # 5.3.2.4
_OP(   code = 0x000F,    name = "source_bearer_type",          type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 8      ), # 5.3.2.6
_OP(   code = 0x0010,    name = "source_telematics_id",        type = SMPPInteger1                                                ), # 5.3.2.8  *** WARNING *** spec says dest_telematics_id is 2 bytes
_OP(   code = 0x0017,    name = "qos_time_to_live",            type = SMPPInteger4                                                ), # 5.3.2.9
_OP(   code = 0x0019,    name = "payload_type",                type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 1      ), # 5.3.2.10
_OP(   code = 0x001D,    name = "additional_status_info_text", type = SMPPCOctetString,  check = lambda v: len(v) < 256           ), # 5.3.2.11 (1-256) *** WARNING *** spec says C-octet string
_OP(   code = 0x001E,    name = "receipted_message_id",        type = SMPPCOctetString,  check = lambda v: len(v) < 65            ), # 5.3.2.12 (1-65)  *** WARNING *** spec says C-octet string
_OP(   code = 0x0030,    name = "ms_msg_wait_facilities",      type = SMPPInteger1,      check = lambda v: v.value & 0x7c == 0    ), # 5.3.2.13
_OP(   code = 0x0201,    name = "privacy_indicator",           type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 3      ), # 5.3.2.14
_OP(   code = 0x0202,    name = "source_subaddress",           type = SMPPOctetString,   check = lambda v: 2 <= len(v) <= 23      ), # 5.3.2.15 (2-23)
_OP(   code = 0x0203,    name = "dest_subaddress",             type = SMPPOctetString,   check = lambda v: 2 <= len(v) <= 23      ), # 5.3.2.16 (2-23)
_OP(   code = 0x0204,    name = "user_message_reference",      type = SMPPInteger2                                                ), # 5.3.2.17
_OP(   code = 0x0205,    name = "user_response_code",          type = SMPPInteger1                                                ), # 5.3.2.18
_OP(   code = 0x020A,    name = "source_port",                 type = SMPPInteger2                                                ), # 5.3.2.20
_OP(   code = 0x020B,    name = "destination_port",            type = SMPPInteger2                                                ), # 5.3.2.21
_OP(   code = 0x020C,    name = "sar_msg_ref_num",             type = SMPPInteger2                                                ), # 5.3.2.22
_OP(   code = 0x020D,    name = "language_indicator",          type = SMPPInteger1                                                ), # 5.3.2.19
_OP(   code = 0x020E,    name = "sar_total_segments",          type = SMPPInteger1,      check = lambda v: 1 <= v.value <= 255    ), # 5.3.2.23
_OP(   code = 0x020F,    name = "sar_segment_seqnum",          type = SMPPInteger1,      check = lambda v: 1 <= v.value <= 255    ), # 5.3.2.24
_OP(   code = 0x0210,    name = "sc_interface_version",        type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 0x34   ), # 5.3.2.25
_OP(   code = 0x0302,    name = "callback_num_pres_ind",       type = SMPPOctetString,   check = lambda v: len(v) == 1            ), # 5.3.2.37 (1)
_OP(   code = 0x0303,    name = "callback_num_atag",           type = SMPPOctetString,   check = lambda v: len(v) <= 65           ), # 5.3.2.38 (0-65)
_OP(   code = 0x0304,    name = "number_of_messages",          type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 99     ), # 5.3.2.39
_OP(   code = 0x0381,    name = "callback_num",                type = SMPPOctetString,   check = lambda v: 4 <= len(v) <= 19      ), # 5.3.2.36 (4-19)
_OP(   code = 0x0420,    name = "dpf_result",                  type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 1      ), # 5.3.2.28
_OP(   code = 0x0421,    name = "set_dpf",                     type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 1      ), # 5.3.2.29
_OP(   code = 0x0422,    name = "ms_availability_status",      type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 2      ), # 5.3.2.30
_OP(   code = 0x0423,    name = "network_error_code",          type = SMPPOctetString,   check = lambda v: len(v) in (0, 3)       ), # 5.3.2.31 (3) *** WARNING *** spec says 3 octets but I've encountered empty strings
_OP(   code = 0x0424,    name = "message_payload",             type = SMPPOctetString,   check = lambda v: len(v) <= 65535        ), # 5.3.2.32 (0-65535)
_OP(   code = 0x0425,    name = "delivery_failure_reason",     type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 3      ), # 5.3.2.33
_OP(   code = 0x0426,    name = "more_messages_to_send",       type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 1      ), # 5.3.2.34
_OP(   code = 0x0427,    name = "message_state",               type = SMPPInteger1,      check = lambda v: 1 <= v.value <= 8      ), # 5.3.2.35
_OP(   code = 0x0501,    name = "ussd_service_op",             type = SMPPOctetString,   check = lambda v: len(v) == 1            ), # 5.3.2.44 (1)
_OP(   code = 0x1201,    name = "display_time",                type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 2      ), # 5.3.2.26
_OP(   code = 0x1203,    name = "sms_signal",                  type = SMPPInteger2                                                ), # 5.3.2.40
_OP(   code = 0x1204,    name = "ms_validity",                 type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 3      ), # 5.3.2.27
_OP(   code = 0x130C,    name = "alert_on_msg_delivery",       type = SMPPOctetString,   check = lambda v: len(v) == 0            ), # 5.3.2.41 (0) *** WARNING *** only allows 0 bytes empty value
_OP(   code = 0x1380,    name = "its_reply_type",              type = SMPPInteger1,      check = lambda v: 0 <= v.value <= 8      ), # 5.3.2.42
_OP(   code = 0x1383,    name = "its_session_info",            type = SMPPOctetString,   check = lambda v: len(v) == 2            ), # 5.3.2.43 (2)
}

optional_pdu_parameters_by_code = { op._code: op for op in optional_pdu_parameters_table }
optional_pdu_parameters = { op._name: op for op in optional_pdu_parameters_table }

###############################################################################

if __name__ == "__main__":

    print("self-testing module smpp_pdu_param_table.py:")

    # the least to test is to call all the late-bound lambdas

    for n, mp in mandatory_pdu_parameters.items():
        try:
            if mp._check: mp._check(SMPPInteger1(0x00))
        except TypeError:
            pass

    for n, op in optional_pdu_parameters.items():
        try:
            if op._check: op._check(SMPPInteger1(0x00))
        except TypeError:
            pass

    print("ok")

###############################################################################
# EOF
