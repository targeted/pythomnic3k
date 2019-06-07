#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Module smpp_pdus. Contains actual PDU parameter definitions.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
#
###############################################################################

__all__ = [ "BindReceiverPDU", "BindTransmitterPDU", "DataSmRespPDU", "SubmitSmPDU",
            "DeliverSmRespPDU", "UnbindRespPDU", "BindReceiverRespPDU", "CancelSmPDU",
            "BindTransceiverRespPDU", "OutbindPDU", "BindTransmitterRespPDU", "DataSmPDU",
            "EnquireLinkPDU", "SubmitSmRespPDU", "DeliverSmPDU", "SubmitMultiRespPDU",
            "UnbindPDU", "ReplaceSmPDU", "CancelSmRespPDU", "BindTransceiverPDU",
            "SubmitMultiPDU", "AlertNotificationPDU", "ReplaceSmRespPDU", "QuerySmPDU",
            "QuerySmRespPDU", "EnquireLinkRespPDU" ]

###############################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import *
import smpp34.smpp_pdu; from smpp34.smpp_pdu import *

###############################################################################

class BindTransmitterPDU(RequestPDU): # section 4.1.1

    command_id = 0x00000002
    mandatory_parameters = (
                            "system_id",
                            "password",
                            "system_type",
                            "interface_version",
                            "addr_ton",
                            "addr_npi",
                            "address_range",
                           )
    optional_parameters =  (
                           )

BindTransmitterPDU.register()

###############################################################################

class BindTransmitterRespPDU(ResponsePDU): # section 4.1.2

    command_id = 0x80000002
    mandatory_parameters = (
                            "system_id",
                           )
    optional_parameters =  (
                            "sc_interface_version",
                           )

BindTransmitterRespPDU.register()

###############################################################################

class BindReceiverPDU(RequestPDU): # section 4.1.3

    command_id = 0x00000001
    mandatory_parameters = (
                            "system_id",
                            "password",
                            "system_type",
                            "interface_version",
                            "addr_ton",
                            "addr_npi",
                            "address_range",
                           )
    optional_parameters =  (
                           )

BindReceiverPDU.register()

###############################################################################

class BindReceiverRespPDU(ResponsePDU): # section 4.1.4

    command_id = 0x80000001
    mandatory_parameters = (
                            "system_id",
                           )
    optional_parameters =  (
                            "sc_interface_version",
                           )

BindReceiverRespPDU.register()

###############################################################################

class BindTransceiverPDU(RequestPDU): # section 4.1.5

    command_id = 0x00000009
    mandatory_parameters = (
                            "system_id",
                            "password",
                            "system_type",
                            "interface_version",
                            "addr_ton",
                            "addr_npi",
                            "address_range",
                           )
    optional_parameters =  (
                           )

BindTransceiverPDU.register()

###############################################################################

class BindTransceiverRespPDU(ResponsePDU): # section 4.1.6

    command_id = 0x80000009
    mandatory_parameters = (
                            "system_id",
                           )
    optional_parameters =  (
                            "sc_interface_version",
                           )

BindTransceiverRespPDU.register()

###############################################################################

class OutbindPDU(PDU): # section 4.1.7

    command_id = 0x0000000B
    mandatory_parameters = (
                            "system_id",
                            "password",
                           )
    optional_parameters =  (
                           )

OutbindPDU.register()

###############################################################################

class UnbindPDU(RequestPDU): # section 4.2.1

    command_id = 0x00000006
    mandatory_parameters = ()
    optional_parameters =  ()

UnbindPDU.register()

###############################################################################

class UnbindRespPDU(ResponsePDU): # section 4.2.2

    command_id = 0x80000006
    mandatory_parameters = ()
    optional_parameters =  ()

UnbindRespPDU.register()

###############################################################################

class SubmitSmPDU(RequestPDU): # section 4.4.1

    command_id = 0x00000004
    mandatory_parameters = (
                            "service_type",
                            "source_addr_ton",
                            "source_addr_npi",
                            "source_addr",
                            "dest_addr_ton",
                            "dest_addr_npi",
                            "destination_addr",
                            "esm_class",
                            "protocol_id",
                            "priority_flag",
                            "schedule_delivery_time",
                            "validity_period",
                            "registered_delivery",
                            "replace_if_present_flag",
                            "data_coding",
                            "sm_default_msg_id",
                            "sm_length",
                            "short_message|sm_length",
                           )
    optional_parameters =  (
                            "user_message_reference",
                            "source_port",
                            "source_addr_subunit",
                            "destination_port",
                            "dest_addr_subunit",
                            "sar_msg_ref_num",
                            "sar_total_segments",
                            "sar_segment_seqnum",
                            "more_messages_to_send",
                            "payload_type",
                            "message_payload",
                            "privacy_indicator",
                            "callback_num",
                            "callback_num_pres_ind",
                            "callback_num_atag",
                            "source_subaddress",
                            "dest_subaddress",
                            "user_response_code",
                            "display_time",
                            "sms_signal",
                            "ms_validity",
                            "ms_msg_wait_facilities",
                            "number_of_messages",
                            "alert_on_msg_delivery",
                            "language_indicator",
                            "its_reply_type",
                            "its_session_info",
                            "ussd_service_op",
                           )
    udh_data_field = "short_message"

SubmitSmPDU.register()

###############################################################################

class SubmitSmRespPDU(ResponsePDU): # section 4.4.2

    command_id = 0x80000004
    mandatory_parameters = (
                            "message_id",
                           )
    optional_parameters =  (
                           )

SubmitSmRespPDU.register()

###############################################################################

class SubmitMultiPDU(RequestPDU): # section 4.5.1

    command_id = 0x00000021
    mandatory_parameters = (
                            "service_type",
                            "source_addr_ton",
                            "source_addr_npi",
                            "source_addr",
                            "number_of_dests",
                            "dest_addresses", # *** WARNING *** spec says dest_address(es)
                            "esm_class",
                            "protocol_id",
                            "priority_flag",
                            "schedule_delivery_time",
                            "validity_period",
                            "registered_delivery",
                            "replace_if_present_flag",
                            "data_coding",
                            "sm_default_msg_id",
                            "sm_length",
                            "short_message|sm_length",
                           )
    optional_parameters =  (
                            "user_message_reference",
                            "source_port",
                            "source_addr_subunit",
                            "destination_port",
                            "dest_addr_subunit",
                            "sar_msg_ref_num",
                            "sar_total_segments",
                            "sar_segment_seqnum",
                            "payload_type",
                            "message_payload",
                            "privacy_indicator",
                            "callback_num",
                            "callback_num_pres_ind",
                            "callback_num_atag",
                            "source_subaddress",
                            "dest_subaddress",
                            "display_time",
                            "sms_signal",
                            "ms_validity",
                            "ms_msg_wait_facilities",
                            "alert_on_msg_delivery",
                            "language_indicator",
                           )
    udh_data_field = "short_message"

SubmitMultiPDU.register()

###############################################################################

class SubmitMultiRespPDU(ResponsePDU): # section 4.5.2

    command_id = 0x80000021
    mandatory_parameters = (
                            "message_id",
                            "no_unsuccess",
                            "unsuccess_smes", # *** WARNING *** spec says unsuccess_sme(s)
                           )
    optional_parameters =  (
                           )

SubmitMultiRespPDU.register()

###############################################################################

class DeliverSmPDU(RequestPDU): # section 4.6.1

    command_id = 0x00000005
    mandatory_parameters = (
                            "service_type",
                            "source_addr_ton",
                            "source_addr_npi",
                            "source_addr",
                            "dest_addr_ton",
                            "dest_addr_npi",
                            "destination_addr",
                            "esm_class",
                            "protocol_id",
                            "priority_flag",
                            "schedule_delivery_time",
                            "validity_period",
                            "registered_delivery",
                            "replace_if_present_flag",
                            "data_coding",
                            "sm_default_msg_id",
                            "sm_length",
                            "short_message|sm_length",
                           )
    optional_parameters =  (
                            "user_message_reference",
                            "source_port",
                            "destination_port",
                            "sar_msg_ref_num",
                            "sar_total_segments",
                            "sar_segment_seqnum",
                            "user_response_code",
                            "privacy_indicator",
                            "payload_type",
                            "message_payload",
                            "callback_num",
                            "source_subaddress",
                            "dest_subaddress",
                            "language_indicator",
                            "its_session_info",
                            "network_error_code",
                            "message_state",
                            "receipted_message_id",
                           )
    udh_data_field = "short_message"

DeliverSmPDU.register()

###############################################################################

class DeliverSmRespPDU(ResponsePDU): # section 4.6.2

    command_id = 0x80000005
    mandatory_parameters = (
                            "message_id",
                           )
    optional_parameters =  (
                           )

DeliverSmRespPDU.register()

###############################################################################

class DataSmPDU(RequestPDU): # section 4.7.1

    command_id = 0x00000103
    mandatory_parameters = (
                            "service_type",
                            "source_addr_ton",
                            "source_addr_npi",
                            "source_addr",
                            "dest_addr_ton",
                            "dest_addr_npi",
                            "destination_addr",
                            "esm_class",
                            "registered_delivery",
                            "data_coding",
                           )
    optional_parameters =  (
                            "source_port",
                            "source_addr_subunit",
                            "source_network_type",
                            "source_bearer_type",
                            "source_telematics_id",
                            "destination_port",
                            "dest_addr_subunit",
                            "dest_network_type",
                            "dest_bearer_type",
                            "dest_telematics_id",
                            "sar_msg_ref_num",
                            "sar_total_segments",
                            "sar_segment_seqnum",
                            "more_messages_to_send",
                            "qos_time_to_live",
                            "payload_type",
                            "message_payload",
                            "set_dpf",
                            "receipted_message_id",
                            "message_state",
                            "network_error_code",
                            "user_message_reference",
                            "privacy_indicator",
                            "callback_num",
                            "callback_num_pres_ind",
                            "callback_num_atag",
                            "source_subaddress",
                            "dest_subaddress",
                            "user_response_code",
                            "display_time",
                            "sms_signal",
                            "ms_validity",
                            "ms_msg_wait_facilities",
                            "number_of_messages",
                            "alert_on_msg_delivery",
                            "language_indicator",
                            "its_reply_type",
                            "its_session_info",
                           )

DataSmPDU.register()

###############################################################################

class DataSmRespPDU(ResponsePDU): # section 4.7.2

    command_id = 0x80000103
    mandatory_parameters = (
                            "message_id",
                           )
    optional_parameters =  (
                            "delivery_failure_reason",
                            "network_error_code",
                            "additional_status_info_text",
                            "dpf_result",
                           )

DataSmRespPDU.register()

###############################################################################

class QuerySmPDU(RequestPDU): # section 4.8.1

    command_id = 0x00000003
    mandatory_parameters = ("message_id", "source_addr_ton", "source_addr_npi", "source_addr")
    optional_parameters =  ()

QuerySmPDU.register()

###############################################################################

class QuerySmRespPDU(ResponsePDU): # section 4.8.2

    command_id = 0x80000003
    mandatory_parameters = ("message_id", "final_date", "message_state", "error_code")
    optional_parameters = ()

QuerySmRespPDU.register()

###############################################################################

class CancelSmPDU(RequestPDU): # section 4.9.1

    command_id = 0x00000008
    mandatory_parameters = ("service_type", "message_id", "source_addr_ton", "source_addr_npi",
                            "source_addr", "dest_addr_ton", "dest_addr_npi", "destination_addr")
    optional_parameters =  ()

CancelSmPDU.register()

###############################################################################

class CancelSmRespPDU(ResponsePDU): # section 4.9.2

    command_id = 0x80000008
    mandatory_parameters = ()
    optional_parameters =  ()

CancelSmRespPDU.register()

###############################################################################

class ReplaceSmPDU(RequestPDU): # section 4.10.1

    command_id = 0x00000007
    mandatory_parameters = ("message_id",
                            "source_addr_ton",
                            "source_addr_npi",
                            "source_addr",
                            "schedule_delivery_time",
                            "validity_period",
                            "registered_delivery",
                            "sm_default_msg_id",
                            "sm_length",
                            "short_message")
    optional_parameters =  ()

ReplaceSmPDU.register()

###############################################################################

class ReplaceSmRespPDU(ResponsePDU): # section 4.10.2

    command_id = 0x80000007
    mandatory_parameters = ()
    optional_parameters =  ()

ReplaceSmRespPDU.register()

###############################################################################

class EnquireLinkPDU(RequestPDU): # section 4.11.1

    command_id = 0x00000015
    mandatory_parameters = ()
    optional_parameters =  ()

EnquireLinkPDU.register()

###############################################################################

class EnquireLinkRespPDU(ResponsePDU): # section 4.11.2

    command_id = 0x80000015
    mandatory_parameters = ()
    optional_parameters =  ()

EnquireLinkRespPDU.register()

###############################################################################

class AlertNotificationPDU(PDU): # section 4.12.1

    command_id = 0x00000102
    mandatory_parameters = (
                            "source_addr_ton",
                            "source_addr_npi",
                            "source_addr",
                            "esme_addr_ton",
                            "esme_addr_npi",
                            "esme_addr",
                           )
    optional_parameters =  (
                            "ms_availability_status",
                           )

AlertNotificationPDU.register()

###############################################################################

if __name__ == "__main__":

    print("self-testing module smpp_pdus.py:")

    from typecheck import tuple_of
    from smpp34.smpp_pdu import RequestPDU, ResponsePDU
    from pmnc.timeout import Timeout
    from io import BytesIO

    ###################################

    def test_pdus():

        for pdu_cls in PDU._registered_pdus.values():
            assert (issubclass(pdu_cls, RequestPDU) and pdu_cls.command_id.value & 0x80000000 == 0) or \
                   (issubclass(pdu_cls, ResponsePDU) and pdu_cls.command_id.value & 0x80000000 != 0) or \
                   (pdu_cls in (OutbindPDU, AlertNotificationPDU))
            assert tuple_of(str)(pdu_cls.mandatory_parameters)
            assert tuple_of(str)(pdu_cls.optional_parameters)
            if issubclass(pdu_cls, RequestPDU):
                assert pdu_cls.command_id.value | 0x80000000 in PDU._registered_pdus

    test_pdus()

    ###################################

    def test_performance():

        t = Timeout(5.0)
        c = 0

        while not t.expired:
            request = SubmitSmPDU.create(service_type = b"",
                                         source_addr_ton = 0x00,
                                         source_addr_npi = 0x00,
                                         source_addr = b"000000",
                                         dest_addr_ton = 0x00,
                                         dest_addr_npi = 0x00,
                                         destination_addr = b"000001",
                                         esm_class = 0x00,
                                         protocol_id = 0x00,
                                         priority_flag = 0x00,
                                         schedule_delivery_time = b"",
                                         validity_period = b"",
                                         registered_delivery = 0x01,
                                         replace_if_present_flag = 0x00,
                                         data_coding = 0x08,
                                         sm_default_msg_id = 0x01,
                                         short_message = b"MESSAGE")
            assert PDU.read(BytesIO(request.serialize())) == request
            c += 1

        print("performance: {0:.01f} request+response packet(s)/sec.".format(c / 5.0))

    test_performance()

    ###################################

    print("ok")

###############################################################################
# EOF
