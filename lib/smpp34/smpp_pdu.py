#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Module smpp_pdu. Contains base PDU class.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
#
###############################################################################

__all__ = [ "PDU", "RequestPDU", "ResponsePDU", "GenericNackPDU" ]

###############################################################################

import io; from io import BytesIO
import threading; from threading import Event
import time; from time import time

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import *
import interlocked_counter; from interlocked_counter import InterlockedCounter
import smpp34.smpp_tools; from smpp34.smpp_tools import *
import smpp34.smpp_types; from smpp34.smpp_types import *
import smpp34.smpp_errors; from smpp34.smpp_errors import *
import smpp34.smpp_pdu_param_table; from smpp34.smpp_pdu_param_table import *

###############################################################################

# base abstract PDU class

class PDU:

    @typecheck
    def __init__(self, command_status: dword, sequence_number: lambda x: dword(x) and 0 < x <= 0x7fffffff): # see 5.1.4

        self.command_status = SMPPInteger4(command_status)
        self.sequence_number = SMPPInteger4(sequence_number)

    _max_length = 65536
    _sequence_number = InterlockedCounter(modulo = 0x80000000)

    ###################################

    def __eq__(self, other):
        return type(self) is type(other) and \
               self.command_status == other.command_status and \
               self.sequence_number == other.sequence_number and \
               self._mandatory_parameters == other._mandatory_parameters and \
               self._optional_parameters == other._optional_parameters

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        if self.command_status.value != error_codes.ESME_ROK:
            if self.command_status.value in error_codes:
                cs = error_codes[self.command_status.value]["name"]
            else:
                cs = str(self.command_status)
            cs = "command_status = {0:s}, ".format(cs)
        else:
            cs = ""
        pf = "{0:s}sequence_number = {1:s}".format(cs, self.sequence_number)
        mp = ", ".join("{0:s} = {1:s}".format(n, v)
                       for n, v in zip(self.__class__.mandatory_parameters,
                                       self._mandatory_parameters))
        if mp: mp = ", {0:s}".format(mp)
        op = ", ".join("{0:s} = {1:s}".format(n, v)
                       for n, v in self._optional_parameters.items())
        if op: op = ", {0:s}".format(op)
        return "{0:s}({1:s}{2:s}{3:s})".format(self.__class__.__name__, pf, mp, op)

    def __repr__(self):
        return "<{0:s} at 0x{1:08x}>".format(self, id(self))

    def __format__(self, spec):
        return format(str(self), spec)

    ###################################

    @classmethod
    @typecheck
    def read(cls, r: with_attr("read")):
        try:

            pdu_cls = None

            command_length = SMPPInteger4.read(r).value
            if not 16 <= command_length <= cls._max_length:
                raise Exception("invalid command length: {0:d} byte(s)".\
                                format(command_length))

            command_id = SMPPInteger4.read(r).value
            pdu_cls = PDU._registered_pdus.get(command_id)
            if not pdu_cls:
                raise Exception("command id 0x{0:08x} is not supported".\
                                format(command_id))

            pdu = pdu_cls(SMPPInteger4.read(r).value, SMPPInteger4.read(r).value)

            pdu_body_length = command_length - 16
            pdu_body = BytesIO(r.read(pdu_body_length))
            pdu_body.eof = lambda: pdu_body.tell() == pdu_body_length

            pdu._read_body(pdu_body)

        except Exception as e:
            raise SMPPPDUReadError("failed to read {0:s}: {1:s}".\
                                   format((pdu_cls or cls).__name__, str(e))) from e
        else:
            return pdu

    ###################################

    @classmethod
    def create(cls, command_status = None, sequence_number = None, **kwargs):
        try:

            if command_status is None:
                command_status = error_codes.ESME_ROK
            if command_status not in error_codes:
                raise Exception("unknown command_status: 0x{0:08x}".format(command_status))
            if sequence_number is None:
                sequence_number = cls._sequence_number.next()
                if sequence_number == 0: # specification forbids zero sequence number, see 5.1.4
                    sequence_number = cls._sequence_number.next()

            pdu = cls(command_status, sequence_number)
            pdu._create_body(**kwargs)

        except Exception as e:
            raise SMPPPDUCreateError("failed to create {0:s}: {1:s}".\
                                     format(cls.__name__, str(e))) from e
        else:
            return pdu

    ###################################

    def _create_body(self, **kwargs):

        # if the packet being created is a failure response, the entire body can be omitted

        if self.command_status.value != error_codes.ESME_ROK and not kwargs:
            self._mandatory_parameters = ()
            self._optional_parameters = {}
            return

        # udh is handled before other parameters and prepended to the data field value

        udh_df = getattr(self.__class__, "udh_data_field", None)
        if udh_df is not None:
            udh = kwargs.pop("udh", None)
            if udh is not None:
                udh = SMPPUDH(*udh)
                df = kwargs.get(udh_df, b"") # data field may not present
                kwargs[udh_df] = udh.serialize() + df
                esm_class = kwargs.get("esm_class")
                if esm_class is not None:
                    kwargs["esm_class"] = esm_class | 0x40 # mark the UDH presence
            setattr(self, "udh", udh) # a pseudo-parameter is inserted, possibly None

        # create mandatory parameters

        skipped_implicit_indice = {}
        self._mandatory_parameters = []
        for n in self.__class__.mandatory_parameters:
            try:

                # implicit parameters are set implicitly when the other
                # parameter whose length they indicate is set

                if n in self.implicit_mandatory_parameters:
                    if n in kwargs:
                        raise Exception("value for {0:s} should not be specified explicitly".format(n))
                    self._mandatory_parameters.append(None) # reserve the spot
                    skipped_implicit_indice[n] = len(self._mandatory_parameters) - 1
                    continue

                if n not in kwargs:
                    raise Exception("value for {0:s} is not specified".format(n))

                mpv = mandatory_pdu_parameters[n].create(kwargs.pop(n))
                self._mandatory_parameters.append(mpv)
                setattr(self, n, mpv)

                # if this parameter has implicit length parameter,
                # set the other one now

                if n in self.mandatory_parameters_ref_lengths:
                    rl = self.mandatory_parameters_ref_lengths[n]
                    rlv = mandatory_pdu_parameters[rl].create(len(mpv))
                    self._mandatory_parameters[skipped_implicit_indice[rl]] = rlv
                    setattr(self, rl, rlv)

            except Exception as e:
                raise Exception("failed to create mandatory parameter "
                                "{0:s}: {1:s}".format(n, str(e)))

        # mandatory parameters are ordered and immutable

        self._mandatory_parameters = tuple(self._mandatory_parameters)

        # create optional parameters

        self._optional_parameters = {}
        for n in self.__class__.optional_parameters:
            try:

                if n in kwargs:
                    opv = optional_pdu_parameters[n].create(kwargs.pop(n))
                    self._optional_parameters[n] = opv
                    setattr(self, n, opv)

            except Exception as e:
                raise Exception("failed to create optional parameter "
                                "{0:s}: {1:s}".format(n, str(e)))

        if kwargs:
            raise Exception("unexpected parameter(s) {0:s}".\
                            format(", ".join(kwargs.keys())))

    ###################################

    @typecheck
    def _read_body(self, r: with_attr("read", "eof")):

        # if the packet being read is a failure response, the entire body can be omitted

        if self.command_status.value != error_codes.ESME_ROK and r.eof():
            self._mandatory_parameters = ()
            self._optional_parameters = {}
            return

        # read mandatory parameters in fixed order

        self._mandatory_parameters = []
        for n in self.__class__.mandatory_parameters:
            try:

                rl = self.mandatory_parameters_ref_lengths.get(n)
                if rl is not None:
                    rl = getattr(self, rl).value
                mpv = mandatory_pdu_parameters[n].read(r, rl)
                self._mandatory_parameters.append(mpv)
                setattr(self, n, mpv)

            except Exception as e:
                raise Exception("failed to read mandatory parameter "
                                "{0:s}: {1:s}".format(n, str(e)))

        # udh is handled after other parameters and extracted from the data field value

        udh_df = getattr(self.__class__, "udh_data_field", None)
        if udh_df is not None:
            if self.esm_class.value & 0x40: # UDH is present
                df = getattr(self, udh_df)
                data = BytesIO(df.value)
                try:
                    udh = SMPPUDH.read(data)
                except Exception as e:
                    raise Exception("failed to parse UDH: {0:s}".format(str(e)))
                setattr(self, "udh", udh)
            else:
                setattr(self, "udh", None)

        # mandatory parameters are ordered and immutable

        self._mandatory_parameters = tuple(self._mandatory_parameters)

        # read optional parameters in arbitrary order

        self._optional_parameters = {}
        while not r.eof():
            try:

                opn = None
                t, v = SMPPTLV.read(r).value
                op = optional_pdu_parameters_by_code.get(t)
                if not op: continue
                opn = op.name

                if opn in self._optional_parameters:
                    raise Exception("{0:s} has already been specified".format(opn))

                if opn not in self.__class__.optional_parameters:
                    raise Exception("{0:s} may not be present".format(opn))

                opv = op.read(BytesIO(v), len(v))
                self._optional_parameters[opn] = opv
                setattr(self, opn, opv)

            except Exception as e:
                raise Exception("failed to read optional parameter{0:s}: {1:s}".\
                                format(opn is not None and " {0:s}".format(opn) or "", str(e)))

    ###################################

    @typecheck
    def serialize(self) -> bytes:

        pdu_content = self.command_id.serialize() + \
                      self.command_status.serialize() + \
                      self.sequence_number.serialize() + \
                      b"".join(v.serialize() for v in self._mandatory_parameters) + \
                      b"".join(SMPPTLV((optional_pdu_parameters[n].code, v.serialize())).serialize()
                               for n, v in self._optional_parameters.items())

        return SMPPInteger4(4 + len(pdu_content)).serialize() + pdu_content

    ###################################

    _registered_pdus = {}

    ###################################

    @classmethod
    def register(cls):

        # parse and verify mandatory parameters for the PDU being registered

        mandatory_parameters = []
        mandatory_parameters_ref_lengths = {}
        implicit_mandatory_parameters = {}

        for i, n in enumerate(cls.mandatory_parameters):

            if "|" in n: # reference to length-containing parameter
                n, rl = n.split("|", 1)
                assert rl in mandatory_pdu_parameters, \
                       "unsupported mandatory parameter: {0:s}".format(rl)
                mandatory_parameters_ref_lengths[n] = rl
                try:
                    assert cls.mandatory_parameters.index(rl) < i, \
                           "mandatory parameter {0:s} should go before {1:s}".format(rl, n)
                except ValueError:
                    assert False, "mandatory parameter {0:s} is missing".format(rl)
                implicit_mandatory_parameters[rl] = n

            assert n in mandatory_pdu_parameters, \
                   "unsupported mandatory parameter: {0:s}".format(n)
            assert n not in mandatory_parameters, "duplicate mandatory parameter: {0:s}".format(n)
            mandatory_parameters.append(n)

        setattr(cls, "mandatory_parameters_ref_lengths", mandatory_parameters_ref_lengths)
        setattr(cls, "mandatory_parameters", tuple(mandatory_parameters))
        setattr(cls, "implicit_mandatory_parameters", implicit_mandatory_parameters)

        # see if the PDU supports UDH

        udh_df = getattr(cls, "udh_data_field", None)
        if udh_df is not None:
            assert udh_df in cls.mandatory_parameters, \
                   "unsupported mandatory parameter: {0:s}".format(udh_df)
            assert "esm_class" in cls.mandatory_parameters, \
                   "mandatory parameter esm_class is required"

        # verify optional parameters for the PDU being registered

        optional_parameters = []
        for n in cls.optional_parameters:
            assert n in optional_pdu_parameters, \
                   "unsupported optional parameter: {0:s}".format(n)
            assert n not in optional_parameters, "duplicate optional parameter: {0:s}".format(n)
            optional_parameters.append(n)
        setattr(cls, "optional_parameters", tuple(optional_parameters))

        assert dword(cls.command_id), "invalid command id"
        PDU._registered_pdus[cls.command_id] = cls
        cls.command_id = SMPPInteger4(cls.command_id)

###############################################################################

class ResponsePDU(PDU):

    def throw_if_error(self):
        command_status = self.command_status.value
        if command_status in error_codes:
            if command_status != error_codes.ESME_ROK:
                raise SMPPResponseError("SMPP error {0[name]:s} ({0[description]:s})".\
                                        format(error_codes[command_status]))
        else:
            raise SMPPResponseError("SMPP error 0x{0:08x} (Unknown Error)".\
                                    format(command_status))

###############################################################################

class GenericNackPDU(ResponsePDU): # section 4.3.1

    command_id = 0x80000000
    mandatory_parameters = ()
    optional_parameters =  ()

GenericNackPDU.register()

###############################################################################

class RequestPDU(PDU):

    @typecheck
    def __init__(self, command_status: one_of(error_codes.ESME_ROK), sequence_number: dword):
        PDU.__init__(self, command_status, sequence_number)
        self._response_ready = Event()

    @typecheck
    def set_response(self, response: ResponsePDU):
        if response.sequence_number != self.sequence_number or \
           response.command_id.value not in (self.command_id.value | 0x80000000, 0x80000000): # plain 0x80000000 == GENERIC_NACK
            raise Exception("mismatched response")
        self._response = response
        self._response_ready.set()

    @typecheck
    def wait_response(self, timeout: optional(float) = None) -> optional(ResponsePDU): # respects wall-time timeout, see issue9892
        if timeout is None:
            self._response_ready.wait()
            return self._response
        start, remain = time(), timeout
        while remain > 0.0:
            self._response_ready.wait(remain)
            if self._response_ready.is_set():
                return self._response
            remain = timeout - (time() - start)
        else:
            return None

    @typecheck
    def create_response(self, command_status: optional(dword) = error_codes.ESME_ROK, **kwargs) -> ResponsePDU:
        resp_cls = PDU._registered_pdus[self.command_id.value | 0x80000000]
        return resp_cls.create(command_status, self.sequence_number.value, **kwargs)

    @typecheck
    def create_nack(self, command_status: lambda x: dword(x) and x != error_codes.ESME_ROK) -> GenericNackPDU:
        return GenericNackPDU.create(command_status, self.sequence_number.value)

###############################################################################

if __name__ == "__main__":

    from os import getenv

    if getenv("PYTHONHASHSEED") != "0":
        raise SystemExit("Running self-tests requires turning off hash randomization, "
                         "set environment variable PYTHONHASHSEED to 0.")

    print("self-testing module smpp_pdu.py:")

    from expected import expected

    ###################################

    def f_s(v):
        s = str(v)
        assert "{0}".format(v) == "{0:s}".format(v) == s
        assert repr(v) == "<{0:s} at 0x{1:08x}>".format(s, id(v))
        return s

    ###################################

    def test_command_length():

        with expected(SMPPPDUReadError("failed to read PDU: invalid command length: 15 byte(s)")):
            PDU.read(BytesIO(SMPPInteger4(15).serialize()))

        with expected(SMPPPDUReadError("failed to read PDU: invalid command length: ")):
            PDU.read(BytesIO(SMPPInteger4(PDU._max_length + 1).serialize()))

    test_command_length()

    ###################################

    def test_command_id():

        with expected(SMPPPDUReadError("failed to read PDU: command id 0x87654321 is not supported")):
            PDU.read(BytesIO(SMPPInteger4(16).serialize() + SMPPInteger4(0x87654321).serialize()))

    test_command_id()

    ###################################

    def test_register():

        with expected(AssertionError("invalid command id")):
            class BrokenPDU(PDU):
                command_id = "foo"
                mandatory_parameters = ()
                optional_parameters = ()
            BrokenPDU.register()

        with expected(AssertionError("unsupported mandatory parameter: not_there")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ("not_there", )
                optional_parameters = ()
            BrokenPDU.register()

        with expected(AssertionError("unsupported optional parameter: not_there")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ()
                optional_parameters = ("not_there", )
            BrokenPDU.register()

        with expected(AssertionError("unsupported mandatory parameter: not_there")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ("short_message|not_there",)
                optional_parameters = ()
            BrokenPDU.register()

        with expected(AssertionError("mandatory parameter sm_length is missing")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ("short_message|sm_length",)
                optional_parameters = ()
            BrokenPDU.register()

        with expected(AssertionError("mandatory parameter sm_length should go before short_message")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ("short_message|sm_length", "sm_length")
                optional_parameters = ()
            BrokenPDU.register()

        with expected(AssertionError("duplicate mandatory parameter: esm_class")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ("esm_class", "esm_class")
                optional_parameters = ()
            BrokenPDU.register()

        with expected(AssertionError("duplicate mandatory parameter: short_message")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ("esm_class", "sm_length", "short_message|sm_length", "short_message|esm_class")
                optional_parameters = ()
            BrokenPDU.register()

        with expected(AssertionError("duplicate optional parameter: dpf_result")):
            class BrokenPDU(PDU):
                command_id = 0x00000001
                mandatory_parameters = ()
                optional_parameters = ("dpf_result", "delivery_failure_reason", "dpf_result")
            BrokenPDU.register()

        class CorrectPDU(PDU):
            command_id = 0x00000333
            mandatory_parameters = ("sm_length", "short_message|sm_length", "addr_ton")
            optional_parameters = ()
        CorrectPDU.register()

        assert CorrectPDU.mandatory_parameters_ref_lengths == { "short_message": "sm_length" }
        assert CorrectPDU.mandatory_parameters == ("sm_length", "short_message", "addr_ton")
        assert CorrectPDU.implicit_mandatory_parameters == { "sm_length": "short_message" }

    test_register()

    ###################################

    def test_read():

        class FooPDU(PDU):
            command_id = 0x00000001
            mandatory_parameters = ("password", )
            optional_parameters = ("sar_msg_ref_num", "ussd_service_op")

        pkt = SMPPInteger4(16).serialize() + SMPPInteger4(0x00000001).serialize() + \
              SMPPInteger4(0).serialize() + SMPPInteger4(3).serialize()

        with expected(SMPPPDUReadError("failed to read PDU: command id 0x00000001 is not supported")):
            PDU.read(BytesIO(pkt))

        FooPDU.register()

        with expected(SMPPPDUReadError("failed to read FooPDU: failed to read mandatory parameter password: "
                                       "SMPPCOctetString.read() has encountered unexpected end of stream")):
            PDU.read(BytesIO(pkt))

        ###############################

        pkt = SMPPInteger4(20).serialize() + SMPPInteger4(0x00000001).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + SMPPCOctetString(b"FOO").serialize()

        fpdu = PDU.read(BytesIO(pkt))
        assert isinstance(fpdu, FooPDU)
        assert fpdu.password == SMPPCOctetString(b"FOO")
        assert not hasattr(fpdu, "sar_msg_ref_num")
        assert not hasattr(fpdu, "ussd_service_op")

        ###############################

        pkt = SMPPInteger4(24).serialize() + SMPPInteger4(0x00000001).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + SMPPCOctetString(b"FOO").serialize() + \
              SMPPTLV((0x0423, b"")).serialize()

        with expected(SMPPPDUReadError("failed to read FooPDU: failed to read optional parameter network_error_code: "
                                       "network_error_code may not be present")):
            PDU.read(BytesIO(pkt))

        ###############################

        pkt = SMPPInteger4(25).serialize() + SMPPInteger4(0x00000001).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + SMPPCOctetString(b"FOO").serialize() + \
              SMPPTLV((0x020C, b"\x00")).serialize()

        with expected(SMPPPDUReadError("failed to read FooPDU: failed to read optional parameter sar_msg_ref_num: "
                                       "read() has got an incompatible value for length: 1")):
            PDU.read(BytesIO(pkt))

        ###############################

        pkt = SMPPInteger4(26).serialize() + SMPPInteger4(0x00000001).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + SMPPCOctetString(b"FOO").serialize() + \
              SMPPTLV((0x020C, b"\x12\x34")).serialize()

        fpdu = PDU.read(BytesIO(pkt))
        assert isinstance(fpdu, FooPDU)
        assert fpdu.password == SMPPCOctetString(b"FOO")
        assert fpdu.sar_msg_ref_num == SMPPInteger2(0x1234)
        assert not hasattr(fpdu, "ussd_service_op")

        ###############################

        pkt = SMPPInteger4(30).serialize() + SMPPInteger4(0x00000001).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + SMPPCOctetString(b"FOO").serialize() + \
              SMPPTLV((0x020C, b"\x12\x34")).serialize() + SMPPTLV((0x0501, b"")).serialize()

        with expected(SMPPPDUReadError("failed to read FooPDU: failed to read optional parameter ussd_service_op: "
                                       "SMPPOctetString(\"\") does not pass the check")):
            PDU.read(BytesIO(pkt))

        ###############################

        pkt = SMPPInteger4(31).serialize() + SMPPInteger4(0x00000001).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + SMPPCOctetString(b"FOO").serialize() + \
              SMPPTLV((0x020C, b"\x12\x34")).serialize() + SMPPTLV((0x0501, b"\xa5")).serialize()

        fpdu = PDU.read(BytesIO(pkt))
        assert isinstance(fpdu, FooPDU)
        assert fpdu.password == SMPPCOctetString(b"FOO")
        assert fpdu.sar_msg_ref_num == SMPPInteger2(0x1234)
        assert fpdu.ussd_service_op == SMPPOctetString(b"\xa5")

        ###############################

        class ReqPDU(RequestPDU):
            command_id = 0x00000001
            mandatory_parameters = ()
            optional_parameters = ()
        ReqPDU.register()

        pkt = SMPPInteger4(16).serialize() + SMPPInteger4(0x00000001).serialize() + SMPPInteger4(0x00000000).serialize() + SMPPInteger4(0x00000001).serialize()
        PDU.read(BytesIO(pkt))

        pkt = SMPPInteger4(16).serialize() + SMPPInteger4(0x00000001).serialize() + SMPPInteger4(0x00000001).serialize() + SMPPInteger4(0x00000001).serialize()
        with expected(SMPPPDUReadError("failed to read ReqPDU: __init__() has got an incompatible value for command_status: 1")):
            PDU.read(BytesIO(pkt))

        pkt = SMPPInteger4(16).serialize() + SMPPInteger4(0x00000001).serialize() + SMPPInteger4(0x00000000).serialize() + SMPPInteger4(0x80000000).serialize()
        with expected(SMPPPDUReadError("failed to read ReqPDU: __init__() has got an incompatible value for sequence_number: 2147483648")):
            PDU.read(BytesIO(pkt))

        ###############################

        class RespPDU(ResponsePDU):
            command_id = 0x00000001
            mandatory_parameters = ()
            optional_parameters = ()
        RespPDU.register()

        pkt = SMPPInteger4(16).serialize() + SMPPInteger4(0x00000001).serialize() + SMPPInteger4(0x00000000).serialize() + SMPPInteger4(0x00000001).serialize()
        PDU.read(BytesIO(pkt))

        pkt = SMPPInteger4(16).serialize() + SMPPInteger4(0x00000001).serialize() + SMPPInteger4(0x00000000).serialize() + SMPPInteger4(0x80000000).serialize()
        with expected(SMPPPDUReadError("failed to read RespPDU: __init__() has got an incompatible value for sequence_number: 2147483648")):
            PDU.read(BytesIO(pkt))

        pkt = SMPPInteger4(16).serialize() + SMPPInteger4(0x00000001).serialize() + SMPPInteger4(0x12345678).serialize() + SMPPInteger4(0x00000001).serialize()
        rpdu = PDU.read(BytesIO(pkt))
        with expected(SMPPResponseError("SMPP error 0x12345678 (Unknown Error)")):
            rpdu.throw_if_error()

        ###############################

    test_read()

    ###################################

    def test_sequence_number():

        with expected(InputParameterError):
            PDU(0x12345678, 0x00000000)

        with expected(InputParameterError):
            PDU(0x12345678, 0x80000000)

        PDU(0x12345678, 0x7fffffff)

        class PDU4321(PDU):
            command_id = 0x00004321
            mandatory_parameters = ()
            optional_parameters = ()
        PDU4321.register()

        with expected(SMPPPDUReadError("failed to read PDU4321: __init__() has got an incompatible value for sequence_number: 0")):
            PDU.read(BytesIO(SMPPInteger4(16).serialize() + SMPPInteger4(0x00004321).serialize() +
                             SMPPInteger4(0x00000000).serialize() + SMPPInteger4(0x00000000).serialize()))

        with expected(SMPPPDUReadError("failed to read PDU4321: __init__() has got an incompatible value for sequence_number: 2147483648")):
            PDU.read(BytesIO(SMPPInteger4(16).serialize() + SMPPInteger4(0x00004321).serialize() +
                             SMPPInteger4(0x00000000).serialize() + SMPPInteger4(0x80000000).serialize()))

        PDU.read(BytesIO(SMPPInteger4(16).serialize() + SMPPInteger4(0x00004321).serialize() +
                         SMPPInteger4(0x00000000).serialize() + SMPPInteger4(0x7fffffff).serialize()))

    test_sequence_number()

    ###################################

    def test_read_ref_length():

        class BarPDU1(PDU):
            command_id = 0x00000002
            mandatory_parameters = ("short_message", )
            optional_parameters = ()

        BarPDU1.register()

        pkt = SMPPInteger4(19).serialize() + SMPPInteger4(0x00000002).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + SMPPOctetString(b"ABC").serialize()

        with expected(SMPPPDUReadError("failed to read BarPDU1: failed to read mandatory parameter short_message: "
                                       "read() has got an incompatible value for length: None")):
            PDU.read(BytesIO(pkt))

        ###############################

        class BarPDU2(PDU):
            command_id = 0x00000003
            mandatory_parameters = ("sm_length", "short_message|sm_length")
            optional_parameters = ()

        BarPDU2.register()

        pkt = SMPPInteger4(20).serialize() + SMPPInteger4(0x00000003).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + \
              SMPPInteger1(3).serialize() + SMPPOctetString(b"ABC").serialize()

        bpdu = PDU.read(BytesIO(pkt))
        assert isinstance(bpdu, BarPDU2)
        assert bpdu.sm_length == SMPPInteger1(3)
        assert bpdu.short_message == SMPPOctetString(b"ABC")

        ###############################

        class BarPDU3(PDU):
            command_id = 0x00000004
            mandatory_parameters = ("number_of_dests", "dest_addresses|number_of_dests")
            optional_parameters = ()

        BarPDU3.register()

        sda = SMPPSmeDestAddress((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo")))
        dl = SMPPDistributionList((SMPPCOctetString(b"bar"), ))
        da1 = SMPPDestAddress((SMPPInteger1(0x01), sda))
        da2 = SMPPDestAddress((SMPPInteger1(0x02), dl))
        das = SMPPDestAddresses((da1, da2))

        pkt = SMPPInteger4(29).serialize() + SMPPInteger4(0x00000004).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + \
              SMPPInteger1(2).serialize() + das.serialize()

        bpdu = PDU.read(BytesIO(pkt))
        assert isinstance(bpdu, BarPDU3)
        assert bpdu.number_of_dests == SMPPInteger1(2)
        assert bpdu.dest_addresses == das

        ###############################

        class BarPDU4(PDU):
            command_id = 0x00000005
            mandatory_parameters = ("no_unsuccess", "unsuccess_smes|no_unsuccess")
            optional_parameters = ()

        BarPDU4.register()

        us1 = SMPPUnsuccessSme((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo"), SMPPInteger4(0xffffffff)))
        us2 = SMPPUnsuccessSme((SMPPInteger1(0x56), SMPPInteger1(0x78), SMPPCOctetString(b"bar"), SMPPInteger4(0x00000000)))
        uss = SMPPUnsuccessSmes((us1, us2))

        pkt = SMPPInteger4(37).serialize() + SMPPInteger4(0x00000005).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + \
              SMPPInteger1(2).serialize() + uss.serialize()

        bpdu = PDU.read(BytesIO(pkt))
        assert isinstance(bpdu, BarPDU4)
        assert bpdu.no_unsuccess == SMPPInteger1(2)
        assert bpdu.unsuccess_smes == uss

        ###############################

        class BarPDU5(PDU):
            command_id = 0x00000006
            mandatory_parameters = ("no_unsuccess", "interface_version", "sm_length", "final_date", "number_of_dests",
                                    "short_message|sm_length", "dest_addresses|number_of_dests", "unsuccess_smes|no_unsuccess")
            optional_parameters = ()

        BarPDU5.register()

        sda = SMPPSmeDestAddress((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo")))
        dl = SMPPDistributionList((SMPPCOctetString(b"bar"), ))
        da1 = SMPPDestAddress((SMPPInteger1(0x01), sda))
        das = SMPPDestAddresses((da1, ))

        us1 = SMPPUnsuccessSme((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo"), SMPPInteger4(0xffffffff)))
        us2 = SMPPUnsuccessSme((SMPPInteger1(0x56), SMPPInteger1(0x78), SMPPCOctetString(b"bar"), SMPPInteger4(0x00000000)))
        uss = SMPPUnsuccessSmes((us1, us2))

        pkt = SMPPInteger4(51).serialize() + SMPPInteger4(0x00000006).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + \
              SMPPInteger1(2).serialize() + SMPPInteger1(0x34).serialize() + SMPPInteger1(3).serialize() + \
              SMPPCOctetString(b"").serialize() + SMPPInteger1(1).serialize() + SMPPOctetString(b"123").serialize() + \
              das.serialize() + uss.serialize()

        bpdu = PDU.read(BytesIO(pkt))
        assert bpdu.no_unsuccess == SMPPInteger1(2)
        assert bpdu.interface_version == SMPPInteger1(0x34)
        assert bpdu.sm_length == SMPPInteger1(3)
        assert bpdu.final_date == SMPPCOctetString(b"")
        assert bpdu.number_of_dests == SMPPInteger1(1)
        assert bpdu.short_message == SMPPOctetString(b"123")
        assert bpdu.dest_addresses == das
        assert bpdu.unsuccess_smes == uss

    test_read_ref_length()

    ###################################

    def test_invalid_optional():

        class OptPDU(PDU):
            command_id = 0x00000007
            mandatory_parameters = ()
            optional_parameters = ("additional_status_info_text", )
        OptPDU.register()

        pkt = SMPPInteger4(28).serialize() + SMPPInteger4(0x00000007).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + \
              SMPPTLV((0x6543, b"")).serialize() + \
              SMPPTLV((0x001D, b"foo\x00")).serialize()

        opdu = PDU.read(BytesIO(pkt)) # unknown tag is skipped, parsing continues
        assert opdu.additional_status_info_text.value == b"foo"

        pkt = SMPPInteger4(20).serialize() + SMPPInteger4(0x00000007).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + \
              SMPPTLV((0x1201, b"")).serialize()

        with expected(SMPPPDUReadError("failed to read OptPDU: failed to read optional parameter display_time: display_time may not be present")):
            PDU.read(BytesIO(pkt))

        pkt = SMPPInteger4(26).serialize() + SMPPInteger4(0x00000007).serialize() + \
              SMPPInteger4(2).serialize() + SMPPInteger4(3).serialize() + \
              SMPPTLV((0x001D, b"\x00")).serialize()  + SMPPTLV((0x001D, b"\x00")).serialize()

        with expected(SMPPPDUReadError("failed to read OptPDU: failed to read optional parameter additional_status_info_text: "
                                       "additional_status_info_text has already been specified")):
            PDU.read(BytesIO(pkt))

    test_invalid_optional()

    ###################################

    def test_create():

        class CreaPDU(PDU):
            command_id = 0x00000008
            mandatory_parameters = ("sm_length", "short_message|sm_length", "message_state")
            optional_parameters = ("number_of_messages", )
        CreaPDU.register()

        ###############################

        with expected(SMPPPDUCreateError("failed to create CreaPDU: failed to create mandatory parameter short_message: "
                                         "value for short_message is not specified")):
            CreaPDU.create()

        with expected(SMPPPDUCreateError("failed to create CreaPDU: failed to create mandatory parameter sm_length: "
                                         "value for sm_length should not be specified explicitly")):
            CreaPDU.create(short_message = b"foo", sm_length = 3)

        with expected(SMPPPDUCreateError("failed to create CreaPDU: failed to create mandatory parameter short_message: "
                                         "SMPPOctetString(\"" + "*" * 255 + "\") does not pass the check")):
            CreaPDU.create(short_message = b"*" * 255)

        ###############################

        cpdu = CreaPDU.create(short_message = b"foo", message_state = 0x01)
        assert cpdu.short_message == SMPPOctetString(b"foo")
        assert cpdu.sm_length == SMPPInteger1(3)
        assert cpdu.message_state == SMPPInteger1(1)
        assert not hasattr(cpdu, "number_of_messages")

        ###############################

        with expected(SMPPPDUCreateError("failed to create CreaPDU: unexpected parameter(s) foo, biz")):
            CreaPDU.create(short_message = b"foo", message_state = 0x01, foo = "bar", biz = "baz")

        with expected(SMPPPDUCreateError("failed to create CreaPDU: failed to create optional parameter number_of_messages: "
                                         "SMPPInteger1(0xff) does not pass the check")):
            CreaPDU.create(short_message = b"foo", message_state = 0x01, number_of_messages = 0xff)

        ###############################

        cpdu = CreaPDU.create(short_message = b"foo", message_state = 0x01, number_of_messages = 0x01)
        assert cpdu.short_message == SMPPOctetString(b"foo")
        assert cpdu.sm_length == SMPPInteger1(3)
        assert cpdu.message_state == SMPPInteger1(1)
        assert cpdu.number_of_messages == SMPPInteger1(1)

        ###############################

        class SomeBadReqPDU(RequestPDU):
            command_id = 0x00000008
            mandatory_parameters = ()
            optional_parameters = ()
        SomeBadReqPDU.register()

        SomeBadReqPDU.create(error_codes.ESME_ROK)

        with expected(SMPPPDUCreateError("failed to create SomeBadReqPDU: __init__() has got an incompatible value for command_status: 6")):
            SomeBadReqPDU.create(error_codes.ESME_RINVPRTFLG)

    test_create()

    ###################################

    def test_serialize():

        class SerPDU(PDU):
            command_id = 0x00000009
            mandatory_parameters = ("priority_flag", "esme_addr_npi", "short_message|priority_flag")
            optional_parameters = ("receipted_message_id", "callback_num")
        SerPDU.register()

        ###############################

        spdu = SerPDU.create(error_codes.ESME_ROK, 0x07654321, esme_addr_npi = 0x09, short_message = b"foo")
        assert spdu.serialize() == b"\x00\x00\x00\x15\x00\x00\x00\x09\x00\x00\x00\x00\x07\x65\x43\x21\x03\x09foo"
        assert PDU.read(BytesIO(spdu.serialize())) == spdu

        ###############################

        spdu = SerPDU.create(error_codes.ESME_ROK, 0x07654321, esme_addr_npi = 0x09, short_message = b"foo", callback_num = b"1234")
        assert spdu.serialize() == b"\x00\x00\x00\x1d\x00\x00\x00\x09\x00\x00\x00\x00\x07\x65\x43\x21\x03\x09foo\x03\x81\x00\x041234"
        assert PDU.read(BytesIO(spdu.serialize())) == spdu

        ###############################

        spdu = SerPDU.create(error_codes.ESME_ROK, 0x07654321, esme_addr_npi = 0x09, short_message = b"foo", receipted_message_id = b"")
        assert spdu.serialize() == b"\x00\x00\x00\x1a\x00\x00\x00\x09\x00\x00\x00\x00\x07\x65\x43\x21\x03\x09foo\x00\x1e\x00\x01\x00"
        assert PDU.read(BytesIO(spdu.serialize())) == spdu

        ###############################

        spdu = SerPDU.create(error_codes.ESME_RINVSRCADR, 0x07654321, esme_addr_npi = 0x09, short_message = b"foo", receipted_message_id = b"", callback_num = b"1234")
        assert spdu.serialize() == b"\x00\x00\x00\x22\x00\x00\x00\x09\x00\x00\x00\x0a\x07\x65\x43\x21\x03\x09foo\x03\x81\x00\x041234\x00\x1e\x00\x01\x00" # fixme: different order in 3.4.0
        assert PDU.read(BytesIO(spdu.serialize())) == spdu

        ###############################

    test_serialize()

    ###################################

    def test_str():

        class NullPDU(PDU):
            command_id = 0x0000000a
            mandatory_parameters = ()
            optional_parameters = ()
        NullPDU.register()

        p = NullPDU.create(0, 1)
        assert f_s(p) == "NullPDU(sequence_number = SMPPInteger4(0x00000001))"

        p = NullPDU.create(error_codes.ESME_RINVDFTMSGID, 1)
        assert f_s(p) == "NullPDU(command_status = ESME_RINVDFTMSGID, sequence_number = SMPPInteger4(0x00000001))"

        p = NullPDU.create(error_codes.ESME_RSUBMITFAIL, 1)
        assert f_s(p) == "NullPDU(command_status = ESME_RSUBMITFAIL, sequence_number = SMPPInteger4(0x00000001))"

        ###############################

        class ManPDU(PDU):
            command_id = 0x0000000b
            mandatory_parameters = ("data_coding", )
            optional_parameters = ()
        ManPDU.register()

        p = ManPDU.create(0, 1, data_coding = 0xff)
        assert f_s(p) == "ManPDU(sequence_number = SMPPInteger4(0x00000001), data_coding = SMPPInteger1(0xff))"

        ###############################

        class ManPDU2(PDU):
            command_id = 0x0000000c
            mandatory_parameters = ("sm_length", "no_unsuccess", "unsuccess_smes|no_unsuccess", "short_message|sm_length")
            optional_parameters = ()
        ManPDU2.register()

        us1 = SMPPUnsuccessSme((SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(b"foo"), SMPPInteger4(0xffffffff)))
        us2 = SMPPUnsuccessSme((SMPPInteger1(0x56), SMPPInteger1(0x78), SMPPCOctetString(b"bar"), SMPPInteger4(0x00000000)))

        p = ManPDU2.create(0, 1, short_message = b"TEXT", unsuccess_smes = (us1, us2))
        assert f_s(p) == "ManPDU2(sequence_number = SMPPInteger4(0x00000001), " \
                                 "sm_length = SMPPInteger1(0x04), no_unsuccess = SMPPInteger1(0x02), " \
                                 "unsuccess_smes = SMPPUnsuccessSmes(SMPPUnsuccessSme(SMPPInteger1(0x12), SMPPInteger1(0x34), SMPPCOctetString(\"foo\"), SMPPInteger4(0xffffffff)), " \
                                                                    "SMPPUnsuccessSme(SMPPInteger1(0x56), SMPPInteger1(0x78), SMPPCOctetString(\"bar\"), SMPPInteger4(0x00000000))), " \
                                 "short_message = SMPPOctetString(\"TEXT\"))"

        ###############################

        class OptPDU(PDU):
            command_id = 0x0000000d
            mandatory_parameters = ()
            optional_parameters = ("source_telematics_id", )
        OptPDU.register()

        p = OptPDU.create(0, 1, source_telematics_id = 0xff)
        assert f_s(p) == "OptPDU(sequence_number = SMPPInteger4(0x00000001), source_telematics_id = SMPPInteger1(0xff))"

        ###############################

        class OptPDU2(PDU):
            command_id = 0x0000000e
            mandatory_parameters = ()
            optional_parameters = ("source_telematics_id", "dest_telematics_id")
        OptPDU2.register()

        p = OptPDU2.create(0, 1, source_telematics_id = 0xff)
        assert f_s(p) == "OptPDU2(sequence_number = SMPPInteger4(0x00000001), source_telematics_id = SMPPInteger1(0xff))"

        p = OptPDU2.create(0, 1, dest_telematics_id = 0xffff)
        assert f_s(p) == "OptPDU2(sequence_number = SMPPInteger4(0x00000001), dest_telematics_id = SMPPInteger2(0xffff))"

        p = OptPDU2.create(0, 1, source_telematics_id = 0xff, dest_telematics_id = 0xffff)
        assert f_s(p) == "OptPDU2(sequence_number = SMPPInteger4(0x00000001), dest_telematics_id = SMPPInteger2(0xffff), source_telematics_id = SMPPInteger1(0xff))" # fixme: different order in 3.4.0

        ###############################

        class ManOptPDU(PDU):
            command_id = 0x0000000f
            mandatory_parameters = ("validity_period", )
            optional_parameters = ("alert_on_msg_delivery", )
        ManOptPDU.register()

        p = ManOptPDU.create(0, 1, validity_period = b"080128151723020+", alert_on_msg_delivery = b"")
        assert f_s(p) == "ManOptPDU(sequence_number = SMPPInteger4(0x00000001), " \
                                   "validity_period = SMPPCOctetString(\"080128151723020+\"), alert_on_msg_delivery = SMPPOctetString(\"\"))"

        ###############################

    test_str()

    ###################################

    def test_response():

        class SomeRequestPDU(RequestPDU):
            command_id = 0x00000010
            mandatory_parameters = ()
            optional_parameters = ()
        SomeRequestPDU.register()

        class SomeRequestRespPDU(ResponsePDU):
            command_id = 0x80000010
            mandatory_parameters = ()
            optional_parameters = ()
        SomeRequestRespPDU.register()

        class WrongRequestRespPDU(ResponsePDU):
            command_id = 0x80000011
            mandatory_parameters = ()
            optional_parameters = ()
        WrongRequestRespPDU.register()

        req = SomeRequestPDU.create(0x00000000, 0x00000001)
        resp = SomeRequestRespPDU.create(0x00000000, 0x00000001)

        before = time()
        assert req.wait_response(0.1) is None
        after = time()
        assert after - before >= 0.1

        req.set_response(resp)

        before = time()
        assert req.wait_response(0.1) is resp
        after = time()
        assert after - before < 0.01

        before = time()
        assert req.wait_response() is resp
        after = time()
        assert after - before < 0.01

        req = SomeRequestPDU(0x00000000, 0x12345678)
        with expected(Exception("mismatched response")):
            req.set_response(WrongRequestRespPDU.create(0x00000000, 0x12345678))
        with expected(Exception("mismatched response")):
            req.set_response(SomeRequestRespPDU.create(0x00000000, 0x07654321))

        class GenericNackPDU(ResponsePDU):
            command_id = 0x80000000
            mandatory_parameters = ()
            optional_parameters =  ()
        GenericNackPDU.register()

        req = SomeRequestPDU(0x00000000, 0x12345678)
        resp = req.create_nack(error_codes.ESME_RSYSERR)
        req.set_response(resp)
        assert req.wait_response() is resp

        assert req.create_response() == SomeRequestRespPDU.create(0x00000000, 0x12345678)
        assert req.create_response(error_codes.ESME_RINVOPTPARSTREAM) == SomeRequestRespPDU.create(error_codes.ESME_RINVOPTPARSTREAM, 0x12345678)

        resp = SomeRequestRespPDU.create(error_codes.ESME_ROK)
        resp.throw_if_error()

        resp = SomeRequestRespPDU.create(error_codes.ESME_RALYBND)
        with expected(SMPPResponseError("SMPP error ESME_RALYBND (ESME Already in Bound State)")):
            resp.throw_if_error()

        resp = SomeRequestRespPDU.create(error_codes.ESME_RUNKNOWNERR)
        with expected(SMPPResponseError("SMPP error ESME_RUNKNOWNERR")):
            resp.throw_if_error()

    test_response()

    ###################################

    def test_no_body_response():

        class NoBodyResponsePDU(ResponsePDU):
            command_id = 0x00000012
            mandatory_parameters = ("system_id", "final_date")
            optional_parameters = ("message_payload", )
        NoBodyResponsePDU.register()

        with expected(SMPPPDUCreateError("failed to create NoBodyResponsePDU: failed to create mandatory parameter system_id: value for system_id is not specified")):
            NoBodyResponsePDU.create()

        with expected(SMPPPDUCreateError("failed to create NoBodyResponsePDU: failed to create mandatory parameter system_id: value for system_id is not specified")):
            NoBodyResponsePDU.create(error_codes.ESME_ROK)

        with expected(SMPPPDUCreateError("failed to create NoBodyResponsePDU: failed to create mandatory parameter final_date: value for final_date is not specified")):
            NoBodyResponsePDU.create(error_codes.ESME_RSUBMITFAIL, 0x00001234, system_id = b"")

        with expected(SMPPPDUCreateError("failed to create NoBodyResponsePDU: failed to create mandatory parameter system_id: value for system_id is not specified")):
            NoBodyResponsePDU.create(error_codes.ESME_RSUBMITFAIL, 0x00001234, message_payload = b"")

        pdu = NoBodyResponsePDU.create(error_codes.ESME_RSUBMITFAIL, 0x00001234)
        assert not hasattr(pdu, "system_id")
        assert not hasattr(pdu, "message_payload")

        assert pdu.serialize() == b"\x00\x00\x00\x10\x00\x00\x00\x12\x00\x00\x00E\x00\x00\x124"
        assert PDU.read(BytesIO(pdu.serialize())) == pdu

        with expected(SMPPPDUReadError("failed to read NoBodyResponsePDU: failed to read mandatory parameter system_id: SMPPCOctetString.read() has encountered unexpected end of stream")):
            PDU.read(BytesIO(b"\x00\x00\x00\x10\x00\x00\x00\x12\x00\x00\x00\x00\x00\x00\x124"))

        ###############################

        class NoBodyRequestPDU(RequestPDU):
            command_id = 0x00000013
            mandatory_parameters = ("system_id", "final_date")
            optional_parameters = ()
        NoBodyRequestPDU.register()

        with expected(SMPPPDUCreateError("failed to create NoBodyRequestPDU: failed to create mandatory parameter system_id: value for system_id is not specified")):
            NoBodyRequestPDU.create()

        with expected(SMPPPDUCreateError("failed to create NoBodyRequestPDU: failed to create mandatory parameter final_date: value for final_date is not specified")):
            NoBodyRequestPDU.create(error_codes.ESME_ROK, system_id = b"")

        with expected(SMPPPDUCreateError("failed to create NoBodyRequestPDU: __init__() has got an incompatible value for command_status: 68")):
            NoBodyRequestPDU.create(error_codes.ESME_RCNTSUBDL)

        pdu = NoBodyRequestPDU.create(error_codes.ESME_ROK, 0x23456789, system_id = b"", final_date = b"")
        assert pdu.system_id == SMPPCOctetString(b"")
        assert pdu.final_date == SMPPCOctetString(b"")

        assert pdu.serialize() == b"\x00\x00\x00\x12\x00\x00\x00\x13\x00\x00\x00\x00\x23\x45\x67\x89\x00\x00"
        assert PDU.read(BytesIO(pdu.serialize())) == pdu

        with expected(SMPPPDUReadError("failed to read NoBodyRequestPDU: __init__() has got an incompatible value for command_status: 98")):
            PDU.read(BytesIO(b"\x00\x00\x00\x12\x00\x00\x00\x13\x00\x00\x00\x62\x23\x45\x67\x89\x00\x00"))

    test_no_body_response()

    ###################################

    def test_null_length():

        class MessagePDU(PDU):
            command_id = 0x00000014
            mandatory_parameters = ("sm_length", "short_message|sm_length")
            optional_parameters = ("message_payload", )
        MessagePDU.register()

        pdu = MessagePDU.create(short_message = b"")
        assert PDU.read(BytesIO(pdu.serialize())) == pdu

        pdu = MessagePDU.create(short_message = b"", message_payload = b"")
        assert PDU.read(BytesIO(pdu.serialize())) == pdu

        pdu = MessagePDU.create(short_message = b"foo", message_payload = b"")
        assert PDU.read(BytesIO(pdu.serialize())) == pdu

        pdu = MessagePDU.create(short_message = b"", message_payload = b"bar")
        assert PDU.read(BytesIO(pdu.serialize())) == pdu

        pdu = MessagePDU.create(short_message = b"foo", message_payload = b"bar")
        assert PDU.read(BytesIO(pdu.serialize())) == pdu

    test_null_length()

    ###################################

    def test_udh():

        class WithUDHPDU(RequestPDU):
            command_id = 0x00000015
            mandatory_parameters = ("esm_class", "sm_length", "short_message|sm_length")
            optional_parameters = ()
            udh_data_field = "short_message"
        WithUDHPDU.register()

        pdu = WithUDHPDU.create(esm_class = 0x00, udh = ((0x01, b"foo"), (0x02, b"bar")))
        assert pdu.esm_class == SMPPInteger1(0x40)
        assert pdu.short_message == SMPPOctetString(b"\x0a\x01\x03foo\x02\x03bar")
        assert pdu.sm_length == SMPPInteger1(0x0b)
        assert pdu.udh == SMPPUDH((0x01, b"foo"), (0x02, b"bar"))

        pdu2 = WithUDHPDU.read(BytesIO(pdu.serialize()))
        assert pdu2 == pdu
        assert pdu2.udh == pdu.udh

        pdu = WithUDHPDU.create(esm_class = 0x40, udh = (), short_message = b"")
        assert pdu.esm_class == SMPPInteger1(0x40)
        assert pdu.short_message == SMPPOctetString(b"\x00")
        assert pdu.sm_length == SMPPInteger1(1)
        assert pdu.udh == SMPPUDH()

        pdu2 = WithUDHPDU.read(BytesIO(pdu.serialize()))
        assert pdu2 == pdu
        assert pdu2.udh == pdu.udh

        pdu = WithUDHPDU.create(esm_class = 0x00, short_message = b"foo")
        assert pdu.udh is None

        pdu2 = WithUDHPDU.read(BytesIO(pdu.serialize()))
        assert pdu2 == pdu
        assert pdu2.udh is None

        pdu = WithUDHPDU.create(esm_class = 0x40, udh = (SMPPUDHElement(0xff, b""), ), short_message = b"foo")
        assert pdu.short_message == SMPPOctetString(b"\x02\xff\x00foo")
        assert pdu.sm_length == SMPPInteger1(6)
        assert pdu.udh == SMPPUDH((0xff, b""))

        pdu2 = WithUDHPDU.read(BytesIO(pdu.serialize()))
        assert pdu2 == pdu
        assert pdu2.udh == pdu.udh

        pdu = WithUDHPDU.create(esm_class = 0x40, short_message = b"")
        with expected(SMPPPDUReadError, ".*failed to parse UDH.*"):
            WithUDHPDU.read(BytesIO(pdu.serialize()))

    test_udh()

    ###################################

    print("ok")

###############################################################################
# EOF
