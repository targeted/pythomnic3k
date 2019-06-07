#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
###############################################################################
#
# This module contains an implementation of SMPP interface/resource.
#
# Sample SMPP interface configuration (config_interface_smpp_1.py):
#
# config = dict \
# (
# protocol = "smpp",                           # meta
# server_address = ("smsc.domain.com", 1234),  # tcp
# connect_timeout = 5.0,                       # tcp + smpp bind
# response_timeout = 3.0,                      # smpp
# ping_interval = 60.0,                        # smpp optional
# system_id = "system_id",                     # smpp
# password = "password",                       # smpp
# system_type = "PYTHOMNIC3K",                 # smpp
# esme_ton = 0x01,                             # smpp
# esme_npi = 0x01,                             # smpp
# esme_addr = "000000",                        # smpp
# esme_type = "xcvr",                          # smpp, "rcvr", "xcvr" or "xmit"
# )
#
# Sample processing module (interface_smpp_1.py):
#
# def process_request(request, response):
#   req = request["pdu"]
#   if isinstance(req, SubmitSmPDU):
#     pmnc.log(req.short_message.value)
#     response["pdu"] = req.create_response(message_id = "123456")
#
# Sample SMPP resource configuration (config_resource_smpp_1.py)
#
# config = dict \
# (
# protocol = "smpp",       # meta
# source_addr_ton = 0x00,  # smpp
# source_addr_npi = 0x00,  # smpp
# source_addr = "",        # smpp
# asynchronous = False,    # smpp, do not wait for submit responses
# pack_7bit = False,       # smpp, pack 7-bit characters to septets
# frag_method = None,      # smpp, "udh", "sar" or None
# )
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.smpp_1.submit_sm(dest_addr_ton = 0x01, dest_addr_npi = 0x01,
#                     destination_addr = "12345", short_message = "test")
# message_id = xa.execute()[0]
#
# or if the only transaction participant:
#
# message_id = pmnc.transaction.smpp_1.submit_sm(...)
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource" ]

###############################################################################

import threading; from threading import current_thread, Lock, Event
import select; from select import select, error as select_error
import socket; from socket import socket, AF_INET, SOCK_STREAM
import datetime; from datetime import datetime, timedelta
import struct; from struct import pack

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, typecheck_with_exceptions, \
                  optional, one_of, callable, either, with_attr, list_of
import interlocked_queue; from interlocked_queue import InterlockedQueue
import interlocked_counter; from interlocked_counter import InterlockedCounter
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, \
                           ResourceError, ResourceInputParameterError
import pmnc.threads; from pmnc.threads import HeavyThread
import smpp34.smpp_tools; from smpp34.smpp_tools import byte, encode_gsm7, encode_ucs2
import smpp34.smpp_errors; from smpp34.smpp_errors import error_codes, SMPPPDUReadError
import smpp34.smpp_pdu; from smpp34.smpp_pdu import *
import smpp34.smpp_pdus; from smpp34.smpp_pdus import *

###############################################################################

class InflightRequests:

    _request_ttl = timedelta(minutes = 10)

    def __init__(self):
        self._lock = Lock()
        self._pdus = {}
        self._cleanup_timeout = Timeout(60.0)

    def _cleanup(self):
        now = datetime.now()
        if self._cleanup_timeout.expired:
            try:
                self._pdus = { k: pdu_exp for k, pdu_exp in self._pdus.items() if now < pdu_exp[1] }
            finally:
                self._cleanup_timeout.reset()
        return now

    @typecheck
    def add(self, pdu: RequestPDU):
        with self._lock:
            now = self._cleanup()
            self._pdus[pdu.sequence_number] = (pdu, now + self._request_ttl)

    @typecheck
    def remove(self, pdu: PDU) -> optional(RequestPDU):
        with self._lock:
            self._cleanup()
            pdu_exp = self._pdus.pop(pdu.sequence_number, None)
            if pdu_exp: return pdu_exp[0]

###############################################################################

@typecheck
def _wait_response(req: RequestPDU, timeout: float) -> ResponsePDU:
    resp = req.wait_response(timeout)
    if not resp:
        raise Exception("timeout waiting for response to {0:s}".format(req.__class__.__name__))
    resp.throw_if_error()
    return resp

###############################################################################

class _SMPPConnection:

    @typecheck
    def __init__(self, name: str,
                 in_q: with_attr("push"),
                 out_q: with_attr("pop"),
                 inflight: with_attr("add", "remove"),
                 *,
                 server_address: (str, int),
                 connect_timeout: float,
                 response_timeout: float,
                 system_id: str,
                 password: str,
                 system_type: str,
                 esme_ton: byte,
                 esme_npi: byte,
                 esme_addr: str,
                 bind_pdu: one_of(BindReceiverPDU, BindTransmitterPDU, BindTransceiverPDU)):

        self._name = name

        # input and output queues and inflight requests registry are external
        # to the connection object and remain persistent across disconnects to
        # prevent losing packets

        self._in_q = in_q
        self._out_q = out_q
        self._inflight = inflight

        self._server_address = server_address
        self._connect_timeout = connect_timeout
        self._response_timeout = response_timeout

        self._bind_pdu = \
            bind_pdu.create(system_id = system_id.encode("ascii", "replace"),
                            password = password.encode("ascii", "replace"),
                            system_type = system_type.encode("ascii", "replace"),
                            interface_version = 0x34,
                            addr_ton = esme_ton,
                            addr_npi = esme_npi,
                            address_range = esme_addr.encode("ascii", "replace"))

        self._bound = Event()
        self._failed = Event()

    failed = property(lambda self: self._failed.is_set())

    ###################################

    def start(self):
        self._bind_timeout = Timeout(self._connect_timeout)
        self._connect(self._bind_timeout.remain)
        self._start_threads()
        try:
            self._wait_for_bind()
        except:
            self.stop()
            raise

    def stop(self):
        self._stop_threads()
        self._disconnect()

    ###################################

    def _connect(self, timeout):
        s = socket(AF_INET, SOCK_STREAM)
        try:
            s.settimeout(timeout or 0.01)
            s.connect(self._server_address)
            s.setblocking(False)
        except:
            s.close()
            raise
        else:
            self._socket = s

    def _disconnect(self):
        try:
            self._socket.close()
        except:
            pmnc.log(exc_string()) # log and ignore

    ###################################

    def _start_threads(self):
        self._reader = HeavyThread(target = self._reader_proc, name = "{0:s}/rd".format(self._name))
        self._reader.start()
        self._writer = HeavyThread(target = self._writer_proc, name = "{0:s}/wr".format(self._name))
        self._writer.start()

    def _stop_threads(self):
        self._writer.stop() # the writer thread must be stopped first, because it depends
        self._reader.stop() # on the reader thread to be active to perform unbind

    ###################################

    def _wait_for_bind(self):
        while not self._bind_timeout.expired:
            self._bound.wait(min(0.5, self._bind_timeout.remain)) # resort to polling because two
            if self._bound.is_set():                              # events need to be waited upon
                break
            if self._failed.is_set():
                raise Exception("binding attempt failed")
        else:
            raise Exception("binding attempt timed out")

    ###################################

    class ThreadStopped(Exception): pass

    ###################################

    def read(self, n):
        while len(self._read_data) < n:
            ss = select([self._socket], [], [], 1.0)[0]
            if current_thread().stopped():                                        # this particular exception is accounted for
                raise _SMPPConnection.ThreadStopped("connection is being closed") # and masked out in _reader_proc below
            if not ss: continue
            self._read_data += ss[0].recv(4096)
        result, self._read_data = self._read_data[:n], self._read_data[n:]
        return result

    ###################################

    def _reader_proc(self):

        self._read_data = b""

        while True:
            try:

                try:
                    pdu = PDU.read(self)
                except SMPPPDUReadError as e:                                  # mask out the exception from
                    if isinstance(e.__cause__, _SMPPConnection.ThreadStopped): # the thread being stopped
                        break # while
                    else:
                        raise

                if isinstance(pdu, RequestPDU):
                    if pmnc.log.debug:
                        pmnc.log.debug("<< [REQ] {0:s}".format(str(pdu)))
                    self._in_q.push(pdu)
                elif isinstance(pdu, ResponsePDU):
                    req = self._inflight.remove(pdu)
                    if req is not None:
                        if pmnc.log.debug:
                            pmnc.log.debug("<< [RSP] {0:s}".format(str(pdu)))
                        req.set_response(pdu)
                    else:
                        pmnc.log.warning("<< [RSP?!] {0:s}".format(str(pdu)))
                else:
                    pmnc.log.warning("<< [???] {0:s}".format(str(pdu)))

            except:
                pmnc.log.error(exc_string())
                self._failed.set()
                break

    ###################################

    def _writer_proc(self):
        try:

            # perform synchronous bind, using cumulative timeout

            if not self._write_pdu(self._bind_pdu, False, self._bind_timeout):
                return
            _wait_response(self._bind_pdu, self._bind_timeout.remain)
            self._bound.set()

            while not current_thread().stopped(): # lifetime loop
                pdu = self._out_q.pop(1.0)
                if pdu and not self._write_pdu(pdu, False, Timeout(self._response_timeout)):
                    break

            # perform synchronous unbind

            unbind_pdu = UnbindPDU.create()
            unbind_timeout = Timeout(self._response_timeout)
            self._write_pdu(unbind_pdu, True, unbind_timeout)
            _wait_response(unbind_pdu, unbind_timeout.remain)

        except:
            pmnc.log.error(exc_string())
            self._failed.set()

    ###################################

    def _write_pdu(self, pdu, stopped, timeout):

        request_pdu = isinstance(pdu, RequestPDU)

        if request_pdu: self._inflight.add(pdu)
        try:
            self._write_packet(pdu.serialize(), stopped, timeout)
        except _SMPPConnection.ThreadStopped:
            if request_pdu: self._inflight.remove(pdu)
            return False
        except:
            if request_pdu: self._inflight.remove(pdu)
            raise

        if pmnc.log.debug:
            if request_pdu:
                pmnc.log.debug(">> [REQ] {0:s}".format(str(pdu)))
            elif isinstance(pdu, ResponsePDU):
                pmnc.log.debug(">> [RSP] {0:s}".format(str(pdu)))
            else:
                pmnc.log.debug(">> [???] {0:s}".format(str(pdu)))

        return True

    ###################################

    def _write_packet(self, packet, stopped, timeout):
        while packet:
            ss = select([], [self._socket], [], min(timeout.remain, 1.0))[1]
            if current_thread().stopped() and not stopped:
                raise _SMPPConnection.ThreadStopped("connection is being closed")
            if timeout.expired:
                raise Exception("timeout writing packet")
            if ss:
                sent = ss[0].send(packet)
                packet = packet[sent:]

###############################################################################

class Interface: # SMPP interface

    @typecheck
    def __init__(self, name: str, *,
                 server_address: (str, int),
                 connect_timeout: float,
                 response_timeout: float,
                 ping_interval: optional(float),
                 system_id: str,
                 password: str,
                 system_type: str,
                 esme_ton: byte,
                 esme_npi: byte,
                 esme_addr: str,
                 esme_type: one_of("rcvr", "xmit", "xcvr"),
                 request_timeout: optional(float) = None,
                 **kwargs): # this kwargs allows for extra application-specific
                            # settings in config_interface_smpp_X.py

        self._name = name
        self._response_timeout = response_timeout

        if ping_interval:
            self._ping_timeout = Timeout(ping_interval)
            self._ping_response_timeout = Timeout(response_timeout)
        else:
            self._ping_timeout = self._ping_response_timeout = None
        self._ping_request = None

        self._in_q = InterlockedQueue()
        self._out_q = InterlockedQueue()
        self._inflight = InflightRequests()
        self._ceased = Event()

        if esme_type == "rcvr":
            bind_pdu = BindReceiverPDU
        elif esme_type == "xmit":
            bind_pdu = BindTransmitterPDU
        elif esme_type == "xcvr":
            bind_pdu = BindTransceiverPDU

        self._create_connection = \
            lambda: _SMPPConnection(name, self._in_q, self._out_q, self._inflight,
                                    server_address = server_address,
                                    connect_timeout = connect_timeout,
                                    response_timeout = response_timeout,
                                    system_id = system_id,
                                    password = password,
                                    system_type = system_type,
                                    esme_ton = esme_ton,
                                    esme_npi = esme_npi,
                                    esme_addr = esme_addr,
                                    bind_pdu = bind_pdu)

        self._request_timeout = request_timeout or \
            pmnc.config_interfaces.get("request_timeout") # this is now static

        if pmnc.request.self_test == __name__: # self-test
            self._process_request = kwargs["process_request"]

    name = property(lambda self: self._name)
    ceased = property(lambda self: self._ceased.is_set())

    ###################################

    def start(self):
        self._maintainer = HeavyThread(target = self._maintainer_proc, name = "{0:s}/mnt".format(self.name))
        self._maintainer.start()

    def cease(self):
        self._ceased.set()
        self._maintainer.stop()

    def stop(self):
        pass

    ###################################

    def _maintainer_proc(self):

        while not current_thread().stopped():
            try:

                # try to establish a connection, do it infinitely or until the interface is stopped

                while True:
                    try:
                        self._connection = self._create_connection()
                        self._connection.start()
                    except:
                        pmnc.log.error(exc_string())
                        failure_timeout = max(self._request_timeout, 30.0)
                        if current_thread().stopped(failure_timeout):
                            return
                    else:
                        break # while True

                try:

                    while not current_thread().stopped() and not self._connection.failed:

                        # process incoming PDUs

                        req = self._in_q.pop(1.0)
                        if req is not None:
                            self._handle_pdu(req)

                        # if there is an outstanding ping request, check for response

                        if self._ping_request and self._ping_response_timeout.expired:
                            ping_request, self._ping_request = self._ping_request, None
                            _wait_response(ping_request, 0.001)

                        # if it's time to send another ping request, do so

                        if self._ping_timeout and self._ping_timeout.expired:
                            try:
                                self._ping_request = EnquireLinkPDU.create()
                                self._out_q.push(self._ping_request)
                                self._ping_response_timeout.reset()
                            finally:
                                self._ping_timeout.reset()

                finally:
                    self._connection.stop()

            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################
    # this method processes the request PDUs received by this interface from SMSC

    def _handle_pdu(self, req):

        if isinstance(req, EnquireLinkPDU): # respond to pings automatically

            resp = req.create_response()
            self._out_q.push(resp)

        else: # note that this interface does not wait for its requests to complete

            request = pmnc.interfaces.begin_request(
                        timeout = self._request_timeout,
                        interface = self._name, protocol = "smpp",
                        parameters = dict(auth_tokens = dict()),
                        description = "incoming {0:s}".format(req))

            pmnc.interfaces.enqueue(request, self.wu_handle_pdu, (req, ), {})

    ###################################

    @typecheck
    def wu_handle_pdu(self, req: RequestPDU):

        try:

            # see for how long the request was on the execution queue up to this moment
            # and whether it has expired in the meantime, if it did there is no reason
            # to proceed and we simply bail out

            if pmnc.request.expired:
                pmnc.log.error("request has expired and will not be processed")
                success = False
                return # goes through finally section below

            with pmnc.performance.request_processing():
                request = dict(pdu = req)
                response = dict(pdu = req.create_nack(error_codes.ESME_RUNKNOWNERR))
                try:
                    self._process_request(request, response)
                except:
                    response["pdu"] = req.create_nack(error_codes.ESME_RSYSERR)
                    raise
                finally:
                    self._out_q.push(response["pdu"])

        except:
            pmnc.log.error(exc_string()) # log and ignore
            success = False
        else:
            success = True
        finally:                                 # the request ends itself
            pmnc.interfaces.end_request(success) # possibly way after deadline

    ###################################

    def _process_request(self, request, response):
        handler_module_name = "interface_{0:s}".format(self._name)
        pmnc.__getattr__(handler_module_name).process_request(request, response)

    ###################################
    # this method is called by the coupled resources to send a request PDU to SMSC

    @typecheck
    def send(self, req: RequestPDU, timeout: optional(float) = None) -> optional(ResponsePDU):
        self._out_q.push(req)
        if timeout is not None:
            return _wait_response(req, min(timeout, self._response_timeout))

###############################################################################

class Resource(TransactionalResource): # SMPP resource

    @typecheck
    def __init__(self, name: str, *,
                 source_addr_ton: byte,
                 source_addr_npi: byte,
                 source_addr: str,
                 asynchronous: bool,
                 pack_7bit: optional(bool) = None,
                 frag_method: optional(one_of("sar", "udh")) = None):

        TransactionalResource.__init__(self, name)

        self._interface_name = name.split("/", 1)[0]
        self._source_addr_ton = source_addr_ton
        self._source_addr_npi = source_addr_npi
        self._source_addr = source_addr
        self._asynchronous = asynchronous
        self._pack_7bit = pack_7bit or False
        self._frag_method = frag_method

        self._interface = None

    ###################################

    def _expired(self):
        return self._interface is None or self._interface.ceased or \
               TransactionalResource._expired(self)

    ###################################

    def connect(self):
        TransactionalResource.connect(self)
        self._interface = pmnc.interfaces.get_interface(self._interface_name)
        if self._interface is None:
            raise Exception("interface {0:s} is unavailable".format(self._interface_name))

    ###################################

    def _encode_message(self, s: str) -> (byte, bytes):
        try:
            return 0x00, encode_gsm7(s, self._pack_7bit)
        except ValueError:
            return 0x08, encode_ucs2(s)

    ###################################

    # fragmentation is a complex matter because the encoded characters
    # could not span fragments (and there are other pecularities)

    @staticmethod
    @typecheck
    def _frag_message(message: bytes, bits_per_char: optional(int) = 8,
                      frag_bytes: optional(int) = 140, prefix_bytes: optional(int) = 0) -> list_of(bytes):

        if not message:
            return [ message ]

        bpc = bits_per_char
        pad_bits = (bpc - prefix_bytes * 8 % bpc) % bpc if (bpc < 8 and prefix_bytes > 0) else 0 # 7 bit left padding to septet

        mbpf = frag_bytes - prefix_bytes - (1 if pad_bits > 0 else 0) # maximum bytes per fragment we could squeeze

        # each fragment should contain full characters, with 7 bit encoding
        # we round each fragment up to 7 bytes to guarantee that

        bpp = bpc if bpc % 8 != 0 else bpc // 8
        if mbpf < bpp:
            raise Exception("fragment size {0:d} is too small".format(frag_bytes))
        bpf = mbpf // bpp * bpp

        # note that escape sequences are not honored and could be split

        frags, frag, message = [], message[:bpf], message[bpf:]
        while frag:

            # each fragment is padded independently

            if pad_bits > 0:
                pbb, pbm, nbb = pad_bits, (1 << pad_bits) - 1, 8 - pad_bits
                pb, fb = 0, []
                for b in frag:
                    fb.append(pb << nbb | b >> pbb)
                    pb = b & pbm
                fb.append(pb << nbb)
                frag = bytes(fb)

            frags.append(frag)
            frag, message = message[:bpf], message[bpf:]

        return frags

    ###################################

    _frag_size = 140
    _encoding_bits = { 0x00: 7, 0x01: 7, 0x08: 16 } # only non-8 bit encodings
    _frag_count = InterlockedCounter(0x10000)

    ###################################

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def submit_sm(self, *, dest_addr_ton: byte, dest_addr_npi: byte, destination_addr: str,
                  short_message: either(str, bytes), **kwargs) -> optional(str):

        try:

            destination_addr_b = destination_addr.encode("ascii", "replace")

            kwargs.setdefault("service_type", b"")
            kwargs.setdefault("source_addr_ton", self._source_addr_ton)
            kwargs.setdefault("source_addr_npi", self._source_addr_npi)
            kwargs.setdefault("source_addr", self._source_addr.encode("ascii", "replace"))
            kwargs.setdefault("esm_class", 0x00)
            kwargs.setdefault("protocol_id", 0x00)
            kwargs.setdefault("priority_flag", 0x00)
            kwargs.setdefault("schedule_delivery_time", b"")
            kwargs.setdefault("validity_period", b"")
            kwargs.setdefault("registered_delivery", 0x00)
            kwargs.setdefault("replace_if_present_flag", 0x00)
            kwargs.setdefault("sm_default_msg_id", 0x00)

            # if message text is provided as str, encode it to bytes using default
            # GSM7/UCS2 encoding, otherwise require the encoding to be specified

            if isinstance(short_message, str):
                if "data_coding" in kwargs:
                    raise Exception("data_coding is specified, provide short_message of type bytes")
                data_coding, short_message_b = self._encode_message(short_message)
                kwargs["data_coding"] = data_coding
            elif "data_coding" in kwargs:
                short_message_b = short_message
            else:
                raise Exception("data_coding is not specified, provide short_message of type str")

            if len(short_message_b) > self._frag_size and self._frag_method: # the message needs to be fragmented

                bits_per_char = Resource._encoding_bits.get(kwargs["data_coding"], 8)

                if bits_per_char < 8 and not self._pack_7bit:
                    bits_per_char = 8 # 7-bit characters are treated by some providers as octets, not septets

                msg_id = self._frag_count.next() # with 16 bits having a counter is better than picking at random

                if self._frag_method == "udh": # UDH is prepended to each fragment

                    udh = kwargs.pop("udh", []) # sender could have provided his own UDH

                    frags = self._frag_message(short_message_b, bits_per_char, self._frag_size, 7) # 7 bytes are reserved to UDH
                    reqs = [ SubmitSmPDU.create(dest_addr_ton = dest_addr_ton,
                                                dest_addr_npi = dest_addr_npi,
                                                destination_addr = destination_addr_b,
                                                short_message = short_message_b,
                                                udh = udh + [ (0x08, pack("BBBB", msg_id >> 8, msg_id & 0xff, len(frags), i + 1)) ],
                                                **kwargs)
                             for i, short_message_b in enumerate(frags) ]

                elif self._frag_method == "sar": # sar_... optional parameters are added to each fragment

                    kwargs.setdefault("sar_msg_ref_num", msg_id) # sender could have provided his own value

                    frags = self._frag_message(short_message_b, bits_per_char, self._frag_size)
                    reqs = [ SubmitSmPDU.create(dest_addr_ton = dest_addr_ton,
                                                dest_addr_npi = dest_addr_npi,
                                                destination_addr = destination_addr_b,
                                                short_message = short_message_b,
                                                sar_segment_seqnum = i + 1,
                                                sar_total_segments = len(frags),
                                                **kwargs)
                             for i, short_message_b in enumerate(frags) ]

            else: # no fragmentation required or possible

                reqs = [ SubmitSmPDU.create(dest_addr_ton = dest_addr_ton,
                                            dest_addr_npi = dest_addr_npi,
                                            destination_addr = destination_addr_b,
                                            short_message = short_message_b,
                                            **kwargs) ]

        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

        try:

            pmnc.log.info("sending short message \"{0:s}\" for {1:s}".\
                          format(short_message, destination_addr))
            try:

                # send all the fragments

                message_id = None
                for req in reqs:
                    self._interface.send(req)

                # the responses are waited upon after all the requests have been sent

                if not self._asynchronous:
                    for req in reqs:
                        resp = _wait_response(req, pmnc.request.remain)
                        resp_id = resp.message_id.value.decode("ascii", "replace")
                        if resp_id and resp_id != message_id:
                            message_id = resp_id # the last different message_id is returned

            except:
                pmnc.log.warning("sending short message \"{0:s}\" for {1:s} failed: {2:s}".\
                                 format(short_message, destination_addr, exc_string()))
                raise
            else:
                pmnc.log.info("short message has been sent{0:s}".\
                              format(" as {0:s}".format(message_id) if message_id else ""))

        except:
            ResourceError.rethrow(recoverable = False, terminal = False)

        return message_id # possibly None, if sending asynchronously

###############################################################################

def self_test():

    from time import time, sleep
    from expected import expected
    from pmnc.request import fake_request
    from smpp34.smpp_errors import SMPPResponseError
    from pmnc.self_test import active_interface

    ###################################

    test_interface_config = dict \
    (
    protocol = "smpp",
    server_address = ("smsc.domain.com", 1234),
    connect_timeout = 5.0,
    response_timeout = 5.0,
    ping_interval = 60.0,
    system_id = "user",
    password = "pass",
    system_type = "PYTHOMNIC3K",
    esme_ton = 0x01,
    esme_npi = 0x01,
    esme_addr = "",
    esme_type = "xcvr",
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    def test_InflightRequests():

        ir = InflightRequests()
        ir._request_ttl = timedelta(seconds = 3)
        ir._cleanup_timeout = Timeout(1.0)

        pdu = EnquireLinkPDU.create()
        ir.add(pdu)
        assert ir.remove(pdu) is pdu
        assert ir.remove(pdu) is None

        ir.add(pdu)
        assert ir.remove(pdu.create_response()) is pdu

        ir.add(pdu)
        sleep(5.0)
        assert ir.remove(pdu) is None

    test_InflightRequests()

    ###################################

    def test_fragmentation():

        assert Resource._frag_message(b"") == [ b"" ]

        # --- 7 bit encoding ---

        # left padding to septet

        assert Resource._frag_message(b"\x11", 7, 140, 0) == [ b"\x11" ]
        assert Resource._frag_message(b"\xff", 7, 140, 1) == [ b"\x03\xfc" ]
        assert Resource._frag_message(b"\x5a\x5a", 7, 140, 2) == [ b"\x02\xd2\xd0" ]
        assert Resource._frag_message(b"\x01\x02\x04", 7, 140, 3) == [ b"\x00\x10\x20\x40" ]
        assert Resource._frag_message(b"\xfe", 7, 140, 4) == [ b"\x1f\xc0" ]
        assert Resource._frag_message(b"\xff\xff\xff", 7, 140, 5) == [ b"\x3f\xff\xff\xc0" ]
        assert Resource._frag_message(b"\x01\x80", 7, 140, 6) == [ b"\x00\xc0\x00" ]
        assert Resource._frag_message(b"\x11", 7, 140, 7) == [ b"\x11" ]

        # fragmentation

        assert Resource._frag_message(b"\x01\x02\x03\x04\x05\x06\x07", 7, 7) == \
               [ b"\x01\x02\x03\x04\x05\x06\x07" ]

        assert Resource._frag_message(b"\x01\x02\x03\x04\x05\x06\x07\x08", 7, 7) == \
               [ b"\x01\x02\x03\x04\x05\x06\x07", b"\x08" ]

        with expected(Exception("fragment size 8 is too small")):
            Resource._frag_message(b"\xff", 7, 8, 1)

        assert Resource._frag_message(b"\xff\xff\xff\xff\xff\xff\xff", 7, 9, 1) == \
               [ b"\x03\xff\xff\xff\xff\xff\xff\xfc" ]

        assert Resource._frag_message(b"\xff\xff\xff\xff\xff\xff\xff\xff", 7, 9, 1) == \
               [ b"\x03\xff\xff\xff\xff\xff\xff\xfc", b"\x03\xfc" ]

        # --- 8 bit encoding ---

        assert Resource._frag_message(b"\x01\x02", 8, 2) == \
               [ b"\x01\x02" ]

        assert Resource._frag_message(b"\x01\x02", 8, 2, 1) == \
               [ b"\x01", b"\x02" ]

        # --- 16 bit encoding ---

        assert Resource._frag_message(b"\x12\x34", 16) == [ b"\x12\x34" ]

        with expected(Exception("fragment size 1 is too small")):
            Resource._frag_message(b"\x12\x34", 16, 1)

        with expected(Exception("fragment size 2 is too small")):
            Resource._frag_message(b"\x12\x34", 16, 2, 1)

        assert Resource._frag_message(b"\x01\x02\x03\x04", 16, 3) == \
               [ b"\x01\x02", b"\x03\x04" ]

    test_fragmentation()

    ###################################

    def test_wait_response():

        pdu = EnquireLinkPDU.create()
        with expected(Exception("timeout waiting for response to EnquireLinkPDU")):
            _wait_response(pdu, 1.0)

        resp = pdu.create_response()
        pdu.set_response(resp)
        before = time()
        assert _wait_response(pdu, 0.1) is resp
        after = time()
        assert after - before < 0.01

        resp = pdu.create_nack(error_codes.ESME_RUNKNOWNERR)
        pdu.set_response(resp)
        with expected(SMPPResponseError("SMPP error ESME_RUNKNOWNERR (Unknown Error)")):
            _wait_response(pdu, 0.1)

    test_wait_response()

    ###################################

    DEST_TON = 0x05
    DEST_NPI = 0x00
    DEST_ADDR = "79876543210"

    russian = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"

    ###################################

    def start_interface():
        config = pmnc.config_interface_smpp_1.copy()
        ifc = pmnc.interface.create("smpp_1", **config)
        ifc.start()
        return ifc

    ###################################

    def test_start_stop():

        def process_request(request, response):
            pass

        with active_interface("smpp", **interface_config(process_request = process_request)):
            sleep(10.0)

    test_start_stop()

    ###################################

    def test_no_interface():

        fake_request(10.0)

        with expected(ResourceError, "^interface smpp_1 is unavailable$"):
            pmnc.transaction.smpp_1.send(0x00, 0x00, "", "test")

    test_no_interface()

    ###################################

    def test_send_one():

        def process_request(request, response):
            pass

        with active_interface("smpp_1", **interface_config(process_request = process_request)):

            sleep(3.0) # to allow connection to spin up

            fake_request(30.0)

            with expected(ResourceError, "^data_coding is not specified, provide short_message of type str$"):
                pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = b"test")

            with expected(ResourceError, "^data_coding is specified, provide short_message of type bytes$"):
                pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = "test", data_coding = 0x00)

            pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = b"test1", data_coding = 0x00)

            pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = "test2") # encoded to GSM7

            pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = "@$\\") # encoded to GSM7

            pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = russian) # encoded to UCS2

    test_send_one()

    ###################################

    def test_send_many():

        def process_request(request, response):
            pass

        with active_interface("smpp_1", **interface_config(process_request = process_request)):

            sleep(3.0) # to allow connection to spin up

            fake_request(30.0)

            xa = pmnc.transaction.create()
            xa.smpp_1.submit_sm(dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI, destination_addr = DEST_ADDR, short_message = "test1")
            xa.smpp_1.submit_sm(dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI, destination_addr = DEST_ADDR, short_message = "test2")
            xa.smpp_1.submit_sm(dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI, destination_addr = DEST_ADDR, short_message = "test3")
            xa.execute()

            sleep(10.0)

    test_send_many()

    ###################################

    def test_send_fragmented():

        def process_request(request, response):
            pass

        with active_interface("smpp_1", **interface_config(process_request = process_request)):

            sleep(3.0) # to allow connection to spin up

            fake_request(30.0)

            pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = "BEGIN" + "o" * 170 + "END")

            # with automatic encoding 80 backslashes become 160 gsm7 bytes

            pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = "BEGIN" + "\\" * 80 + "END")

            # with automatic encoding 132 international chars becomes 264 unicode bytes

            pmnc.transaction.smpp_1.submit_sm(
                        dest_addr_ton = DEST_TON, dest_addr_npi = DEST_NPI,
                        destination_addr = DEST_ADDR, short_message = russian * 2)

    test_send_fragmented()

    ###################################

    def test_processing():

        def process_request(request, response):
            req = request["pdu"]
            if isinstance(req, QuerySmPDU):
                resp = req.create_response(message_id = req.message_id.value,
                                           final_date = b"",
                                           message_state = 0x03,
                                           error_code = 0x00)
            else:
                raise Exception("not supported")
            response["pdu"] = resp

        with active_interface("smpp_1", **interface_config(process_request = process_request, ping_interval = 600.0)) as ifc:

            sleep(3.0)                     # to allow connection to spin up
            ifc._connection._writer.stop() # to prevent writer from interfering

            fake_request(10.0)

            ###########################

            req = EnquireLinkPDU.create()
            ifc._in_q.push(req)
            resp = ifc._out_q.pop(3.0)

            assert isinstance(resp, EnquireLinkRespPDU)
            assert resp.sequence_number == req.sequence_number

            ###########################

            req = QuerySmPDU.create(message_id = b"RECEIPT",
                                    source_addr_ton = 0x00,
                                    source_addr_npi = 0x01,
                                    source_addr = b"SENDER")
            ifc._in_q.push(req)
            resp = ifc._out_q.pop(3.0)

            assert isinstance(resp, QuerySmRespPDU)
            assert resp.sequence_number == req.sequence_number
            assert resp.message_id == req.message_id
            assert resp.final_date.value == b""
            assert resp.message_state.value == 0x03
            assert resp.error_code.value == 0x00

            ###########################

            req = UnbindPDU.create()
            ifc._in_q.push(req)
            resp = ifc._out_q.pop(3.0)

            assert isinstance(resp, GenericNackPDU)
            assert resp.sequence_number == req.sequence_number
            assert resp.command_status.value == error_codes.ESME_RSYSERR

            ###########################

    test_processing()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
