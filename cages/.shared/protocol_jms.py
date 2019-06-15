#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
###############################################################################
#
# This module contains an implementation of JMS interface/resource.
#
# Sample JMS interface configuration (config_interface_jms_1.py):
#
# config = dict \
# (
# protocol = "jms",                                             # meta
# request_timeout = None,                                       # meta, optional
# java = "C:\\Sun\\SDK\\jdk\\bin\\java.exe",                    # jms
# arguments = ("-Dfile.encoding=windows-1251", ),               # jms
# classpath = "c:\\pythomnic3k\\lib;"
#             "c:\\pythomnic3k\\lib\\jms.jar;"
#             "c:\\pythomnic3k\\lib\\joram_client.jar",         # jms
# jndi = { "java.naming.factory.initial":
#             "org.objectweb.carol.jndi.spi.MultiOrbInitialContextFactory",
#          "java.naming.provider.url": "rmi://1.2.3.4:5678" },  # jms
# factory = "JCF",                                              # jms
# queue = "test.queue",                                         # jms
# username = "",                                                # jms
# password = "",                                                # jms
# )
#
# Sample processing module (interface_jms_1.py):
#
# def process_request(request, response):
#     message_id = request["message_id"]
#     message_text = request["message_text"]
#     correlation_id = request["headers"]["JMSCorrelationID"]
#
# Sample JMS resource configuration (config_resource_jms_1.py)
#
# config = dict \
# (
# protocol = "jms",                                             # meta
# java = "C:\\Sun\\SDK\\jdk\\bin\\java.exe",                    # jms
# arguments = ("-Dfile.encoding=windows-1251", ),               # jms
# classpath = "c:\\pythomnic3k\\lib;"
#             "c:\\pythomnic3k\\lib\\jms.jar;"
#             "c:\\pythomnic3k\\lib\\joram_client.jar",         # jms
# jndi = { "java.naming.factory.initial":
#             "org.objectweb.carol.jndi.spi.MultiOrbInitialContextFactory",
#          "java.naming.provider.url = "rmi://1.2.3.4:5678" },  # jms
# factory = "JCF",                                              # jms
# queue = "test.queue",                                         # jms
# username = "",                                                # jms
# password = "",                                                # jms
# )
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.jms_1.send("message text", JMSCorrelationID = "foo")
# message_id = xa.execute()[0]
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource" ]

###############################################################################

import os; from os import path as os_path, urandom
import binascii; from binascii import b2a_base64, a2b_base64, b2a_hex, crc32
import threading; from threading import current_thread
import re; from re import compile as regex
import time; from time import sleep

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, typecheck_with_exceptions, optional, \
                                        dict_of, with_attr, by_regex, tuple_of
import interlocked_queue; from interlocked_queue import InterlockedQueue
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, \
                                ResourceError, ResourceInputParameterError
import pmnc.popen; from pmnc.popen import popen
import pmnc.threads; from pmnc.threads import HeavyThread, LightThread
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.thread_pool; from pmnc.thread_pool import WorkUnitTimedOut

###############################################################################
# this class encapsulates a JMS message being sent or received from the server,
# (de)serialization on the other side is done by the adapter residing in
# lib/org/pythomnic/jms

class Packet(dict):

    _valid_key = by_regex("^[A-Za-z0-9_]+$")
    _valid_keys = dict_of(_valid_key, str)

    _bol_b = b"4F36095410830A13"
    _eol_b = b"92B4782E3B570FD3"

    @typecheck
    def __init__(self, **params):
        assert self._valid_keys(params)
        self.update(params)

    ###################################

    @classmethod
    @typecheck
    def load_from_stream(cls, stream: with_attr("readline"), bol_b: bytes, eol_b: bytes):

        params = {}

        def decode_line(b):
            key, value = b.split(b"=", 1)
            key, value = key.decode("ascii"), a2b_base64(value).decode("utf-8")
            params[key] = value

        # the output lines may contain garbage emitted by any java
        # library and they are therefore filtered and checksummed

        valid_line = regex(b"^.*(?:" + bol_b + b"|" + cls._bol_b + b")" +
                           b"([0-9A-F]{8})( ?[A-Za-z0-9+/=_]*)" +
                           b"(?:" + eol_b + b"|" + cls._eol_b + b").*$")

        def get_next_line(prev_crc32 = 0):
            while True:
                b = stream.readline()
                if not b:
                    return None
                b = b.rstrip()
                if not b:
                    continue
                valid_parts = valid_line.findall(b)
                if len(valid_parts) != 1:
                    pmnc.log.warning("skipping unexpected output: {0:s}".format(str(b)[2:-1]))
                    continue
                next_crc32, bb = int(valid_parts[0][0], 16), valid_parts[0][1]
                if next_crc32 != crc32(bb, prev_crc32):
                    pmnc.log.warning("skipping broken output: {0:s}".format(str(b)[2:-1]))
                    continue
                return bb, next_crc32

        curr_lines = []
        next_line_crc32 = get_next_line()
        if next_line_crc32 is None:
            return None

        next_line, curr_crc32 = next_line_crc32
        while next_line:
            if next_line.startswith(b" "):
                if curr_lines:
                    curr_lines.append(next_line[1:])
                else:
                    raise Exception("invalid folding")
            else:
                if curr_lines:
                    decode_line(b"".join(curr_lines))
                    del curr_lines[:]
                curr_lines.append(next_line)
            next_line_crc32 = get_next_line(curr_crc32)
            if next_line_crc32 is None:
                raise Exception("unexpected eof")
            next_line, curr_crc32 = next_line_crc32

        if curr_lines:
            decode_line(b"".join(curr_lines))

        return cls(**params)

    ###################################

    @typecheck
    def save_to_stream(self, stream: with_attr("write", "flush"), fold_width: int):

        for k, v in self.items():
            encoded = k.encode("ascii") + b"=" + b2a_base64(v.encode("utf-8")).rstrip()
            first_line, encoded = encoded[:fold_width], encoded[fold_width:]
            stream.write(first_line + b"\n")
            for folded_line in [ b" " + encoded[i:i+fold_width-1]
                                 for i in range(0, len(encoded), fold_width-1) ]:
                stream.write(folded_line + b"\n")

        stream.write(b"\n")
        stream.flush()

###############################################################################

class AdapterHost:

    @typecheck
    def __init__(self, class_name: str, *,
                 java: os_path.isfile,
                 arguments: tuple_of(str),
                 classpath: str,
                 jndi: dict_of(str, str),
                 factory: str,
                 queue: str,
                 username: str,
                 password: str):

        # random line prefix and suffix for packet serialization

        bol = b2a_hex(urandom(8)).decode("ascii").upper()
        eol = b2a_hex(urandom(8)).decode("ascii").upper()

        # compose the executable command line

        self._args = [ java ] + list(arguments) + \
                     [ "-classpath", classpath, class_name,
                       "connection.factory={0:s}".format(factory),
                       "connection.queue={0:s}".format(queue),
                       "stdout.bol={0:s}".format(bol),
                       "stdout.eol={0:s}".format(eol) ]

        if username or password:
            self._args.append("connection.username={0:s}".format(username))
            self._args.append("connection.password={0:s}".format(password))

        self._args.extend("jndi.{0:s}={1:s}".format(*t) for t in jndi.items())

        self._bol_b = bol.encode("ascii")
        self._eol_b = eol.encode("ascii")

        # this set tracks messages that have been processed but
        # not committed on the server due to a failure

        self._processed_messages = set()

    ###################################

    # adapter input from stdin consists of Packet sequence

    def _stdin_writer_proc(self, stdin, stdin_queue):
        try:
            try:
                pkt = stdin_queue.pop()
                while pkt is not None: # this light thread exits upon None in the queue or exception
                    pkt.save_to_stream(stdin, 128)
                    if pkt.get("XPmncResponse") == "COMMIT":
                        message_id = pkt.pop("XPmncMessageID")
                        self._processed_messages.remove(message_id)
                    pkt = stdin_queue.pop()
            finally:
                stdin.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore

    ###################################

    # adapter output from stdout consists of Packet sequence

    def _stdout_reader_proc(self, stdout, stdin_queue):
        try:
            try:
                try:
                    pkt = Packet.load_from_stream(stdout, self._bol_b, self._eol_b)
                    while pkt is not None: # this light thread exits upon eof or exception
                        self._stdout_queue.push(pkt)
                        pkt = Packet.load_from_stream(stdout, self._bol_b, self._eol_b)
                finally:
                    stdout.close()
            finally:
                stdin_queue.push(None) # this releases the associated stdin writer
        except:
            pmnc.log.error(exc_string()) # log and ignore

    ###################################

    # adapter output from stderr is discarded (yet it has to be read)

    def _stderr_reader_proc(self, stderr):
        try:
            try:
                while stderr.read(512): # this light thread exits only upon exception
                    pass
            finally:
                stderr.close()
        except:
            pmnc.log.error(exc_string()) # log and ignore

    ###################################

    # the adapter is considered up until the process exits

    def _adapter_running(self):

        return self._adapter.poll() is None

    ###################################

    # this method starts the adapter process, creates the handling
    # threads and waits for the adapter to report its readiness

    def _start_adapter(self, adapter_usage, adapter_name, start_timeout):

        pmnc.log.info("starting adapter process for {0:s} {1:s}".\
                      format(adapter_usage, adapter_name))

        if pmnc.log.debug:
            pmnc.log.debug("adapter process command line: {0:s}".format(" ".join(self._args)))

        self._adapter = popen(*self._args) # start the java process
        try:

            if pmnc.log.debug:
                pmnc.log.debug("adapter process (pid {0:d}) has started".\
                               format(self._adapter.pid))

            # the process has started and its initialization is underway
            # create light threads for controlling stdin/out/err, these
            # threads are not stopped explicitly but exit when the adapter
            # exits and the pipes break

            self._stdin_queue = InterlockedQueue()
            self._stdin_writer = LightThread(target = self._stdin_writer_proc,
                                             args = (self._adapter.stdin, self._stdin_queue),
                                             name = "{0:s}:stdin".format(adapter_name))
            self._stdin_writer.start()

            self._stdout_queue = InterlockedQueue()
            self._stdout_reader = LightThread(target = self._stdout_reader_proc,
                                              args = (self._adapter.stdout, self._stdin_queue),
                                              name = "{0:s}:stdout".format(adapter_name))
            self._stdout_reader.start()

            self._stderr_queue = InterlockedQueue()
            self._stderr_reader = LightThread(target = self._stderr_reader_proc,
                                              args = (self._adapter.stderr, ),
                                              name = "{0:s}:stderr".format(adapter_name))
            self._stderr_reader.start()

            # wait for the adapter to come up and report readiness

            while not start_timeout.expired:
                pkt = self._stdout_queue.pop(min(0.5, start_timeout.remain))
                if pkt is None: # see whether the adapter has exited
                    if not self._adapter_running():
                        retcode = self._adapter.wait()
                        if retcode is not None:
                            raise Exception("adapter process exited with "
                                            "retcode {0:d}".format(retcode))
                elif "XPmncError" in pkt:
                    raise Exception(pkt["XPmncError"])
                elif pkt.get("XPmncStatus") == "READY":
                    break
                else:
                    raise Exception("adapter process returned invalid status")
            else:
                raise Exception("timeout waiting for adapter process to initialize")

        except:
            self._stop_adapter(start_timeout) # what remained of start timeout is used for stopping
            raise

        pmnc.log.info("adapter process (pid {0:d}) is ready".format(self._adapter.pid))

    ###################################

    # if not exited peacefully, adapter has to be killed

    def _stop_adapter(self, stop_timeout):

        while self._adapter_running() and not stop_timeout.expired:
            sleep(min(0.5, stop_timeout.remain))

        if self._adapter_running():
            pmnc.log.warning("killing runaway adapter process "
                             "(pid {0:d})".format(self._adapter.pid))
            self._adapter.kill()
        else:
            pmnc.log.info("adapter process (pid {0:d}) exited with retcode {1:d}".\
                          format(self._adapter.pid, self._adapter.wait()))

###############################################################################

class Interface(AdapterHost): # JMS interface

    @typecheck
    def __init__(self, name: str, *,
                 java: os_path.isfile,
                 arguments: tuple_of(str),
                 classpath: str,
                 jndi: dict_of(str, str),
                 factory: str,
                 queue: str,
                 username: str,
                 password: str,
                 request_timeout: optional(float) = None,
                 **kwargs): # this kwargs allows for extra application-specific
                            # settings in config_interface_jms_X.py

        self._name = name

        AdapterHost.__init__(self, "org.pythomnic.jms.Receiver",
                             java = java, arguments = arguments, classpath = classpath,
                             jndi = jndi, factory = factory, queue = queue,
                             username = username, password = password)

        self._request_timeout = request_timeout or \
            pmnc.config_interfaces.get("request_timeout") # this is now static

        if pmnc.request.self_test == __name__: # self-test
            self._process_request = kwargs["process_request"]

    name = property(lambda self: self._name)

    ###################################

    def start(self):
        self._maintainer = HeavyThread(target = self._maintainer_proc,
                                       name = "{0:s}:mnt".format(self._name))
        self._maintainer.start()

    def cease(self):
        self._maintainer.stop()

    def stop(self):
        pass

    ###################################

    # this thread keeps the interface up by (re)starting
    # the adapter and its controlling i/o threads

    def _maintainer_proc(self):

        while not current_thread().stopped():

            while True: # keep trying to start the adapter
                try:
                    self._start_adapter("interface", self._name, Timeout(15.0))
                except:
                    pmnc.log.error(exc_string())
                    failure_timeout = max(self._request_timeout, 30.0)
                    if current_thread().stopped(failure_timeout):
                        return
                else:
                    break

            # now that the adapter is running, keep receiving messages
            # until the adapter exits or the interface is ceased

            try:

                try:

                    while self._adapter_running():

                        # even when the queue is idle, the adapter should be sending in a ping once in 3 seconds

                        receive_timeout = Timeout(self._request_timeout + 3.0)
                        pkt = receive_timeout.pop(self._stdout_queue)
                        if pkt is None:
                            raise Exception("adapter process failed to produce a message")

                        if "XPmncError" in pkt: # adapter reports error, abort
                            raise Exception(pkt["XPmncError"])

                        request_id = pkt.pop("XPmncRequestID")

                        if current_thread().stopped(): # any command after shutdown gets an EXIT response
                            response = Packet(XPmncResponse = "EXIT", XPmncRequestID = request_id)
                            self._stdin_queue.push(response)
                            break

                        request = pkt.pop("XPmncRequest")

                        if request == "NOOP": # ping always gets an OK response
                            response = Packet(XPmncResponse = "OK", XPmncRequestID = request_id)
                            self._stdin_queue.push(response)
                        elif request == "RECEIVE": # process an incoming message
                            message_id = pkt["JMSMessageID"]
                            if message_id not in self._processed_messages: # don't process the message again
                                success = self._process_message(message_id, pkt, receive_timeout.remain)
                            else:
                                success = True
                            response = Packet(XPmncResponse = success and "COMMIT" or "ROLLBACK",
                                              XPmncRequestID = request_id, XPmncMessageID = message_id)
                            self._stdin_queue.push(response)
                        else:
                            raise Exception("invalid request")

                finally:
                    self._stop_adapter(Timeout(5.0)) # make sure the adapter is not running

            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################

    # request processing in a message-oriented interface is rather straightforward

    def _process_message(self, message_id, pkt, request_timeout):

        message_text = pkt.pop("XPmncMessageText")

        message_description = "JMS message {0:s}{1:s}".format(message_id,
                              ", corr.id {0:s}".format(pkt["JMSCorrelationID"])
                              if pkt.get("JMSCorrelationID") else "")

        request = pmnc.interfaces.begin_request(
                    timeout = min(request_timeout, self._request_timeout),
                    interface = self._name, protocol = "jms",
                    parameters = dict(auth_tokens = dict()),
                    description = message_description)
        try:
            pmnc.interfaces.enqueue(request, self.wu_process_request,
                                    (message_id, message_text, pkt.copy())).wait()
        except WorkUnitTimedOut:
            pmnc.log.error("{0:s} processing timed out".format(message_description))
            success = None
        except:
            pmnc.log.error("{0:s} processing failed: {1:s}".\
                           format(message_description, exc_string()))
            success = False
        else:
            if pmnc.log.debug:
                pmnc.log.debug("{0:s} processing succeeded".format(message_description))
            self._processed_messages.add(message_id)
            success = True
        finally:
            pmnc.interfaces.end_request(success, request)
            return success == True

    ###################################

    # this method is a work unit executed by one of the interface pool threads
    # if this method fails, the exception is rethrown in _process_message in wait()

    @typecheck
    def wu_process_request(self, message_id: str, message_text: str, headers: dict):

        # see for how long the request was on the execution queue up to this moment
        # and whether it has expired in the meantime, if it did there is no reason
        # to proceed and we simply bail out

        if pmnc.request.expired:
            pmnc.log.error("request has expired and will not be processed")
            return

        try:
            with pmnc.performance.request_processing():
                request = dict(message_id = message_id, message_text = message_text, headers = headers)
                self._process_request(request, {})
        except:
            pmnc.log.error(exc_string()) # don't allow an exception to be silenced
            raise                        # when this work unit is not waited upon

    ###################################

    def _process_request(self, request, response):
        handler_module_name = "interface_{0:s}".format(self._name)
        pmnc.__getattr__(handler_module_name).process_request(request, response)

###############################################################################

class Resource(TransactionalResource, AdapterHost): # JMS resource

    @typecheck
    def __init__(self, name: str, *,
                 java: os_path.isfile,
                 arguments: tuple_of(str),
                 classpath: str,
                 jndi: dict_of(str, str),
                 factory: str,
                 queue: str,
                 username: str,
                 password: str):

        TransactionalResource.__init__(self, name)
        AdapterHost.__init__(self, "org.pythomnic.jms.Sender",
                             java = java, arguments = arguments, classpath = classpath,
                             jndi = jndi, factory = factory, queue = queue,
                             username = username, password = password)
        self._request_count = 0

    ###################################

    def _expired(self):
        return not self._adapter_running() or \
               TransactionalResource._expired(self)

    ###################################

    # to initiate the JMS "connection" a sender adapter is started

    def connect(self):
        TransactionalResource.connect(self)
        self._start_adapter("resource instance", self.name,
                            Timeout(pmnc.request.remain))

    ###################################

    def begin_transaction(self, *args, **kwargs):
        TransactionalResource.begin_transaction(self, *args, **kwargs)
        self._message_id = None

    ###################################

    # this wraps and sends a message to the adapter and returns
    # the actual id assigned to the message by the queueing server

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def send(self, message_text: str, **kwargs) -> str:

        try:

            header_fields = " and {0:d} custom header field(s): {1:s}".\
                            format(len(kwargs), ", ".join(kwargs.keys())) \
                            if kwargs else ""

            correlation_id = kwargs.get("JMSCorrelationID")

            message_description = \
                "JMS message{0:s} with {1:d} content byte(s){2:s}".\
                format(" {0:s}".format(correlation_id) if correlation_id else "",
                       len(message_text), header_fields)

            pmnc.log.info("sending {0:s}".format(message_description))
            try:
                pkt = self._sync_adapter_command("SEND", XPmncMessageText = message_text, **kwargs)
            except:
                pmnc.log.warning("sending {0:s} failed: {1:s}".\
                                 format(message_description, exc_string()))
                raise
            else:
                self._message_id = pkt["XPmncMessageID"]

        except:
            ResourceError.rethrow(recoverable = True) # no irreversible changes
        else:
            return self._message_id

    ###################################

    # this method sends a commit command to the adapter

    def commit(self):
        self._sync_adapter_command("COMMIT")
        pmnc.log.info("JMS message has been sent as {0:s}".format(self._message_id))

    ###################################

    # this method sends a rollback command to the adapter

    def rollback(self):
        if self._message_id is not None:
            self._sync_adapter_command("ROLLBACK")
            pmnc.log.info("JMS message has been rolled back")

    ###################################

    # this method attempts to gracefully shut down the adapter

    def disconnect(self):
        try:
            try:
                self._sync_adapter_command("EXIT")
            finally:
                self._stop_adapter(Timeout(min(5.0, pmnc.request.remain)))
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            TransactionalResource.disconnect(self)

    ###################################

    # this method sends a command to the sender adapter
    # and synchronously waits for its response

    def _sync_adapter_command(self, request, **kwargs):

        request_id = "{0:d}".format(self._request_count)
        self._request_count += 1

        # push the request to the adapter input queue

        req_pkt = Packet(XPmncRequest = request, XPmncRequestID = request_id, **kwargs)
        self._stdin_queue.push(req_pkt)

        while True: # wait for adapter to respond

            pkt = pmnc.request.pop(self._stdout_queue)
            if pkt is None:
                raise Exception("request deadline waiting for adapter "
                                "response to {0:s}".format(request))

            if pkt.pop("XPmncRequestID") != request_id: # responses to previous requests are skipped
                if "XPmncError" in pkt:
                    pmnc.log.warning(pkt["XPmncError"]) # the errors are logged but not thrown
            elif "XPmncError" in pkt:
                raise Exception(pkt["XPmncError"])
            elif pkt.pop("XPmncResponse") != "OK":
                raise Exception("invalid response to {0:s}".format(request))
            else:
                return pkt

###############################################################################

def self_test():

    from io import BytesIO
    from random import random
    from expected import expected
    from typecheck import InputParameterError
    from pmnc.request import fake_request
    from pmnc.self_test import active_interface
    from threading import Event

    ###################################

    russian = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"

    ###################################

    def test_packet():

        # wrapping is straightforward, because no checksumming is performed

        def wrap(**kwargs):
            p = Packet(**kwargs)
            s = BytesIO()
            p.save_to_stream(s, 5)
            return s.getvalue()

        assert wrap() == b"\n"
        assert wrap(f = "") == b"f=\n\n"
        assert wrap(f = "1") == b"f=MQ=\n =\n\n"
        assert wrap(abcd = "") == b"abcd=\n\n"
        assert wrap(abcde = "") == b"abcde\n =\n\n"
        assert wrap(abcd = "1") == b"abcd=\n MQ==\n\n"
        assert wrap(foobar = "123") == b"fooba\n r=MT\n Iz\n\n"
        assert wrap(f = "", abcd = "1") == b"abcd=\n MQ==\nf=\n\n"
        assert wrap(abcde = "", fghij = "") == b"fghij\n =\nabcde\n =\n\n"
        assert wrap(foo = "*" * 10, AbcdEfghIjkl = "x" * 5) == b"AbcdE\n fghI\n jkl=\n eHh4\n eHg=\nfoo=K\n ioqK\n ioqK\n ioqK\n g==\n\n"
        assert wrap(rus = russian) == b"rus=0\n JDQk\n dCS0\n JPQl\n NCV0\n IHQl\n tCX0\n JjQm\n dCa0\n" \
                                      b" JvQn\n NCd0\n J7Qn\n 9Cg0\n KHQo\n tCj0\n KTQp\n dCm0\n KfQq\n" \
                                      b" NCp0\n KzQq\n 9Cq0\n K3Qr\n tCv0\n LDQs\n dCy0\n LPQt\n NC10\n" \
                                      b" ZHQt\n tC30\n LjQu\n dC60\n LvQv\n NC90\n L7Qv\n 9GA0\n YHRg\n" \
                                      b" tGD0\n YTRh\n dGG0\n YfRi\n NGJ0\n YzRi\n 9GK0\n Y3Rj\n tGP\n\n"

        # unwrapping is trickier because prefix/suffix/checksums are being checked

        bol = b2a_hex(urandom(4))
        eol = b2a_hex(urandom(4))

        def unwrap(b):
            return Packet.load_from_stream(BytesIO(b), bol, eol)

        # the line if prefixed and then optionally mangled

        def line(b):
            b = bol + b + eol
            while True:
                r = random()
                if r < 0.1:
                    return b + b"\r\n"
                elif r < 0.2:
                    return b + b"\n"
                elif r < 0.3:
                    b += b"\r\n"
                elif r < 0.4:
                    b += b"\n"
                elif r < 0.7:
                    b = urandom(1) + b
                else:
                    b = b + urandom(1)

        # see whether it chokes on plain garbage

        assert unwrap(b"") is None
        assert unwrap(b"\n") is None

        assert unwrap(b"garbage") is None
        assert unwrap(b"garbage\n") is None
        assert unwrap(b"gar\n\r\nbage") is None
        assert unwrap(b"gar\r\nbage") is None
        assert unwrap(b"gar\nbage\r\n") is None

        # check that default delimiters work

        assert unwrap(bol + b"00000000" + eol + b"\n") == dict()
        assert unwrap(Packet._bol_b + b"00000000" + eol + b"\n") == dict()
        assert unwrap(bol + b"00000000" + Packet._eol_b + b"\n") == dict()
        assert unwrap(Packet._bol_b + b"00000000" + Packet._eol_b + b"\n") == dict()

        for i in range(20): # to cause all sorts of mangling

            assert unwrap(line(b"00000000")) == dict()

            assert unwrap(line(b"2A1692CFf=") + line(b"2A1692CF")) == dict(f = "")
            assert unwrap(line(b"1A1692CFf=") + line(b"1A1692CF")) is None
            assert unwrap(line(b"2A1692CFf=") + line(b"2A1692CFf=") + line(b"2A1692CF")) == dict(f = "")

            assert unwrap(line(b"519B6B1Bf=MQ=") + line(b"FF63589D =") + line(b"FF63589D")) == dict(f = "1")
            with expected(Exception("unexpected eof")):
                unwrap(line(b"519B6B1Bf=MQ=") + line(b"FF63589D ="))

            assert unwrap(line(b"293D787Afghij") + line(b"073B3B03 =") + line(b"827241A3abcde") +
                          line(b"2E101F97 _=") + line(b"2E101F97")) == dict(abcde_ = "", fghij = "")

            assert unwrap(line(b"EE327B94rus=") + line(b"1E7EDDF8 0JD") + line(b"ADEED2B8 Qkd") + line(b"7AB4FB9C CS0") +
                          line(b"24D275B1 JPQ") + line(b"396DF0CF lNC") + line(b"2DBDFD85 V0I") + line(b"6B74C736 HQl") +
                          line(b"BCF95634 tCX") + line(b"123D22DC 0Jj") + line(b"B89E2957 Qmd") + line(b"E2ABFFAF Ca0") +
                          line(b"31151CEE JvQ") + line(b"07E76E98 nNC") + line(b"3201524A d0J") + line(b"E3A5813B 7Qn") +
                          line(b"8FE0B379 9Cg") + line(b"3CC108DC 0KH") + line(b"C61A3BF9 Qot") + line(b"5B1B3E38 Cj0") +
                          line(b"71E7B003 KTQ") + line(b"DA15D384 pdC") + line(b"4376F161 m0K") + line(b"E8A2ADD4 fQq") +
                          line(b"3A55E9CE NCp") + line(b"372199E7 0Kz") + line(b"0861BE08 Qq9") + line(b"9B1E1281 Cq0") +
                          line(b"EC65C502 K3Q") + line(b"FEDECD55 rtC") + line(b"513C9281 v0L") + line(b"58A4BCF0 DQs") +
                          line(b"D9DC7778 dCy") + line(b"BF0DD28A 0LP") + line(b"D4A68391 QtN") + line(b"8C425A1F C10") +
                          line(b"8B817108 ZHQ") + line(b"BE7DC55C ttC") + line(b"A7806D22 30L") + line(b"7EFC46B4 jQu") +
                          line(b"33F540B4 dC6") + line(b"74266151 0Lv") + line(b"AA1B7993 QvN") + line(b"B0176254 C90") +
                          line(b"B5437571 L7Q") + line(b"D6D00BCE v9G") + line(b"49908C5C A0Y") + line(b"B31146A7 HRg") +
                          line(b"B91DFD0D tGD") + line(b"A0C5B928 0YT") + line(b"C6E70CBF Rhd") + line(b"4271A70F GG0") +
                          line(b"7E7BAA5C YfR") + line(b"6E70A29F iNG") + line(b"8B60EAC2 J0Y") + line(b"0355397A zRi") +
                          line(b"850789AB 9GK") + line(b"D7A94DDA 0Y3") + line(b"70EDBB4D Rjt") + line(b"B297E091 GP") +
                          line(b"B297E091")) == dict(rus = russian)

            with expected(Exception("unexpected eof")):
                unwrap(line(b"D34233ACfoo=AA") + line(b"936E71EB AA"))

            with expected(Exception("invalid folding")):
                unwrap(line(b"F244CC11 foo"))

            with expected(ValueError):
                unwrap(line(b"29D6A3E8_") + line(b"29D6A3E8"))

            with expected(AssertionError):
                unwrap(line(b"0BAD6683/=") + line(b"0BAD6683"))

            assert unwrap(line(b"FF1683DFXPmncError=amF2YS5sYW5nLklsbGVnYWxBcmd1bWVudEV4Y2VwdGlvbjogY29ubmVjdG") +
                          line(b"C4F21C67 lvbiBmYWN0b3J5IG5hbWUgaXMgbm90IHNwZWNpZmllZA==") +
                          line(b"C4F21C67")) == \
                dict(XPmncError = "java.lang.IllegalArgumentException: connection factory name is not specified")

    test_packet()

    ###################################

    test_interface_config = dict \
    (
    protocol = "jms",
    java = "c:\\progra~1\\java\\jdk16~1.0_1\\bin\\java.exe",
    arguments = ("-Dfile.encoding=windows-1251", ),
    classpath = "c:\\pythomnic3k\\lib;"
                "c:\\pythomnic3k\\lib\\jms.jar;"
                "c:\\pythomnic3k\\lib\\imq.jar;"
                "c:\\pythomnic3k\\lib\\fscontext.jar",
    jndi = { "java.naming.factory.initial": "com.sun.jndi.fscontext.RefFSContextFactory",
             "java.naming.provider.url": "file:///c:/pythomnic3k/lib/jndi" },
    factory = "connection_factory",
    queue = "test.queue",
    username = "",
    password = "",
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    def drain_queue():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request)

        with active_interface("jms_1", **interface_config(process_request = process_request)):
            while loopback_queue.pop(10.0) is not None:
                pass

    drain_queue()

    ###################################

    def test_process_one():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request)

        with active_interface("jms_1", **interface_config(process_request = process_request)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.jms_1.send(russian, JMSCorrelationID = russian, FOOBAR = "123")
            message_id = xa.execute()[0]

            request = loopback_queue.pop(10.0)

        assert request["message_id"] == message_id
        assert request["message_text"] == russian
        headers = request["headers"]
        assert headers["JMSCorrelationID"] == russian and headers["FOOBAR"] == "123"

    test_process_one()

    ###################################

    def test_process_failure():

        loopback_queue = InterlockedQueue()
        fail = Event(); fail.set()

        def process_request(request, response):
            if fail.is_set():
                sleep(1.0)
                raise Exception("processing failure")
            else:
                loopback_queue.push(request)

        with active_interface("jms_1", **interface_config(process_request = process_request)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.jms_1.send("<xml/>")
            message_id = xa.execute()[0]

            assert loopback_queue.pop(3.0) is None

            fail.clear()

            request = loopback_queue.pop(10.0)

        assert request["message_id"] == message_id
        assert request["message_text"] == "<xml/>"
        headers = request["headers"]
        assert not headers.get("JMSCorrelationID")

    test_process_failure()

    ###################################

    def test_process_timeout():

        loopback_queue = InterlockedQueue()
        delay = Event(); delay.set()

        def process_request(request, response):
            if delay.is_set():
                sleep(pmnc.request.remain + 1.0)
            loopback_queue.push(request)

        with active_interface("jms_1", **interface_config(process_request = process_request,
                              request_timeout = 3.0)):

            fake_request(10.0)

            xa = pmnc.transaction.create()
            xa.jms_1.send("<xml/>")
            message_id = xa.execute()[0]

            assert loopback_queue.pop(3.0) is None

            delay.clear()

            request = loopback_queue.pop(10.0)

        assert request["message_id"] == message_id
        assert request["message_text"] == "<xml/>"

    test_process_timeout()

    ###################################

    def test_send_many():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(int(request["message_text"]))

        with active_interface("jms_1", **interface_config(process_request = process_request)):

            for i in range(200):

                fake_request(30.0)

                xa = pmnc.transaction.create()
                xa.jms_1.send(str(i*5))
                xa.jms_1.send(str(i*5+1))
                xa.jms_1.send(str(i*5+2))
                xa.jms_1.send(str(i*5+3))
                xa.jms_1.send(str(i*5+4))
                xa.execute()

            received = []
            message_number = loopback_queue.pop(10.0)
            while message_number is not None:
                received.append(message_number)
                message_number = loopback_queue.pop(10.0)

        assert len(received) == 1000
        received_sorted = []
        for i in range(200): # every 5 should have been sent atomically
            received_sorted.extend(list(sorted(received[i*5:(i+1)*5])))
        assert received_sorted == list(range(1000))

    pmnc._loader.set_log_level("LOG")
    try:
        test_send_many()
    finally:
        pmnc._loader.set_log_level("DEBUG")

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
