#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module contains an implementation of a generic UDP fire-and-forget interface,
# single packet corresponds to a single request. The module also contains a generic
# resource for sending packets over UDP.
#
# Sample UDP interface configuration (config_interface_udp_1.py):
#
# config = dict \
# (
# protocol = "udp",                        # meta
# request_timeout = 10.0,                  # meta, optional
# listener_address = ("127.0.0.1", 8000),  # udp
# )
#
# Sample processing module (interface_udp_1.py):
#
# def process_request(request, response):
#    packet = request["packet"]
#
# Sample UDP resource configuration (config_resource_udp_1.py)
#
# config = dict \
# (
# protocol = "udp",                        # meta
# server_address = ("1.2.3.4", 5678),      # udp, target server address
# broadcast = False,                       # udp, whether SO_BROADCAST
# )
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.udp_1.send(b"FOOBAR")
# xa.execute() # returns nothing
#
# or if the only transaction participant:
#
# pmnc.transaction.udp_1.send(b"FOOBAR")
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource" ]

###############################################################################

import threading; from threading import current_thread
import select; from select import select
import socket; from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, \
                                  SO_BROADCAST, error as socket_error, timeout as socket_timeout
try:
    from socket import SO_REUSEPORT
except ImportError:
    have_reuse_port = False
else:
    have_reuse_port = True

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, optional
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource
import pmnc.threads; from pmnc.threads import HeavyThread
import pmnc.request; from pmnc.request import Request

###############################################################################

class Interface:

    @typecheck
    def __init__(self, name: str, *,
                 listener_address: (str, int),
                 request_timeout: optional(float) = None,
                 **kwargs): # this kwargs allows for extra application-specific
                            # settings in config_interface_udp_X.py

        self._name = name
        self._listener_address = listener_address

        self._request_timeout = request_timeout or \
            pmnc.config_interfaces.get("request_timeout") # this is now static

        if pmnc.request.self_test == __name__: # self-test
            self._process_request = kwargs["process_request"]

    name = property(lambda self: self._name)
    listener_address = property(lambda self: self._listener_address)

    ###################################

    def start(self):
        self._listener = HeavyThread(target = self._listener_proc,
                                     name = "{0:s}:lsn".format(self._name))
        self._listener.start()

    ###################################

    def cease(self):
        self._listener.stop()

    ###################################

    def stop(self):
        pass

    ###################################

    def _create_server_socket(self) -> socket:

        s = socket(AF_INET, SOCK_DGRAM)
        try:
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            if have_reuse_port:
                s.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1)
            s.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
            s.bind(self._listener_address)
        except:
            s.close()
            raise
        else:
            return s

    ###################################

    def _listener_proc(self):

        while not current_thread().stopped():
            try:

                s = self._create_server_socket()
                try:

                    pmnc.log.message("started listening for packets at {0[0]:s}:{0[1]:d}".\
                                     format(self._listener_address))

                    while not current_thread().stopped():

                        if not select([s], [], [], 1.0)[0]:
                            continue

                        try:
                            packet_b, (client_addr, client_port) = s.recvfrom(57344)
                        except socket_timeout:
                            continue
                        except socket_error as e:
                            if e.args[0] == 10054: # workaround for this issue: http://support.microsoft.com/kb/263823
                                continue
                            else:
                                raise

                        if pmnc.log.debug:
                            pmnc.log.debug("incoming UDP packet from {0:s}:{1:d}, {2:d} byte(s)".\
                                           format(client_addr, client_port, len(packet_b)))

                        # the listener thread initiates the received packet processing

                        try:
                            self._enqueue_packet(packet_b, client_addr, client_port)
                        except:
                            pmnc.log.error(exc_string()) # log and ignore

                finally:
                    s.close()

                pmnc.log.message("stopped listening")

            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################

    def _enqueue_packet(self, packet_b, client_addr, client_port):

        # create a new request for processing the message

        request = pmnc.interfaces.begin_request(
                    timeout = self._request_timeout,
                    interface = self._name, protocol = "udp",
                    parameters = dict(auth_tokens = dict(peer_ip = client_addr)),
                    description = "UDP packet from {0:s}, {1:d} byte(s)".\
                                  format(client_addr, len(packet_b)))

        # enqueue the request but do not wait for its completion

        pmnc.interfaces.enqueue(request, self.wu_process_request, (packet_b, ))

    ###################################

    def wu_process_request(self, packet_b):

        try:

            # see for how long the request was on the execution queue up to this moment
            # and whether it has expired in the meantime, if it did there is no reason
            # to proceed and we simply bail out

            if pmnc.request.expired:
                pmnc.log.error("request has expired and will not be processed")
                success = False
                return

            with pmnc.performance.request_processing():
                request = dict(packet = packet_b)
                self._process_request(request, {})

        except:
            pmnc.log.error(exc_string())
            success = False
        else:
            success = True
        finally:                                 # the request ends itself
            pmnc.interfaces.end_request(success) # possibly way after deadline

    ###################################

    def _process_request(self, request, response):
        handler_module_name = "interface_{0:s}".format(self._name)
        pmnc.__getattr__(handler_module_name).process_request(request, response)

###############################################################################

class Resource(TransactionalResource):

    @typecheck
    def __init__(self, name: str, *,
                 server_address: (str, int),
                 broadcast: bool):

        TransactionalResource.__init__(self, name)

        self._server_address = server_address
        self._broadcast = broadcast

        self._server_info = "{0:s} udp://{1[0]:s}:{1[1]:d}".\
                            format(name, self._server_address)

    server_info = property(lambda self: self._server_info)

    ###################################

    def connect(self):

        TransactionalResource.connect(self)

        self._socket = socket(AF_INET, SOCK_DGRAM)
        try:
            if self._broadcast:
                self._socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
            else:
                self._socket.connect(self._server_address)
        except:
            self._socket.close()
            raise

    ###################################

    def disconnect(self):
        try:
            self._socket.close()
        finally:
            TransactionalResource.disconnect()

    ###################################

    @typecheck
    def send(self, packet_b: bytes):

        packet_length = len(packet_b)

        pmnc.log.info("sending UDP packet to {0[0]:s}:{0[1]:d}, {1:d} byte(s)".\
                      format(self._server_address, packet_length))
        try:

            if not select([], [self._socket], [], pmnc.request.remain)[1]:
                raise Exception("request deadline writing data to {0:s}".format(self._server_info))

            if self._broadcast:
                sent = self._socket.sendto(packet_b, self._server_address)
            else:
                sent = self._socket.send(packet_b)

            if sent != packet_length:
                raise Exception("packet truncated, sent {0:d} byte(s)")

        except:
            pmnc.log.warning("sending UDP packet to {0[0]:s}:{0[1]:d}, {1:d} byte(s) failed: {2:s}".\
                             format(self._server_address, packet_length, exc_string()))
            raise
        else:
            pmnc.log.info("UDP packet to {0[0]:s}:{0[1]:d}, {1:d} byte(s) has been sent".\
                          format(self._server_address, packet_length))

###############################################################################

def self_test():

    from time import sleep
    from interlocked_queue import InterlockedQueue
    from pmnc.request import fake_request
    from pmnc.self_test import active_interface

    test_interface_config = dict \
    (
    protocol = "udp",
    listener_address = ("0.0.0.0", 5371),
    request_timeout = 3.0,
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    def test_success():

        fake_request(10.0)

        q = InterlockedQueue()

        def process_request(request, response):
            q.push(request["packet"])

        with active_interface("udp", **interface_config(process_request = process_request)) as ifc:
            msg = b"foo"
            pmnc.transaction.udp_1.send(msg)
            assert q.pop(3.0) == msg

    test_success()

    ###################################

    def test_failure():

        fake_request(10.0)

        q = InterlockedQueue()

        def process_request(request, response):
            1 / 0

        with active_interface("udp", **interface_config(process_request = process_request)) as ifc:
            msg = b"foo"
            pmnc.transaction.udp_1.send(msg)
            assert q.pop(3.0) is None

    test_failure()

    ###################################

    def test_timeout():

        fake_request(10.0)

        q = InterlockedQueue()

        def process_request(request, response):
            sleep(5.0)

        with active_interface("udp", **interface_config(process_request = process_request)) as ifc:
            msg = b"foo"
            pmnc.transaction.udp_1.send(msg)
            assert q.pop(3.0) is None

    test_timeout()

    ###################################

    def test_large():

        fake_request(10.0)

        q = InterlockedQueue()

        def process_request(request, response):
            q.push(request["packet"])

        with active_interface("udp", **interface_config(process_request = process_request)) as ifc:
            msg = b"x" * 57344
            pmnc.transaction.udp_1.send(msg)
            assert q.pop(3.0) == msg

    test_large()

    ###################################

    def test_too_large():

        fake_request(10.0)

        q = InterlockedQueue()

        def process_request(request, response):
            q.push(request["packet"])

        with active_interface("udp", **interface_config(process_request = process_request)) as ifc:
            pmnc.transaction.udp_1.send(b"x" * 60000)
            assert q.pop(3.0) is None

    test_too_large()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
