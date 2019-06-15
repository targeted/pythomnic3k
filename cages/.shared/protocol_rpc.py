#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This module contains implementation of an RPC interface and resource, built
# on top of TcpInterface/TcpResource. Pythomnic3k RPC is based on the following
# principles:
#
# 1. Automatic discovery of a target cage. Whenever some cage executes
#    pmnc("othercage").module.method(...)
#    the location of othercage is discovered automatically and transparently
#    by sending out UDP broadcasts to which (either copy of) othercage presumably
#    responds. The received response contains the listening address of the
#    responded cage and TCP/SSL connection can be initiated.
#    Target cage addresses can be configured separately, if necessary,
#    for example if UDP broadcasts can't go freely between the cages.
#    Therefore a network of cages can well exist without broadcasts,
#    but that would require a lot of manual configuration.
#
# 2. Multiple cages with the same name can be running on different servers,
#    they all respond to the discovery requests. One of the responses to arrive
#    first is chosen, and responses from different cages may cause connections
#    to different copies of the target cage to exist at the same time.
#
# 3. Hence this - in Pythomnic3k there is no difference between copies of
#    identically named cages - they are supposed to be freely interchangeable.
#    This fact is stressed by using SSL certificates to authenticate cages
#    to each other - each "cage" presumably owns a certificate which has common
#    name of "cage". This allows cages to trust each other at least by name.
#
# 4. RPC implemented here is synchronous and plays by Pythomnic3k rules of non-
#    blocking and timing out within deadline. Execution of the call on the target
#    cage inherits the original timeout, therefore the original request is logically
#    "extended" to the target cage and impersonated for the course of RPC execution.
#
# 5. Each cage advertises its location by periodically sending out broadast
#    requests. The received locations of other cages are kept on record and
#    can be accessed by get_cages and get_nodes methods of the rpc interface,
#    this is used by the health_monitor cage to locate active cages. Typically
#    a cage broadcasts its own presence but it is technically possible for one
#    cage (a sort of registry perhaps) to broadcast the locations of other cages.
#
# There is just one RPC interface and RPC resource required for a cage to be
# able to exchange RPC calls with other cages.
#
# Call arguments are marshaled into binary packets using one of the configured
# methods. Currently supported are pickle (historically being the only) and
# msgpack (using its umsgpack.py implementation). Msgpack is the preferred
# method from security standpoint. Both interface and resource have marshaling_methods
# in their configuration files. Interface will accept incoming calls marshaled
# using any of the configured methods, and resource will always marshal outgoing
# requests using the first supported method.
#
# Sample RPC interface configuration (config_interface_rpc.py):
#
# config = dict \
# (
# protocol = "rpc",                                        # meta
# request_timeout = None,                                  # meta, optional
# random_port = -63000,                                    # tcp, negative means "in range 63000..63999"
# max_connections = 100,                                   # tcp
# broadcast_address = ("1.2.3.4/1.2.3.255", 12480),        # rpc, "interface address/broadcast address", port
# flock_id = "DEFAULT",                                    # rpc, arbitrary cage group identifier
# marshaling_methods = ("msgpack", "pickle"),              # rpc, allowed marshaling methods
# max_packet_size = 1048576,                               # rpc, maximum allowed request/response size in bytes
# )
#
# Sample RPC resource configuration (config_resource_rpc.py)
#
# config = dict \
# (
# protocol = "rpc",                                        # meta
# broadcast_address = ("1.2.3.4/1.2.3.255", 12480),        # rpc, "interface address/broadcast address", port
# discovery_timeout = 3.0,                                 # rpc + tcp (discovery + connect timeout)
# multiple_timeout_allowance = 0.5,                        # rpc, in range 0.0..1.0
# flock_id = "DEFAULT",                                    # rpc, arbitrary cage group identifier
# marshaling_methods = ("msgpack", "pickle"),              # rpc, marshaling methods, priority ordered
# max_packet_size = 1048576,                               # rpc, maximum allowed request/response size in bytes
# exact_locations = { "SomeCage": "ssl://1.2.3.5:63842" }, # rpc, maps cage names to their fixed locations
# )
#
# Pythomnic3k project
# (c) 2005-2019, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource", "Handler" ]

###############################################################################

import os; from os import urandom, SEEK_SET, SEEK_CUR, SEEK_END, path as os_path
import time; from time import time
import binascii; from binascii import a2b_hex, b2a_hex
import select; from select import select
import io; from io import BytesIO
import hashlib; from hashlib import sha1
import threading; from threading import current_thread, Lock
import pickle; from pickle import load as unpickle, dumps as pickle
import struct; from struct import pack, unpack
import random; from random import randint
import ssl; from ssl import CERT_REQUIRED
import socket; from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, \
                                  SO_BROADCAST, SO_REUSEADDR, error as socket_error
try:
    from socket import SO_REUSEPORT
except ImportError:
    have_reuse_port = False
else:
    have_reuse_port = True
try:
    import umsgpack; from umsgpack import load as unmsgpack, dumps as msgpack
except ImportError:
    have_msgpack = False
else:
    have_msgpack = True

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, optional, by_regex, dict_of, tuple_of, one_of, anything
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, ResourceError, RPCError
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.threads; from pmnc.threads import HeavyThread

###############################################################################

valid_cage_name = by_regex("^[A-Za-z0-9_-]{1,32}$")
valid_node_name = by_regex("^[A-Za-z0-9_-]{1,32}$")
valid_flock_id = by_regex("^[A-Za-z0-9_-]+$")
valid_location = by_regex("^(ssl|tcp)://([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|\\*):[0-9]{1,5}/$")
valid_exact_location = by_regex("^(ssl|tcp)://[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}:[0-9]{1,5}/$")
valid_marshaling_method = one_of("pickle", "msgpack")
valid_marshaling_methods = lambda t: tuple_of(valid_marshaling_method)(t) and len(t) > 0

###############################################################################

@typecheck
def _filter_marshaling_methods(marshaling_methods: optional(valid_marshaling_methods)) -> valid_marshaling_methods:
    marshaling_methods = marshaling_methods or ("msgpack", "pickle")
    if "msgpack" in marshaling_methods and not have_msgpack:
        pmnc.log.warning("marshaling method msgpack requires a missing library u-msgpack-python and is disabled")
        marshaling_methods = tuple(mm for mm in marshaling_methods if mm != "msgpack")
        if not marshaling_methods:
            raise Exception("none of the configured marshaling methods can be used")
    return marshaling_methods

###############################################################################

def _get_peer_name(ssl_socket):
    for f in ssl_socket.getpeercert()["subject"]:
        if f[0][0] == "commonName":
            return f[0][1]
    else:
        return None

###############################################################################

def _create_sending_broadcast_socket(addr: str) -> socket:
    bc_socket = socket(AF_INET, SOCK_DGRAM)
    try:
        bc_socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        bc_socket.bind((addr, 0))
    except:
        bc_socket.close()
        raise
    else:
        return bc_socket

###############################################################################

def _create_receiving_broadcast_socket(port: int) -> socket:
    bc_socket = socket(AF_INET, SOCK_DGRAM)
    try:
        bc_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        if have_reuse_port:
            bc_socket.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1)
        bc_socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        bc_socket.bind(("0.0.0.0", port))
    except:
        bc_socket.close()
        raise
    else:
        return bc_socket

###############################################################################

# key files required to establish SSL connection
# may reside in current cage or in .shared

def _locate_key_file(filename: str) -> optional(os_path.isfile):

    s = os_path.join(__cage_dir__, "ssl_keys", filename)
    if os_path.isfile(s):
        return s

    s = os_path.normpath(os_path.join(__cage_dir__, "..", ".shared", "ssl_keys", filename))
    if os_path.isfile(s):
        return s

    return None

###############################################################################

class RpcMarshaler:

    def __init__(self, marshaling_methods, max_packet_size):
        self._marshaling_methods = marshaling_methods
        self._max_packet_size = max_packet_size

    @typecheck
    def __call__(self, value: anything, method = None) -> (valid_marshaling_method, bytes):
        method = method or self._marshaling_methods[0]
        data_b = getattr(self, "_marshal_{0:s}".format(method))(value)
        if len(data_b) > self._max_packet_size:
            raise Exception("packet size exceeded")
        return method, data_b

    # pickle packet is historically marshaled using printable hex
    # length | hash | pickle data
    # which is a stupid thing to do

    def _marshal_pickle(self, value):
        content_b = b"PKL3" + pickle(value)
        return "{0:08X}".format(len(content_b)).encode("ascii") + \
               sha1(content_b).hexdigest().upper().encode("ascii") + \
               content_b

    # more logical is to use binary representation of the same
    # length | hash | msgpack data

    def _marshal_msgpack(self, value):
        content_b = b"MSGP" + msgpack(value)
        return pack(">L", len(content_b)) + \
               sha1(content_b).digest() + \
               content_b

###############################################################################

class RpcUnmarshaler:

    def __init__(self, marshaling_methods, max_packet_size):

        self._marshaling_methods = marshaling_methods
        self._max_packet_size = max_packet_size

        self._stream = BytesIO()
        self._length_size = None
        self._length = None
        self._hash_size = None
        self._hash_b = None
        self._hash = sha1()

    @typecheck
    def __call__(self, data_b: bytes) -> optional((valid_marshaling_method, anything)):

        self._stream.write(data_b)
        stream_length = self._stream.tell()

        if stream_length == 0:
            return None
        elif stream_length > self._max_packet_size:
            raise Exception("packet size exceeded")
        elif self._length is not None and stream_length > self._length:
            raise Exception("request length exceeded")

        if self._length_size is None:
            self._stream.seek(0, SEEK_SET) # by reading the first byte
            b = self._stream.read(1)       # determine whether the packet
            if ord(b) & 0xf0 == 0:         # has hex or binary header
                self._length_size = 4
                self._hash_size = 20
            else:
                self._length_size = 8
                self._hash_size = 40
            self._stream.seek(0, SEEK_END)

        if stream_length < self._length_size:
            return None
        elif self._length is None:
            self._stream.seek(0, SEEK_SET)
            length_b = self._stream.read(self._length_size)
            if self._length_size == 4:
                self._length = unpack(">L", length_b)[0]
            else:
                self._length = int(length_b, 16)
            self._length += self._length_size + self._hash_size
            if self._length > self._max_packet_size:
                raise Exception("packet size exceeded")
            self._stream.seek(0, SEEK_END)

        if stream_length < self._length_size + self._hash_size:
            return None
        elif self._hash_b is None:
            self._stream.seek(self._length_size, SEEK_SET)
            if self._hash_size == 20:
                self._hash_b = self._stream.read(self._hash_size)
            else:
                self._hash_b = a2b_hex(self._stream.read(self._hash_size))
            self._hash.update(self._stream.read())
        else:
            self._hash.update(data_b)

        if stream_length == self._length:
            if self._hash.digest() != self._hash_b:
                raise Exception("request hash mismatch")
            self._stream.seek(self._length_size + self._hash_size, SEEK_SET)
            sig_b = self._stream.read(4)
            if sig_b == b"MSGP" and "msgpack" in self._marshaling_methods:
                return "msgpack", unmsgpack(self._stream)
            elif sig_b == b"PKL3" and "pickle" in self._marshaling_methods:
                return "pickle", unpickle(self._stream)
            else:
                raise Exception("unsupported marshaling method")

###############################################################################

class Interface: # RPC interface

    @typecheck
    def __init__(self, name: str, *,
                 random_port: lambda i: isinstance(i, int) and (-65500 <= i <= 65535),
                 max_connections: int,
                 broadcast_address: (str, int),
                 flock_id: valid_flock_id,
                 marshaling_methods: optional(valid_marshaling_methods) = None,
                 max_packet_size: optional(int) = None,
                 separate_thread_pool: optional(bool) = False,
                 disable_broadcast: optional(bool) = False,
                 request_timeout: optional(float) = None,
                 **kwargs):

        self._name = name
        broadcast_address, self._broadcast_port = broadcast_address
        self._bind_address, self._broadcast_address = broadcast_address.split("/")
        self._disable_broadcast = disable_broadcast
        self._request_prefix = "PYTHOMNIC3K-REQUEST:{0:s}:".format(flock_id)
        self._response_prefix = "PYTHOMNIC3K-RESPONSE:{0:s}:".format(flock_id)

        self._known_cages = {} # { cage: { node: { location: ..., timeout: ... } } }
        self._known_cages_lock = Lock()
        self._ad_period_idx = 0

        ssl_key_cert_file = _locate_key_file("key_cert.pem")
        ssl_ca_cert_file = _locate_key_file("ca_cert.pem")
        ssl_ciphers = "HIGH:!aNULL:!MD5"
        ssl_protocol = "TLSv1"

        if pmnc.request.self_test == __name__: # self-test
            self.process_request = kwargs["process_request"]
            self._cage_name = kwargs["cage_name"]
            if "ad_periods" in kwargs:
                self._ad_periods = kwargs["ad_periods"]
        else:
            self._cage_name = __cage__

        # having handler factory create handlers through a pmnc call
        # allows online modifications to this module, after it is reloaded

        marshaling_methods = _filter_marshaling_methods(marshaling_methods)
        if pmnc.log.debug:
            pmnc.log.debug("interface {0:s} supports the following marshaling methods: {1:s}".\
                           format(self._name, ", ".join(marshaling_methods)))

        max_packet_size = max_packet_size if max_packet_size is not None else 1048576

        handler_factory = lambda prev_handler: \
            pmnc.protocol_rpc.Handler(self.process_request, self._cage_name,
                                      marshaling_methods, max_packet_size)

        # create an instance of underlying TCP interface, note that in case
        # of SSL we require the client (source cage) to present a certificate

        request_timeout = request_timeout or \
                          pmnc.config_interfaces.get("request_timeout") # this is now static

        self._tcp_interface = \
            pmnc.protocol_tcp.TcpInterface(name, handler_factory, request_timeout,
                                           listener_address = (self._bind_address, random_port),
                                           max_connections = max_connections,
                                           ssl_key_cert_file = ssl_key_cert_file,
                                           ssl_ca_cert_file = ssl_ca_cert_file,
                                           ssl_ciphers = ssl_ciphers,
                                           ssl_protocol = ssl_protocol,
                                           required_auth_level = CERT_REQUIRED)

        # RPC interface is special in that it can be configured to enqueue
        # its requests to a separate private thread pool instead of using
        # the thread pool shared with all other interfaces

        if separate_thread_pool:
            self._thread_pool = pmnc.shared_pools.get_private_thread_pool()
            self._tcp_interface._enqueue_request = self._enqueue_request

    name = property(lambda self: self._tcp_interface.name)
    listener_address = property(lambda self: self._tcp_interface.listener_address)

    ###################################

    def start(self):
        self._tcp_interface.start()
        if not self._disable_broadcast: # create and start broadcaster thread
            self._broadcaster = HeavyThread(target = self._broadcaster_proc,
                                            name = "{0:s}:brc".format(self._name))
            self._broadcaster.start()

    def cease(self):
        if not self._disable_broadcast:
            self._broadcaster.stop()
        self._tcp_interface.cease()

    def stop(self):
        self._tcp_interface.stop()

    ###################################

    def _broadcaster_proc(self):
        try:

            # the listening socket must be bound to the configured port

            self._bc_socket = _create_receiving_broadcast_socket(self._broadcast_port)
            try:

                ad_timeout = self._next_advertisement_timeout()

                while not current_thread().stopped(): # lifetime loop
                    try:

                        # receive incoming broadcast packets

                        receive_timeout = Timeout(1.0)
                        while not receive_timeout.expired and select([self._bc_socket], [], [], receive_timeout.remain)[0]:
                            try:
                                try:
                                    packet, (client_addr, client_port) = self._bc_socket.recvfrom(57344)
                                except socket_error as e:
                                    if e.args[0] == 10054: # workaround for http://support.microsoft.com/kb/263823
                                        continue
                                    else:
                                        raise
                                try:
                                    packet = packet.decode("ascii")
                                except UnicodeDecodeError:
                                    continue
                                self._handle_received_packet(packet, client_addr, client_port)
                            except:
                                pmnc.log.error(exc_string()) # log and ignore

                        # send self-advertisement once in a while

                        if ad_timeout.expired:
                            try:
                                self._purge_known_cages()
                                self._broadcast_advertisement()
                            finally:
                                ad_timeout = self._next_advertisement_timeout()

                    except:
                        pmnc.log.error(exc_string()) # log and ignore

            finally:
                self._bc_socket.close()

        except:
            pmnc.log.error(exc_string()) # log and ignore

    ###################################

    # this method overrides identical method from TcpInterface
    # if the interface is configured to use private thread pool

    def _enqueue_request(self, *args, **kwargs):
        self._thread_pool.enqueue(*args, **kwargs)

    ###################################

    def get_cages(self):
        with self._known_cages_lock:
            return set(self._known_cages.keys())

    ###################################

    @typecheck
    def get_nodes(self, cage: valid_cage_name):
        with self._known_cages_lock:
            nodes = self._known_cages.get(cage, {})
            return { node: cage_info["location"]
                     for node, cage_info in nodes.items() }

    ###################################

    def _discovery_response(self, request_id: str) -> bytes:
        return "{0:s}{1:s}:{2:s}://*:{3:d}/".\
               format(self._response_prefix, request_id,
                      self._tcp_interface.encrypted and "ssl" or "tcp",
                      self._tcp_interface.listener_address[1]).\
               encode("ascii")

    ###################################

    # each cage broadcasts its presence, issuing increasing timeouts
    # between the broadcasts, so that all the other cages know soon,
    # but do not receive excessive flood afterwards

    _ad_periods = (10.0, 20.0, 30.0, 60.0, 180.0, 300.0, 600.0) # in seconds, the last one is repeated

    def _next_advertisement_timeout(self) -> Timeout:
        ad_period = self._ad_periods[self._ad_period_idx]
        if self._ad_period_idx < len(self._ad_periods) - 1:
            self._ad_period_idx += 1
        return Timeout(ad_period)

    ###################################

    def _advertisement_packet(self) -> bytes:

        request_id = b2a_hex(urandom(8)).decode("ascii").upper()
        return "{0:s}{1:s}:ADVERTISE-CAGE:{2:s}:{3:s}:{4:d}:{5:s}://*:{6:d}/".\
               format(self._request_prefix, request_id, __node__,
                      self._cage_name, int(self._ad_periods[self._ad_period_idx]),
                      self._tcp_interface.encrypted and "ssl" or "tcp",
                      self._tcp_interface.listener_address[1]).\
               encode("ascii")

    ###################################

    def _broadcast_advertisement(self):

        bc_socket = _create_sending_broadcast_socket(self._bind_address)
        try:
            if pmnc.log.debug:
                pmnc.log.debug("broadcasting cage advertisement: {0:s}.{1:s} is at "
                               "{2:s}://*:{3:d}/".format(__node__, self._cage_name,
                               self._tcp_interface.encrypted and "ssl" or "tcp",
                               self._tcp_interface.listener_address[1]))
            bc_socket.sendto(self._advertisement_packet(),
                             (self._broadcast_address, self._broadcast_port))
        finally:
            bc_socket.close()

    ###################################

    def _handle_received_packet(self, packet: str, client_addr: str, client_port: int):

        if not packet.startswith(self._request_prefix):
            return

        request_id, request, request_params = \
            packet[len(self._request_prefix):].split(":", 2)

        if request == "DISCOVER-CAGE": # received discovery request from another cage

            if request_params == self._cage_name:
                discovery_response = self._discovery_response(request_id)
                if pmnc.log.debug:
                    pmnc.log.debug("sending response to discovery request "
                                   "from {0:s}".format(client_addr))
                self._bc_socket.sendto(discovery_response, (client_addr, client_port))

        elif request == "ADVERTISE-CAGE": # received other cage's presence broadcast

            node, cage, period, advertised_location = request_params.split(":", 3)
            try:
                period = int(period)
            except ValueError:
                period = 0

            if valid_cage_name(cage) and valid_node_name(node) and \
               1 <= period <= 3600 and valid_location(advertised_location):

                # the advertisement should contain the URL to the cage,
                # for example ssl://1.2.3.4:5678/ or ssl://*:5678/ the latter
                # form meaning the address from which it has been sent

                cage_addr, cage_port = advertised_location[6:-1].split(":")
                if cage_addr == "*": cage_addr = client_addr
                location = "{0:s}{1:s}:{2:s}/".format(advertised_location[:6],
                                                      cage_addr, cage_port)

                # record the received advertisement, keeping track of time,
                # so that the record expires and disappears without receiving
                # more advertisements

                with self._known_cages_lock:
                    nodes = self._known_cages.setdefault(cage, {})
                    if nodes.get(node, {}).get("location") != location:
                        if pmnc.log.debug:
                            pmnc.log.debug("received cage advertisement: {0:s}.{1:s} "
                                           "is at {2:s}".format(node, cage, location))
                    nodes[node] = dict(location = location, timeout = Timeout(period * 2.0))

    ###################################

    def _purge_known_cages(self):

        with self._known_cages_lock:
            for cage, nodes in self._known_cages.items():
                expired_nodes = [ node for node, cage_info in nodes.items()
                                  if cage_info["timeout"].expired ]
                for expired_node in expired_nodes:
                    del nodes[expired_node]

    ###################################

    def process_request(self, module, method, args, kwargs):
        return pmnc.__getattr__(module).__getattr__(method)(*args, **kwargs)

###############################################################################

class Handler: # this class is instantiated from interface_tcp

    protocol = "rpc"

    idle_timeout = 86400.0 # server side of an RPC connection will be kept until
                           # the client closes it, either as idle or expired

    ###################################

    def __init__(self, process_request, cage_name, marshaling_methods, max_packet_size):
        self._process_request = process_request
        self._cage_name = cage_name
        self._marshaling_methods = marshaling_methods
        self._max_packet_size = max_packet_size
        self._unmarshaler = RpcUnmarshaler(marshaling_methods, max_packet_size)
        self._local_ts = None

    ###################################

    @typecheck
    def consume(self, data_b: bytes) -> bool:
        if self._local_ts is None:
            self._local_ts = time()
        method_request = self._unmarshaler(data_b)
        if method_request is not None:
            self._method, self._request = method_request # the packet has been received in full and parsed
            return True
        else:
            return False

    ###################################

    # this method is executed by one of the interface pool threads and performs
    # the actual processing of a previously read and parsed request

    def process_tcp_request(self):

        # extract call information from the dict contained
        # in the received request

        request = self._request
        source_node = request.get("source_node") # backwards compatibility 1.5+
        target_cage = request["target_cage"]
        source_cage = request["source_cage"]
        module = request["module"]
        method = request["method"]
        args = request["args"]
        kwargs = request["kwargs"]
        request_dict = request["request"]
        rpc_dict = request.get("rpc") # backwards compatibility 1.5+

        assert target_cage == self._cage_name, "expected call to this cage"

        # if the connection arrived over SSL, verify source
        # cage name against the peer's SSL certificate

        auth_tokens = pmnc.request.parameters["auth_tokens"]

        if auth_tokens["encrypted"] and \
           not by_regex("^{0:s}$".format(auth_tokens["peer_cn"]))(source_cage):
            raise Exception("source cage name does not match its SSL certificate")

        if rpc_dict: # hop-by-hop information is processed

            if __node__ != source_node: # correct request deadline by the hosts time difference
                request_dict["deadline"] += self._local_ts - rpc_dict["ts"]

        # extract the original request parameters, note that its remaining time
        # is decreased if the current RPC request has less time left

        request_dict.setdefault("interface", "rpc") # backwards compatibility section
        request_dict.setdefault("protocol", "rpc")
        request_dict["parameters"].setdefault("auth_tokens", {}).\
                                   setdefault("source_cage", source_cage)

        # request deadline calculation, this cage imposes its own restriction
        # with local request_timeout, the received request has its own deadline,
        # and time on this host and  on source host could be different

        request = pmnc.request.from_dict(request_dict, timeout = pmnc.request.remain)

        # now we know more about the request, it is still the current
        # RPC request, we haven't impersonated the received request yet

        module_method = "{0:s}.{1:s}".format(module, method)

        pmnc.request.describe("RPC call {0:s} from {1:s} at {2:s}".\
                              format(module_method, source_cage, auth_tokens["peer_ip"]))
        try:
            if pmnc.log.debug:
                pmnc.log.debug("impersonating request {0:s}".format(request.description))
            original_request = current_thread()._request
            current_thread()._request = request
            try:
                result = self._process_request(module, method, args, kwargs)
            finally:
                current_thread()._request = original_request
        except:
            error = exc_string()
            pmnc.log.error("incoming RPC call failed: {0:s}".format(error))
            response = dict(exception = error)
            response_description = "RPC error"
        else:
            response = dict(result = result)
            response_description = "RPC response"

        # marshal the response and prepare for delivery, note that
        # the delivery is done on behalf and within the local request

        marshaler = RpcMarshaler(self._marshaling_methods, self._max_packet_size)
        response_method, response_b = marshaler(response, self._method) # note that the response is marshaled
        assert response_method == self._method                          # using the same method as the request

        self._response_stream = BytesIO(response_b)

        if pmnc.log.debug:
            pmnc.log.debug("returning {0:s}, {1:d} {2:s} byte(s)".\
                           format(response_description, len(response_b), self._method))

    ###################################

    @typecheck
    def produce(self, n: int) -> bytes:
        return self._response_stream.read(n)

    ###################################

    @typecheck
    def retract(self, n: int):
        self._response_stream.seek(-n, SEEK_CUR)

###############################################################################

class Resource(TransactionalResource): # RPC resource

    @typecheck
    def __init__(self, name, *,
                 broadcast_address: (str, int),
                 discovery_timeout: float,
                 multiple_timeout_allowance: float,
                 flock_id: valid_flock_id,
                 marshaling_methods: optional(valid_marshaling_methods) = None,
                 max_packet_size: optional(int) = None,
                 exact_locations: dict_of(valid_cage_name, valid_exact_location),
                 pool__resource_name: valid_cage_name):

        TransactionalResource.__init__(self, name)

        self._cage_name = pool__resource_name
        self._exact_location = exact_locations.get(self._cage_name)
        self._connect_timeout = discovery_timeout

        if self._exact_location is None:
            broadcast_address, self._broadcast_port = broadcast_address
            self._bind_address, self._broadcast_address = broadcast_address.split("/")
            self._multiple_timeout_allowance = min(multiple_timeout_allowance, 1.0)
            self._request_prefix = "PYTHOMNIC3K-REQUEST:{0:s}:".format(flock_id)
            self._response_prefix = "PYTHOMNIC3K-RESPONSE:{0:s}:".format(flock_id)

        self._marshaling_methods = _filter_marshaling_methods(marshaling_methods)
        if pmnc.log.debug:
            pmnc.log.debug("resource {0:s} is using marshaling method {1:s}".\
                           format(name, marshaling_methods[0]))

        self._max_packet_size = max_packet_size if max_packet_size is not None else 1048576

    ###################################

    def connect(self):

        TransactionalResource.connect(self)

        # discover and establish connection

        connect_timeout = Timeout(min(self._connect_timeout, pmnc.request.remain))
        if self._exact_location is None:
            self._tcp_resource = self._discover(connect_timeout)
        else:
            self._tcp_resource = self._create_tcp_resource(connect_timeout, self._exact_location)
        self._tcp_resource.connect()

        # if the connection was established over SSL, we verify
        # target cage name against the peer's SSL certificate

        if self._tcp_resource.encrypted and \
           not by_regex("^{0:s}$".format(self._tcp_resource.peer_cn))(self._cage_name):
            raise Exception("target cage name does not match its SSL certificate")

        # reset collected module/method attributes

        self._attrs = []

    ###################################

    def __getattr__(self, name):
        self._attrs.append(name)
        return self

    ###################################

    def __call__(self, *args, **kwargs):

        try:

            attrs, self._attrs = self._attrs, []
            assert len(attrs) == 2, "expected module.method RPC syntax"
            module, method = attrs

            assert not self.resource_args and not self.resource_kwargs, \
                   "synchronous remote calls have no options"

            # wrap up an RPC call and assemble a byte stream

            request_dict = pmnc.request.to_dict()

            # remove request parameters that must not cross the RPC border

            request_dict["parameters"].pop("retry", None)

            # wrap all the call parameters in a plain dict

            request = dict(source_node = __node__,
                           source_cage = __cage__,
                           target_cage = self._cage_name,
                           module = module, method = method,
                           args = args, kwargs = kwargs,
                           request = request_dict)

            # hop-by-hop information is inserted

            request["rpc"] = dict(ts = time()) # time on sending host

            # marshal the call into a packet of bytes

            marshaler = RpcMarshaler(self._marshaling_methods, self._max_packet_size)
            request_method, request_b = marshaler(request)

            self._unmarshaler = RpcUnmarshaler(self._marshaling_methods, self._max_packet_size)

            request_description = \
                "RPC request {0:s}.{1:s} to {2:s}, {3:d} {4:s} byte(s)".\
                format(module, method, self._tcp_resource.server_info, len(request_b), request_method)

            service_request = module == "remote_call" and method == "accept"

        except:
            ResourceError.rethrow(recoverable = True, terminal = False)

        if not service_request: pmnc.log.info("sending {0:s}".format(request_description))
        try:

            response_method, response = self._tcp_resource.send_request(request_b, self._unmarshaler)
            try:
                result = response["result"]
            except KeyError:
                raise RPCError(description = response["exception"], terminal = False)
            else:
                assert response_method == request_method

        except RPCError as e:
            pmnc.log.warning("{0:s} returned error: {1:s}".\
                             format(request_description, e.description))
            raise
        except:
            pmnc.log.warning("{0:s} failed: {1:s}".\
                             format(request_description, exc_string()))
            raise
        else:
            if not service_request: pmnc.log.info("RPC request returned successfully")
            return result

    ###################################

    def disconnect(self):
        try:
            self._tcp_resource.disconnect()
        finally:
            TransactionalResource.disconnect(self)

    ###################################

    def _create_discovery_request(self) -> (bytes, bytes):

        request_id = b2a_hex(urandom(8)).decode("ascii").upper()
        discovery_request = "{0:s}{1:s}:DISCOVER-CAGE:{2:s}".\
                            format(self._request_prefix, request_id, self._cage_name)
        discovery_response_prefix = "{0:s}{1:s}:".format(self._response_prefix, request_id)

        return discovery_request.encode("ascii"), \
               discovery_response_prefix.encode("ascii")

    ###################################

    @typecheck
    def _create_tcp_resource(self, timeout: Timeout, discovered_location: valid_location,
                             remote_addr: optional(str) = None):

        if discovered_location[:6] in ("ssl://", "tcp://") and discovered_location[-1] == "/":
            cage_addr, cage_port = discovered_location[6:-1].split(":")
            if cage_addr == "*":
                assert remote_addr is not None
                cage_addr = remote_addr
        else:
            raise Exception("unsupported RPC protocol")

        if discovered_location.startswith("ssl://"):
            ssl_key_cert_file = _locate_key_file("key_cert.pem")
            ssl_ca_cert_file = _locate_key_file("ca_cert.pem")
            ssl_ciphers = "HIGH:!aNULL:!MD5"
            ssl_protocol = "TLSv1"
        elif discovered_location.startswith("tcp://"):
            ssl_key_cert_file = None
            ssl_ca_cert_file = None
            ssl_ciphers = None
            ssl_protocol = None

        return pmnc.protocol_tcp.TcpResource(self._cage_name,
                                             server_address = (cage_addr, int(cage_port)),
                                             connect_timeout = timeout.remain,
                                             ssl_key_cert_file = ssl_key_cert_file,
                                             ssl_ca_cert_file = ssl_ca_cert_file,
                                             ssl_ciphers = ssl_ciphers,
                                             ssl_protocol = ssl_protocol,
                                             ssl_server_hostname = None,
                                             ssl_ignore_hostname = True)

    ###################################

    def _estimate_node_count(self) -> int:
        rpc_interface = pmnc.interfaces.get_interface("rpc")
        return rpc_interface and len(rpc_interface.get_nodes(self._cage_name)) or 0

    ###################################

    def _discover(self, timeout: Timeout):

        # during discovery we expect to receive as many responses as
        # there are cage instances currently known through advertising

        expected_responses = self._estimate_node_count()

        if pmnc.log.debug:
            pmnc.log.debug("discovering cage {0:s}, advertised from {1:d} node(s)".\
                           format(self._cage_name, expected_responses))
        try:

            # if more than one cage is expected to respond, we allow slightly longer
            # initial waiting for responses, controlled by multiple_timeout_allowance

            if expected_responses > 1:
                multiple_timeout_allowance = Timeout(self._multiple_timeout_allowance)

            discovery_request, discovery_response_prefix = self._create_discovery_request()
            received_responses = {}

            # the one-time broadcasting socket must be bound to a random port

            bc_socket = _create_sending_broadcast_socket(self._bind_address)
            try:

                resend_timeout = Timeout(1.0) # broadcast once a second

                while not timeout.expired and not received_responses: # until target cage is discovered or timeout expires

                    # broadcast discovery request

                    bc_socket.sendto(discovery_request, (self._broadcast_address, self._broadcast_port))
                    resend_timeout.reset()

                    # wait for a matching response (there could be more than one)

                    while select([bc_socket], [], [], min(timeout.remain, resend_timeout.remain))[0]:
                        try:

                            packet, (remote_addr, remote_port) = bc_socket.recvfrom(57344)

                            # note that the received packet does not necessarily
                            # contain the discovery response we wait for

                            if remote_port == self._broadcast_port and \
                               packet.startswith(discovery_response_prefix): # but this one is ours

                                # parse the response and append channel instance to the list of discovered

                                discovered_location = packet[len(discovery_response_prefix):].decode("ascii")
                                if pmnc.log.debug:
                                    pmnc.log.debug("received discovery response from {0:s}, cage {1:s} is at {2:s}".\
                                                   format(remote_addr, self._cage_name, discovered_location))

                                received_responses[remote_addr] = discovered_location
                                if len(received_responses) >= expected_responses:
                                    break # while select

                            # if some cage instance(s) have already responded, but there are still
                            # more responses to expect, keep waiting within allowance time

                            if received_responses and \
                               (multiple_timeout_allowance.expired or
                                not select([bc_socket], [], [],
                                           min(timeout.remain, resend_timeout.remain,
                                               multiple_timeout_allowance.remain))[0]):
                                break # while select

                        except:
                            pmnc.log.error(exc_string()) # log and ignore

            finally:
                bc_socket.close()

            # if more than one cage instance has been discovered,
            # pick one at random to improve load balancing

            if received_responses:
                remote_addrs = list(received_responses.keys())
                remote_addr = remote_addrs[randint(0, len(remote_addrs) - 1)]
                discovered_location = received_responses[remote_addr]
                tcp_resource = self._create_tcp_resource(timeout, discovered_location, remote_addr)
            else:
                raise Exception("no discovery response from cage {0:s} in {1:.01f} second(s)".\
                                format(self._cage_name, timeout.timeout))

        except:
            pmnc.log.warning("discovery attempt failed: {0:s}".format(exc_string()))
            raise
        else:
            if pmnc.log.debug:
                pmnc.log.debug("discovered {0:s}".format(tcp_resource.server_info))
            return tcp_resource

###############################################################################

def self_test():

    if not have_msgpack:
        raise Exception("umsgpack.py is required to test this module")

    from time import sleep
    from expected import expected
    from pmnc.request import fake_request
    from pmnc.self_test import active_interface
    from pmnc.resource_pool import TransactionExecutionError
    from umsgpack import InsufficientDataException
    from pmnc.timeout import Timeout

    ###################################

    def test_marshaling_methods():

        global have_msgpack

        assert _filter_marshaling_methods(None) == ("msgpack", "pickle")
        assert _filter_marshaling_methods(("pickle", )) == ("pickle", )
        assert _filter_marshaling_methods(("msgpack", )) == ("msgpack", )
        assert _filter_marshaling_methods(("pickle", "msgpack")) == ("pickle", "msgpack")

        have_msgpack = False
        assert _filter_marshaling_methods(None) == ("pickle", )
        assert _filter_marshaling_methods(("pickle", )) == ("pickle", )
        with expected(Exception("none of the configured marshaling methods can be used")):
            _filter_marshaling_methods(("msgpack", ))
        assert _filter_marshaling_methods(("pickle", "msgpack")) == ("pickle", )

        have_msgpack = True

    test_marshaling_methods()

    ###################################

    def test_marshaling():

        with expected(Exception("packet size exceeded")):
            RpcMarshaler(("pickle", "msgpack"), 1)("foo")

        m = RpcMarshaler(("pickle", "msgpack"), 1024)
        t, p = m("foo")

        assert t == "pickle"
        assert p == b"00000011788B085646895FE0185AAD076A9FD05F9D436EE2PKL3\x80\x03X\x03\x00\x00\x00fooq\x00."
        assert len(b"PKL3\x80\x03X\x03\x00\x00\x00fooq\x00.") == 0x00000011
        assert sha1(b"PKL3\x80\x03X\x03\x00\x00\x00fooq\x00.").hexdigest().upper() == "788B085646895FE0185AAD076A9FD05F9D436EE2"

        with expected(Exception("packet size exceeded")):
            RpcUnmarshaler(("msgpack", "pickle"), 1)(p)

        def unmarshal(p):
            um = RpcUnmarshaler(("msgpack", "pickle"), 1024)
            while p:
                i = randint(1, 10)
                pp, p = p[:i], p[i:]
                r = um(pp)
                assert (p and not r) or (not p and r)
            return r

        assert RpcUnmarshaler(("msgpack", "pickle"), 1024)(p) == ("pickle", "foo")

        t = Timeout(10.0)
        while not t.expired:
            assert unmarshal(p) == ("pickle", "foo")

        with expected(Exception("unsupported marshaling method")):
            RpcUnmarshaler(("msgpack", ), 1024)(p)

        with expected(Exception("request hash mismatch")):
            RpcUnmarshaler(("pickle", ), 1024)(p[:8] + b"0" + p[9:])

        um = RpcUnmarshaler(("pickle", ), 1024)
        um(b"00000010")
        with expected(Exception("request length exceeded")):
            um(p[8:])

        um = RpcUnmarshaler(("pickle", ), 1024)
        um(b"00000010389C94157D9C6C6B3F9231E3F4E2EFD3FF1271A4")
        with expected(EOFError):
            um(p[48:-1])

        ###

        with expected(Exception("packet size exceeded")):
            RpcMarshaler(("msgpack", "pickle"), 1)("foo")

        m = RpcMarshaler(("msgpack", "pickle"), 1024)
        t, p = m("foo")

        assert t == "msgpack"
        assert p == b"\x00\x00\x00\x08\x06\x9d\x93\x14'\xdc\xfbCO\x9f5[\x8eS+\x9b$~\xc5,MSGP\xa3foo"
        assert len(b"MSGP\xa3foo") == 0x00000008
        assert sha1(b"MSGP\xa3foo").digest() == b"\x06\x9d\x93\x14'\xdc\xfbCO\x9f5[\x8eS+\x9b$~\xc5,"

        with expected(Exception("packet size exceeded")):
            RpcUnmarshaler(("msgpack", "pickle"), 1)(p)

        assert RpcUnmarshaler(("msgpack", "pickle"), 1024)(p) == ("msgpack", "foo")

        t = Timeout(10.0)
        while not t.expired:
            assert unmarshal(p) == ("msgpack", "foo")

        with expected(Exception("unsupported marshaling method")):
            RpcUnmarshaler(("pickle", ), 1024)(p)

        with expected(Exception("request hash mismatch")):
            RpcUnmarshaler(("msgpack", ), 1024)(p[:4] + b"\x00" + p[5:])

        um = RpcUnmarshaler(("msgpack", ), 1024)
        um(b"\x00\x00\x00\x07")
        with expected(Exception("request length exceeded")):
            um(p[4:])

        um = RpcUnmarshaler(("msgpack", ), 1024)
        um(b"\x00\x00\x00\x07")
        um(b"\x041\xb0\x02U\xa4C\x93\xb9<^\x06\xe0\xaa\x94\xa4y\xfd\xa8\xd7")
        with expected(InsufficientDataException):
            um(p[24:-1])

    test_marshaling()

    ###################################

    test_interface_config = dict \
    (
    protocol = "rpc",
    random_port = -63000,
    max_connections = 100,
    broadcast_address = ("0.0.0.0/255.255.255.255", 12481),
    flock_id = "SELF_TEST",
    marshaling_methods = ("pickle", "msgpack"),
    max_packet_size = 1048576,
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    local_addresses, broadcast_port = test_interface_config["broadcast_address"]
    bind_address, broadcast_address = local_addresses.split("/")

    ###################################

    def test_create_broadcast_socket():

        s1 = _create_receiving_broadcast_socket(broadcast_port)
        s2 = _create_receiving_broadcast_socket(broadcast_port)
        s = _create_sending_broadcast_socket(bind_address)

        p = urandom(64)
        s.sendto(p, (broadcast_address, broadcast_port))

        assert select([s1], [], [], 3.0)[0] == [s1]
        packet = s1.recvfrom(57344)[0]
        assert packet == p

        assert select([s2], [], [], 3.0)[0] == [s2]
        packet = s2.recvfrom(57344)[0]
        assert packet == p

    test_create_broadcast_socket()

    ###################################

    def test_start_stop_interface():

        def process_request(module, method, args, kwargs):
            pass

        with active_interface("rpc", cage_name = "cage_start_stop", **interface_config(process_request = process_request)) as ifc:
            assert ifc.listener_address[0] == bind_address
            assert 63000 <= ifc.listener_address[1] < 64000
            assert ifc._broadcast_port == 12481

    test_start_stop_interface()

    ###################################

    def test_create_discovery_request():

        params = pmnc.config_resource_rpc.copy()
        del params["protocol"]           # normally resource.py does this
        del params["pool__idle_timeout"] # normally transaction.py does this
        del params["pool__max_age"]
        r = Resource("test", pool__resource_name = "cage123", **params)

        rq, rsp = r._create_discovery_request()
        rq = rq.decode("ascii"); rsp = rsp.decode("ascii")
        assert by_regex("^PYTHOMNIC3K-REQUEST:SELF_TEST:[0-9A-F]{16}:DISCOVER-CAGE:cage123$")(rq)
        rid = rq.split(":", 4)[2]
        assert rsp == "PYTHOMNIC3K-RESPONSE:SELF_TEST:{0:s}:".format(rid)

    test_create_discovery_request()

    ###################################

    def test_discover_once():

        def process_request(module, method, args, kwargs):
            pass

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_discover_once")) as ifc:

            s = _create_sending_broadcast_socket(bind_address)

            request_id = b2a_hex(urandom(8)).decode("ascii").upper()
            request = "PYTHOMNIC3K-REQUEST:SELF_TEST:{0:s}:DISCOVER-CAGE:{1:s}".\
                      format(request_id, "cage_discover_once").encode("ascii")
            response = "PYTHOMNIC3K-RESPONSE:SELF_TEST:{0:s}:ssl://*:{1:d}/".\
                       format(request_id, ifc.listener_address[1]).encode("ascii")

            for i in range(3):
                s.sendto(request, (broadcast_address, ifc._broadcast_port))
                sleep(0.5)

            t = Timeout(3.0)
            while not t.expired and select([s], [], [], t.remain)[0]:
                packet = s.recvfrom(57344)[0]
                if packet == response:
                    break
            else:
                raise Exception("no discovery response")

    test_discover_once()

    ###################################

    def test_advertisement():

        def process_request(module, method, args, kwargs):
            pass

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_advertisement", ad_periods = (1.0, ))) as ifc:

            assert not ifc.get_cages()
            assert ifc.get_nodes("cage_advertisement") == {}

            timeout = Timeout(5.0)
            while not timeout.expired:
                nodes = ifc.get_nodes("cage_advertisement")
                if nodes:
                    assert len(nodes) == 1
                    loc = nodes["self_test"]
                    assert valid_location(loc)
                    assert loc.endswith(":{0:d}/".format(ifc.listener_address[1]))
                    break
                sleep(1.0)
            else:
                assert False, "should have advertised itself"

        sleep(3.0)
        ifc._purge_known_cages()
        assert ifc.get_cages() == { "cage_advertisement" }
        assert ifc.get_nodes("cage_advertisement") == {}

    test_advertisement()

    ###################################

    def test_exact_location():

        def process_request(module, method, args, kwargs):
            return "been there"

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_exact_location", ad_periods = (1.0, ))) as ifc:

            fake_request(10.0)

            # wait for the cage to be advertised

            nodes = ifc.get_nodes("cage_exact_location")
            while not nodes and not pmnc.request.expired:
                sleep(1.0)
                nodes = ifc.get_nodes("cage_exact_location")
            assert nodes, "should have advertised itself"

            # then initiate connection to the now known exact address

            exact_locations = { "cage_exact_location": nodes["self_test"] }

            resource = pmnc.protocol_rpc.Resource("self_test.cage_exact_location",
                                                  broadcast_address = ("UNUSED/UNUSED", 0),
                                                  discovery_timeout = 3.0,
                                                  multiple_timeout_allowance = 0.0,
                                                  flock_id = "UNUSED",
                                                  marshaling_methods = ("msgpack", "pickle"),
                                                  max_packet_size = 1048576,
                                                  exact_locations = exact_locations, # same as in config file
                                                  pool__resource_name = "cage_exact_location")
            resource.connect()
            try:
                resource.begin_transaction("xid", source_module_name = __name__,
                                           transaction_options = {}, resource_args = (),
                                           resource_kwargs = {})
                try:
                    result = resource.module.method()
                except:
                    resource.rollback()
                    raise
                else:
                    resource.commit()
            finally:
                resource.disconnect()

            assert result == "been there"

    test_exact_location()

    ###################################

    def test_resource_success():

        def process_request(module, method, args, kwargs):
            return eval(args[0])

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_resource_success")) as ifc:

            fake_request(10.0)
            pmnc.request.describe("it's my request all right")

            xa = pmnc.transaction.create()
            xa.rpc__cage_resource_success.foo.bar("module, method, args, kwargs", "param", a = "b")
            xa.rpc__cage_resource_success.biz.baz("pmnc.request.to_dict()")
            r1, r2 = xa.execute()

            assert r1 in \
                (
                    ("foo", "bar", ("module, method, args, kwargs", "param"), { "a": "b" }),
                    ["foo", "bar", ["module, method, args, kwargs", "param"], { "a": "b" }],
                )

            deadline = r2.pop("deadline")
            assert abs(deadline - (time() + pmnc.request.remain)) < 0.01

            assert r2 == dict \
                         (
                            protocol = pmnc.request.protocol,
                            description = "it's my request all right",
                            parameters = dict(auth_tokens = { "source_cage": __cage__ }),
                            interface = pmnc.request.interface,
                            unique_id = pmnc.request.unique_id,
                            log_levels = [],
                         )

    test_resource_success()

    ###################################

    def test_resource_failure():

        def process_request(module, method, args, kwargs):
            1 / 0

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_resource_failure")) as ifc:

            fake_request(10.0)

            try:
                pmnc.transaction.rpc__cage_resource_failure.foo.bar()
            except RPCError as e:
                assert e.description.startswith("ZeroDivisionError")
            else:
                assert False

    test_resource_failure()

    ###################################

    def test_resource_local_timeout():

        def process_request(module, method, args, kwargs):
            assert 1.5 < pmnc.request.remain < 2.5 # interface timeout takes over the received request timeout
            sleep(3.0)

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_resource_local_timeout", request_timeout = 2.0)) as ifc:

            fake_request(5.0)

            try:
                pmnc.transaction.rpc__cage_resource_local_timeout.foo.bar()
            except ResourceError as e:
                assert str(e).startswith("unexpected eof reading data from cage_resource_local_timeout") # as interface drops the connection
            else:
                assert False

    test_resource_local_timeout()

    ###################################

    def test_resource_remote_timeout():

        def process_request(module, method, args, kwargs):
            assert 2.5 < pmnc.request.remain < 3.5 # inherited remote timeout
            sleep(4.0)

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_resource_remote_timeout")) as ifc:

            fake_request(5.0)
            sleep(2.0)

            try:
                pmnc.transaction.rpc__cage_resource_remote_timeout.foo.bar()
            except (ResourceError, TransactionExecutionError) as e:
                assert str(e).startswith("request deadline reading data from cage_resource_remote_timeout") or \
                       str(e).startswith("request deadline waiting for intermediate result from resource rpc__cage_resource_remote_timeout")
            else:
                assert False

    test_resource_remote_timeout()

    ###################################

    def test_pmnc_discovery():

        fake_request(5.0)

        try:
            pmnc("never_existed").foo.bar()
        except ResourceError as e:
            assert str(e) == "no discovery response from cage never_existed in 3.0 second(s)"
            assert e.recoverable and e.terminal
        else:
            assert False

    test_pmnc_discovery()

    ###################################

    def test_pmnc_success():

        def process_request(module, method, args, kwargs):
            return module, method, args, kwargs, pmnc.request.to_dict()

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_pmnc_success")) as ifc:
            fake_request(5.0)
            pmnc.request.describe("yes it is")
            result = pmnc("cage_pmnc_success").foo.bar(1, "foo", biz = "baz")

        module, method, args, kwargs, request = result
        assert module == "foo" and method == "bar" and args in ( (1, "foo"), [1, "foo"] ) and kwargs == { "biz": "baz" }

        deadline = request.pop("deadline")
        assert abs(deadline - (time() + pmnc.request.remain)) < 0.01

        assert request == dict \
                          (
                              protocol = pmnc.request.protocol,
                              description = "yes it is",
                              parameters = dict(auth_tokens = { "source_cage": __cage__ }),
                              interface = pmnc.request.interface,
                              unique_id = pmnc.request.unique_id,
                              log_levels = [],
                          )

    test_pmnc_success()

    ###################################

    def test_pmnc_failure():

        def process_request(module, method, args, kwargs):
            {}["not-there"]

        with active_interface("rpc", **interface_config(process_request = process_request,
                              cage_name = "cage_pmnc_failure")) as ifc:
            fake_request(5.0)
            try:
                pmnc("cage_pmnc_failure").foo.bar()
            except RPCError as e:
                assert e.description.startswith("KeyError(\"'not-there'\") in process_request()")
                assert not e.recoverable and not e.terminal
            else:
                assert False

    test_pmnc_failure()

    ###################################

    def test_two_calls():

        def process_request1(module, method, args, kwargs):
            assert pmnc.request.parameters["auth_tokens"]["source_cage"] == __cage__
            pmnc.request.parameters["auth_tokens"]["source_cage"] = "set once"
            return pmnc("cage_second").module.second()

        def process_request2(module, method, args, kwargs):
            return pmnc.request.to_dict()

        with active_interface("rpc1", **interface_config(process_request = process_request1,
                              cage_name = "cage_first")) as ifc1:
            with active_interface("rpc2", **interface_config(process_request = process_request2,
                                  cage_name = "cage_second")) as ifc2:
                fake_request(10.0)
                result = pmnc("cage_first").foo.bar()

        deadline = result.pop("deadline")
        assert abs(deadline - (time() + pmnc.request.remain)) < 0.01

        assert result == dict \
                         (
                            protocol = pmnc.request.protocol,
                            description = None,
                            parameters = dict(auth_tokens = { "source_cage": "set once" }),
                            interface = pmnc.request.interface,
                            unique_id = pmnc.request.unique_id,
                            log_levels = [],
                         )

    test_two_calls()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
