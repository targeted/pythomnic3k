#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module contains an implementation of XMLRPC interface/resource.
#
# Sample XMLRPC interface configuration (config_interface_xmlrpc_1.py):
#
# config = dict \
# (
# protocol = "xmlrpc",                                       # meta
# request_timeout = None,                                    # meta, optional
# listener_address = ("127.0.0.1", 8000),                    # tcp
# max_connections = 100,                                     # tcp
# ssl_key_cert_file = None,                                  # ssl, optional filename
# ssl_ca_cert_file = None,                                   # ssl, optional filename
# ssl_ciphers = None,                                        # ssl, optional str
# response_encoding = "windows-1251",                        # http
# original_ip_header_fields = ("X-Forwarded-For", ),         # http
# keep_alive_support = True,                                 # http
# keep_alive_idle_timeout = 120.0,                           # http
# keep_alive_max_requests = 10,                              # http
# )
#
# Sample processing module (interface_xmlrpc_1.py):
#
# def process_request(request, response):
#   module, method = request["method"].split(".")
#   args = request["args"]
#   result = pmnc.__getattr__(module).__getattr__(method)(*args)
#   response["result"] = result
#
# Sample XMLRPC resource configuration (config_resource_xmlrpc_1.py)
#
# config = dict \
# (
# protocol = "xmlrpc",                                       # meta
# server_address = ("127.0.0.1", 8000),                      # tcp
# connect_timeout = 3.0,                                     # tcp
# ssl_key_cert_file = None,                                  # ssl, optional filename
# ssl_ca_cert_file = None,                                   # ssl, optional filename
# ssl_ciphers = None,                                        # ssl, optional str
# extra_headers = { "Authorization": "Basic dXNlcjpwYXNz" }, # http
# http_version = "HTTP/1.1",                                 # http
# server_uri = "/xmlrpc",                                    # xmlrpc
# request_encoding = "windows-1251",                         # xmlrpc
# )
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.xmlrpc_1.Module.Method(*args)
# result = xa.execute()[0]
#
# or if the only transaction participant:
#
# result = pmnc.transaction.xmlrpc_1.Module.Method(*args)
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource", "process_http_request" ]

###############################################################################

import os; from os import path as os_path
import xmlrpc.client; from xmlrpc.client import loads, dumps, Fault

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import typecheck, typecheck_with_exceptions, \
                                        optional, tuple_of, dict_of, callable
import exc_string; from exc_string import exc_string
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, ResourceError

###############################################################################

class Interface: # XMLRPC interface built on top of HTTP interface

    @typecheck
    def __init__(self, name: str, *,
                 listener_address: (str, int),
                 max_connections: int,
                 ssl_key_cert_file: optional(os_path.isfile),
                 ssl_ca_cert_file: optional(os_path.isfile),
                 ssl_ciphers: optional(str) = None,
                 response_encoding: str,
                 original_ip_header_fields: tuple_of(str),
                 keep_alive_support: bool,
                 keep_alive_idle_timeout: float,
                 keep_alive_max_requests: int,
                 request_timeout: optional(float) = None,
                 **kwargs): # this kwargs allows for extra application-specific
                            # settings in config_interface_xmlrpc_X.py

        # create an instance of underlying HTTP interface

        request_timeout = request_timeout or \
                          pmnc.config_interfaces.get("request_timeout") # this is now static

        self._http_interface = \
            pmnc.protocol_http.Interface(name,
                                         listener_address = listener_address,
                                         max_connections = max_connections,
                                         ssl_key_cert_file = ssl_key_cert_file,
                                         ssl_ca_cert_file = ssl_ca_cert_file,
                                         ssl_ciphers = ssl_ciphers,
                                         response_encoding = response_encoding,
                                         original_ip_header_fields = original_ip_header_fields,
                                         allowed_methods = ("POST", ),
                                         keep_alive_support = keep_alive_support,
                                         keep_alive_idle_timeout = keep_alive_idle_timeout,
                                         keep_alive_max_requests = keep_alive_max_requests,
                                         gzip_content_types = (),
                                         request_timeout = request_timeout)

        # override the default process_http_request method of the created HTTP interface,
        # having the HTTP handler method to be called through a pmnc call allows
        # online modifications to this module, when it is reloaded

        if pmnc.request.self_test == __name__: # self-test
            self.process_xmlrpc_request = kwargs["process_xmlrpc_request"]

        self._http_interface.process_http_request = \
            lambda http_request, http_response: \
                pmnc.__getattr__(__name__).process_http_request(http_request, http_response,
                                                                self.process_xmlrpc_request,
                                                                response_encoding = response_encoding)

    name = property(lambda self: self._http_interface.name)
    listener_address = property(lambda self: self._http_interface.listener_address)

    ###################################

    def start(self):
        self._http_interface.start()

    def cease(self):
        self._http_interface.cease()

    def stop(self):
        self._http_interface.stop()

    ###################################

    def process_xmlrpc_request(self, request, response):
        handler_module_name = "interface_{0:s}".format(self.name)
        pmnc.__getattr__(handler_module_name).process_request(request, response)

###############################################################################

def process_http_request(http_request: dict, http_response: dict,
                         process_xmlrpc_request: callable, *,
                         response_encoding: str):

    assert http_request["method"] == "POST"
    headers = http_request["headers"]
    content = http_request["content"]

    content_type = headers.get("content-type", "application/octet-stream")
    if not content_type.startswith("text/xml"):
        http_response["status_code"] = 415 # unsupported media type
        return

    # extract xmlrpc request from http request content, the parser
    # will deduce the bytes encoding from the <?xml encoding attribute

    try:
        args, method = loads(content)
    except:
        raise Exception("invalid XMLRPC request: {0:s}".format(exc_string()))

    # now we know more about the request

    auth_tokens = pmnc.request.parameters["auth_tokens"]
    pmnc.request.describe("XMLRPC{0:s} request {1:s} from {2:s}".\
                          format(auth_tokens["encrypted"] and "S" or "",
                                 method, auth_tokens["peer_ip"]))

    # the request contained a valid xmlrpc packet,
    # it would be polite to respond with one as well

    try:

        # populate the request parameters with XMLRPC-specific values

        pmnc.request.protocol = "xmlrpc"
        xmlrpc_request = dict(method = method, args = args)
        xmlrpc_response = dict(result = None)

        # invoke the application handler

        process_xmlrpc_request(xmlrpc_request, xmlrpc_response)

        # fetch the XMLRPC call result

        result = xmlrpc_response["result"]
        if result is None:
            result = ()

        # marshal the result in an XMLRPC packet

        content = dumps((result, ), methodresponse = True, encoding = response_encoding)

    except:
        error = exc_string()
        content = dumps(Fault(500, error), methodresponse = True, encoding = response_encoding) # 500 as in "Internal Server Error"
        pmnc.log.error("returning XMLRPC fault: {0:s}".format(error))
    else:
        if pmnc.log.debug:
            pmnc.log.debug("returning XMLRPC response")

    http_response["headers"]["content-type"] = "text/xml"
    http_response["content"] = content

###############################################################################

class Resource(TransactionalResource): # XMLRPC resource

    @typecheck
    def __init__(self, name, *,
                 server_address: (str, int),
                 connect_timeout: float,
                 ssl_key_cert_file: optional(os_path.isfile),
                 ssl_ca_cert_file: optional(os_path.isfile),
                 ssl_ciphers: optional(str) = None,
                 extra_headers: dict_of(str, str),
                 http_version: str,
                 server_uri: str,
                 request_encoding: str):

        TransactionalResource.__init__(self, name)

        self._server_uri = server_uri
        self._request_encoding = request_encoding

        self._http_resource = \
            pmnc.protocol_http.Resource(name,
                                        server_address = server_address,
                                        connect_timeout = connect_timeout,
                                        ssl_key_cert_file = ssl_key_cert_file,
                                        ssl_ca_cert_file = ssl_ca_cert_file,
                                        ssl_ciphers = ssl_ciphers,
                                        extra_headers = extra_headers,
                                        http_version = http_version)

    ###################################

    def connect(self):
        TransactionalResource.connect(self)
        self._attrs = []
        self._http_resource.connect()

    def disconnect(self):
        try:
            self._http_resource.disconnect()
        finally:
            TransactionalResource.disconnect(self)

    ###################################

    # overriding the following methods allows the contained HTTP
    # resource to time out at the same time with this resource

    def set_idle_timeout(self, idle_timeout):
        self._http_resource.set_idle_timeout(idle_timeout)
        TransactionalResource.set_idle_timeout(self, idle_timeout)

    def reset_idle_timeout(self):
        self._http_resource.reset_idle_timeout()
        TransactionalResource.reset_idle_timeout(self)

    def set_max_age(self, max_age):
        self._http_resource.set_max_age(max_age)
        TransactionalResource.set_max_age(self, max_age)

    def _expired(self):
        return self._http_resource.expired or \
               TransactionalResource._expired(self)

    ###################################

    def __getattr__(self, name):
        self._attrs.append(name)
        return self

    ###################################

    def __call__(self, *args):

        try:
            method, self._attrs = ".".join(self._attrs), []
            request = dumps(args, methodname = method, encoding = self._request_encoding)
            request_description = "XMLRPC request {0:s} to {1:s}".\
                                  format(method, self._http_resource.server_info)
        except:
            ResourceError.rethrow(recoverable = True)

        pmnc.log.info("sending {0:s}".format(request_description))
        try:

            status_code, headers, content = \
                self._http_resource.post(self._server_uri, request.encode(self._request_encoding),
                                         { "Content-Type": "text/xml" })

            if status_code != 200:
                raise Exception("HTTP request returned code {0:d}".format(status_code))

            result = loads(content)[0][0]

        except Fault as e:
            pmnc.log.warning("{0:s} returned fault {1:d}: {2:s}".\
                             format(request_description, e.faultCode, e.faultString))
            ResourceError.rethrow(code = e.faultCode,
                description = e.faultString, terminal = False)
        except:
            pmnc.log.warning("{0:s} failed: {1:s}".\
                             format(request_description, exc_string()))
            raise
        else:
            pmnc.log.info("XMLRPC request returned successfully")

        return result

###############################################################################

def self_test():

    from socket import socket, AF_INET, SOCK_STREAM
    from pmnc.request import fake_request
    from pmnc.self_test import active_interface

    def sendall(ifc, data):
        s = socket(AF_INET, SOCK_STREAM)
        s.connect(ifc.listener_address)
        s.sendall(data)
        return s

    def recvall(s):
        result = b""
        data = s.recv(1024)
        while data:
            result += data
            data = s.recv(1024)
        return result

    rus = "\u0410\u0411\u0412\u0413\u0414\u0415\u0401\u0416\u0417\u0418\u0419" \
          "\u041a\u041b\u041c\u041d\u041e\u041f\u0420\u0421\u0422\u0423\u0424" \
          "\u0425\u0426\u0427\u0428\u0429\u042c\u042b\u042a\u042d\u042e\u042f" \
          "\u0430\u0431\u0432\u0433\u0434\u0435\u0451\u0436\u0437\u0438\u0439" \
          "\u043a\u043b\u043c\u043d\u043e\u043f\u0440\u0441\u0442\u0443\u0444" \
          "\u0445\u0446\u0447\u0448\u0449\u044c\u044b\u044a\u044d\u044e\u044f"

    def post_string(ifc, method, s, request_encoding):
        req = "<?xml version=\"1.0\" encoding=\"{0:s}\"?>" \
              "<methodCall><methodName>{1:s}</methodName>" \
              "<params><param><value><string>{2:s}</string>" \
              "</value></param></params></methodCall>".format(request_encoding, method, s).encode(request_encoding)
        hdr = "POST / HTTP/1.0\nContent-Type: text/xml\nContent-Length: {0:d}\n\n".format(len(req))
        s = sendall(ifc, hdr.encode(request_encoding) + req)
        resp = recvall(s)
        assert resp.startswith(b"HTTP/1.1 200 OK\r\n")
        resp = resp.split(b"\r\n\r\n", 1)[1]
        return loads(resp)[0][0]

    ###################################

    test_interface_config = dict \
    (
    protocol = "xmlrpc",
    listener_address = ("127.0.0.1", 23673),
    max_connections = 100,
    ssl_key_cert_file = None,
    ssl_ca_cert_file = None,
    ssl_ciphers = None,
    response_encoding = "windows-1251",
    original_ip_header_fields = ("X-Forwarded-For", ),
    keep_alive_support = True,
    keep_alive_idle_timeout = 3.0,
    keep_alive_max_requests = 3,
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    def test_interface_start_stop():

        def process_xmlrpc_request(request, response):
            pass

        with active_interface("xmlrpc_1", **interface_config(process_xmlrpc_request = process_xmlrpc_request)):
            pass

    test_interface_start_stop()

    ###################################

    def test_interface_broken_requests():

        def process_xmlrpc_request(request, response):
            pass

        with active_interface("xmlrpc_1", **interface_config(process_xmlrpc_request = process_xmlrpc_request)) as ifc:

            s = sendall(ifc, b"POST / HTTP/1.0\nContent-Type: text/plain\n\n")
            resp = recvall(s)
            assert resp.startswith(b"HTTP/1.1 415 Unsupported Media Type\r\n")

            s = sendall(ifc, b"POST / HTTP/1.0\nContent-Type: text/xml\nContent-Length: 3\n\nfoo")
            resp = recvall(s)
            assert resp.startswith(b"HTTP/1.1 500 Internal Server Error\r\n")
            assert b"invalid XMLRPC request" in resp

    test_interface_broken_requests()

    ###################################

    def test_interface_marshaling():

        def process_xmlrpc_request(request, response):
            if request["method"] == "raise":
                raise Exception(request["args"][0])
            response["result"] = [request["method"], request["args"]]

        with active_interface("xmlrpc_1", **interface_config(process_xmlrpc_request = process_xmlrpc_request)) as ifc:

            assert post_string(ifc, "MethodName", "foo", "utf-8") == ["MethodName", ["foo"]]
            assert post_string(ifc, rus, rus, "cp866") == [rus, [rus]]

            try:
                post_string(ifc, "raise", "foo", "iso-8859-5")
            except Fault as e:
                assert e.faultCode == 500 and e.faultString.startswith("Exception(\"foo\")")
            else:
                assert False

            try:
                post_string(ifc, "raise", rus, "utf-8")
            except Fault as e:
                assert e.faultCode == 500 and e.faultString.startswith("Exception(\"" + rus + "\")")
            else:
                assert False

    test_interface_marshaling()

    ################################### TESTING RESOURCE

    def test_resource():

        def process_xmlrpc_request(request, response):
            if request["method"] == "ShouldBe.Failing":
                raise Exception(request["args"][0])
            else:
                response["result"] = request, pmnc.request.parameters["auth_tokens"]

        with active_interface("xmlrpc_1", **interface_config(process_xmlrpc_request = process_xmlrpc_request)):

            fake_request(10.0)

            for i in range(16):
                s = "*" * 2 ** i
                result = pmnc.transaction.xmlrpc_1.Module.Method(i, s, [ s ], { s: i })
                assert result == [ { "method": "Module.Method", "args": [ i, s, [ s ], { s: i } ] },
                                   { "username": "user", "peer_ip": "127.0.0.1", "password": "pass", "encrypted": False } ]

            try:
                pmnc.transaction.xmlrpc_1.ShouldBe.Failing("some error")
            except ResourceError as e:
                assert e.code == 500 and e.description.startswith("Exception(\"some error\")")
                assert not e.recoverable and not e.terminal

    test_resource()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
