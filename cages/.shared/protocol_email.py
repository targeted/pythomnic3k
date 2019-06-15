#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
###############################################################################
#
# This module contains an implementation of e-mail interface (polling over POP3)
# and resource (sending over SMTP). This code also supports POP3S and SMTPS
# (same protocols but over SSL rather than plain TCP) and this is configurable.
#
# Sample e-mail interface configuration (config_interface_email_1.py):
#
# config = dict \
# (
# protocol = "email",                                 # meta
# request_timeout = None,                             # meta, optional
# server_address = ("mail.domain.com", 110),          # tcp
# connect_timeout = 3.0,                              # tcp
# ssl_key_cert_file = None,                           # ssl, optional filename
# ssl_ca_cert_file = None,                            # ssl, optional filename
# ssl_ciphers = None,                                 # ssl, optional str
# ssl_protocol = None,                                # ssl, optional "SSLv23", "TLSv1", "TLSv1_1", "TLSv1_2" or "TLS"
# ssl_server_hostname = None,                         # ssl, optional str
# ssl_ignore_hostname = False,                        # ssl, ignore certificate common/alt name name mismatch
# interval = 30.0,                                    # email
# username = "user",                                  # email
# password = "pass",                                  # email
# )
#
# Sample processing module (interface_email_1.py):
#
# def process_request(request, response):
#     message_id = request["message_id"]
#     message = request["message"]
#     subject = message["Subject"]
#
# Sample e-mail resource configuration (config_resource_email_1.py)
#
# config = dict \
# (
# protocol = "email",                                 # meta
# server_address = ("mail.domain.com", 25),           # tcp
# connect_timeout = 3.0,                              # tcp
# ssl_key_cert_file = None,                           # ssl, optional filename
# ssl_ca_cert_file = None,                            # ssl, optional filename
# ssl_ciphers = None,                                 # ssl, optional str
# ssl_protocol = None,                                # ssl, optional "SSLv23", "TLSv1", "TLSv1_1", "TLSv1_2" or "TLS"
# ssl_server_hostname = None,                         # ssl, optional str
# ssl_ignore_hostname = False,                        # ssl, ignore certificate common/alt name name mismatch
# encoding = "windows-1251",                          # email
# helo = "hostname",                                  # email
# auth_method = None,                                 # email, optional "PLAIN" or "LOGIN"
# username = None,                                    # email, optional
# password = None,                                    # email, optional
# )
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.email_1.send("from@mail", "to@mail", "subject", "message text") # simple text message
# ---OR---
# xa.email_1.send("From <from@mail>", "To <to@mail>", "subject", "message text",
#                 { "X-Name": "Value", ... },                # optional extra header fields
#                 (("foo.jpg", "image/jpeg", b"JPEG"), ...)) # optional tuple of attached files
# ---OR---
# xa.email_1.send("From <from@mail>", "To <to@mail>", "subject", "message text",
#                 { "X-Name": "Value", ... },
#                 (("foo.jpg", "image/jpeg", b"JPEG"), ...),
#                 (("application/pdf", b"PDF", ()),                           # even more optional tuple of
#                  ("text/html", b"<html><img src=\"cid:{bar.jpg}\"></html>", # alternatives, each with its
#                   (("bar.jpg", "image/jpeg", b"JPEG"), ...)),               # own tuple of related files
#                  ...))
# xa.execute() # sending e-mail message yields no result
#
# or if the only transaction participant:
#
# pmnc.transaction.email_1.send(...)
#
# Note:
#
# Within each alternative content its related files could be referenced
# as cid:{filename} which is replaced with a proper unique Content-ID.
#
# Pythomnic3k project
# (c) 2005-2019, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource", "decode_value", "format_addr" ]

###############################################################################

import email.utils; from email.utils import parseaddr
import email.parser; from email.parser import FeedParser
import email.message; from email.message import Message
import email.header; from email.header import decode_header
import io; from io import StringIO, BytesIO
import re; from re import compile as regex, escape, sub
import base64; from base64 import b64encode
import os; from os import urandom, SEEK_END, path as os_path
import binascii; from binascii import b2a_hex
import threading; from threading import current_thread
import collections; from collections import OrderedDict as odict

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, typecheck_with_exceptions, one_of, \
                                        optional, tuple_of, dict_of, by_regex, either
import pmnc.threads; from pmnc.threads import HeavyThread
import pmnc.request; from pmnc.request import fake_request
import pmnc.thread_pool; from pmnc.thread_pool import WorkUnitTimedOut
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, \
                           ResourceError, ResourceInputParameterError

###############################################################################

@typecheck
def decode_value(s: str) -> str: # takes "=?B?FOO?=", returns "foo"
    try:
        return " ".join(value.strip() if isinstance(value, str) else
                        value.decode(charset or "ascii").strip() # see http://bugs.python.org/issue1079
                        for value, charset in decode_header(s))
    except (UnicodeDecodeError, LookupError):
        return "?"

def _parse_addr(s: str): # takes "=?B?FOO?= <foo@bar>", returns ("foo", "foo@bar")
    parsed = parseaddr(s)
    return decode_value(parsed[0]), parsed[1]

@typecheck
def format_addr(s: str) -> str: # takes "=?B?FOO?= <foo@bar>", returns "Foo <foo.bar>"
    name, addr = _parse_addr(s)
    return "{0:s} <{1:s}>".format(name, addr) if name else addr if addr else "<>"

def _encode_value(s, encoding): # takes "foo", returns "=?B?FOO?=" unless all ascii
    try:
        s.encode("ascii")
    except UnicodeEncodeError:
        return "=?" + encoding + "?B?" + b64encode(s.encode(encoding)).decode("ascii") + "?="
    else:
        return s

def _encode_addr(addr, encoding): # takes "Foo <foo@bar>", returns "=?B?FOO?= <foo@bar>"
    name, addr = _parse_addr(addr)
    return _encode_value(name, encoding) + " <" + addr + ">" if name else addr

def _encode_auth_plain(username, password): # returns SMTP login token for AUTH PLAIN
    return b64encode(b"\x00" + username.encode("ascii") +
                     b"\x00" + password.encode("ascii")).decode("ascii")

def _encode_auth_login(s): # returns BASE64 encoded token for AUTH LOGIN
    return b64encode(s.encode("ascii")).decode("ascii")

###############################################################################

class Interface: # email (POP3) interface

    @typecheck
    def __init__(self, name: str, *,
                 server_address: (str, int),
                 connect_timeout: float,
                 ssl_key_cert_file: optional(os_path.isfile),
                 ssl_ca_cert_file: optional(os_path.isfile),
                 ssl_ciphers: optional(str) = None,
                 ssl_protocol: optional(one_of("SSLv23", "TLSv1", "TLSv1_1", "TLSv1_2", "TLS")) = None,
                 ssl_server_hostname: optional(str) = None,
                 ssl_ignore_hostname: optional(bool) = False,
                 username: str,
                 password: str,
                 interval: float,
                 request_timeout: optional(float) = None,
                 **kwargs): # this kwargs allows for extra application-specific
                            # settings in config_interface_email_X.py

        self._name = name
        self._server_address = server_address
        self._connect_timeout = connect_timeout
        self._ssl_key_cert_file = ssl_key_cert_file
        self._ssl_ca_cert_file = ssl_ca_cert_file
        self._ssl_ciphers = ssl_ciphers
        self._ssl_protocol = ssl_protocol
        self._ssl_server_hostname = ssl_server_hostname
        self._ssl_ignore_hostname = ssl_ignore_hostname
        self._username = username
        self._password = password
        self._interval = interval

        self._request_timeout = request_timeout or \
            pmnc.config_interfaces.get("request_timeout") # this is now static

        # this set tracks messages that have been processed but
        # not deleted on the server due to deletion failure

        self._processed_messages = set()

        if pmnc.request.self_test == __name__: # self-test
            self._process_request = kwargs["process_request"]

    name = property(lambda self: self._name)

    ###################################

    def start(self):
        self._poller = HeavyThread(target = self._poller_proc,
                                   name = "{0:s}:poll".format(self._name))
        self._poller.start()

    def cease(self):
        self._poller.stop()

    def stop(self):
        pass

    ###################################

    # this thread periodically polls the mail server for new messages

    def _poller_proc(self):

        interval = self._interval

        while not current_thread().stopped(interval):
            try:

                # attach a fake request to this thread so that network access
                # in TcpResource times out properly; since this timeout will
                # include connection establishment and POP3 protocol overhead,
                # the timeout is increased by connect_timeout

                timeout = self._connect_timeout + self._request_timeout
                fake_request(timeout = timeout, interface = self._name)
                pmnc.request.describe("polling mailbox")

                # since there is no other way to commit POP3 receiving transaction
                # but to disconnect, we have to limit each session to one message

                self._pop3 = pmnc.protocol_tcp.TcpResource(self._username,
                                                           server_address = self._server_address,
                                                           connect_timeout = self._connect_timeout,
                                                           ssl_key_cert_file = self._ssl_key_cert_file,
                                                           ssl_ca_cert_file = self._ssl_ca_cert_file,
                                                           ssl_ciphers = self._ssl_ciphers,
                                                           ssl_protocol = self._ssl_protocol,
                                                           ssl_server_hostname = self._ssl_server_hostname,
                                                           ssl_ignore_hostname = self._ssl_ignore_hostname)
                self._pop3.connect()
                try:

                    self._login()

                    message_count = self._get_message_count()
                    if message_count == 0:
                        self._logout()
                        continue

                    message_id = self._get_message_id()
                    if message_id not in self._processed_messages: # don't process the message again
                        message = self._get_message()
                        delete_message = self._process_message(message_id, message)
                    else:
                        delete_message = True # simply delete the message on the server

                    try:
                        if delete_message:
                            self._delete_message()
                        self._logout()
                    except:
                        if delete_message:               # if there was an error deleting
                            pmnc.log.error(exc_string()) # message, reconnect immediately
                            interval = 0.0
                            continue
                        else:
                            raise

                    if delete_message:
                        self._processed_messages.remove(message_id)

                finally:
                    self._pop3.disconnect()

            except:
                pmnc.log.error(exc_string())
                interval = self._interval
            else:
                interval = self._interval if message_count <= 1 else 0.0

    ###################################

    def _process_message(self, message_id, message):

        # now that the message is parsed, we know more about the request

        subject = message["Subject"]
        subject = decode_value(subject) if subject is not None else ""
        from_ = format_addr(message["From"] or "")

        request_description = "e-mail \"{0:s}\" from {1:s}".format(subject, from_)
        pmnc.request.describe(request_description)

        # create a new request for processing the message, note that the timeout
        # depends on how much time the current request has spent receiving

        request = pmnc.interfaces.begin_request(
                    timeout = min(self._request_timeout, pmnc.request.remain),
                    interface = self._name, protocol = "email",
                    parameters = dict(auth_tokens = dict()),
                    description = request_description)

        # enqueue the request and wait for its completion

        try:
            pmnc.interfaces.enqueue(request, self.wu_process_request,
                                    (message_id, message)).wait()
        except WorkUnitTimedOut:
            pmnc.log.error("message processing timed out")
            success = None
        except:
            pmnc.log.error("message processing failed: {0:s}".format(exc_string()))
            success = False
        else:
            if pmnc.log.debug:
                pmnc.log.debug("message processing succeeded")
            self._processed_messages.add(message_id)
            success = True
        finally:
            pmnc.interfaces.end_request(success, request)
            return success == True

    ###################################

    # this method is a work unit executed by one of the interface pool threads
    # if this method fails, the exception is rethrown in _process_message in wait()

    @typecheck
    def wu_process_request(self, message_id: str, message: Message):

        # see for how long the request was on the execution queue up to this moment
        # and whether it has expired in the meantime, if it did there is no reason
        # to proceed and we simply bail out

        if pmnc.request.expired:
            pmnc.log.error("request has expired and will not be processed")
            return

        try:
            with pmnc.performance.request_processing():
                request = dict(message_id = message_id, message = message)
                self._process_request(request, {})
        except:
            pmnc.log.error(exc_string()) # don't allow an exception to be silenced
            raise                        # when this work unit is not waited upon

    ###################################

    def _process_request(self, request, response):
        handler_module_name = "interface_{0:s}".format(self._name)
        pmnc.__getattr__(handler_module_name).process_request(request, response)

    ###################################

    def _login(self):
        self._send_line(None)
        self._send_line("USER {0:s}".format(self._username))
        self._send_line("PASS {0:s}".format(self._password))

    def _get_message_count(self):
        stat = self._send_line("STAT")
        return int(stat.split(" ")[0])

    def _get_message_id(self):
        uidl = self._send_line("UIDL 1")
        return uidl.split(" ")[1]

    def _get_message(self):
        return self._send_line("RETR 1", True)

    def _delete_message(self):
        self._send_line("DELE 1")

    def _logout(self):
        self._send_line("QUIT")

    ###################################

    def _send_line(self, line: optional(str), read_message: optional(bool) = False):

        self._response = BytesIO()
        self._response_offset = 0
        self._read_message = read_message # this triggers the response_handler callback behaviour

        if line:
            if pmnc.log.debug:
                pmnc.log.debug(">> {0:s}".format(line))
        data = "{0:s}\r\n".format(line).encode("ascii") if line is not None else b""
        response = self._pop3.send_request(data, self.response_handler)

        if pmnc.log.debug:
            if not read_message:
                pmnc.log.debug("<< {0:s}".format(response))
            else:
                pmnc.log.debug("<< ...message body...")

        return response

    ###################################

    _content_type_charset = regex("^content-type[ \\t]*:[ \\t]*text/.*;[ \\t]*charset=\"?([A-Za-z0-9_-]+)\"?[ \\t]*(?:;|$)")
    _folded_charset = regex("^[ \\t]+charset=\"?([A-Za-z0-9_-]+)\"?[ \\t]*(?:;|$)")

    @typecheck
    def response_handler(self, data_b: bytes): # this callback method is invoked by
                                               # TcpResource as server response is read

        self._response.seek(0, SEEK_END)
        self._response.write(data_b)

        while True:

            self._response.seek(self._response_offset)

            line_b = self._response.readline()
            if line_b.endswith(b"\r\n"):
                line_b = line_b[:-2]
            elif line_b.endswith(b"\n"):
                line_b = line_b[:-1]
            else:
                return None # no complete line at the end of buffer, wait for more data

            response_offset, self._response_offset = self._response_offset, self._response.tell()

            if response_offset == 0: # first (possibly the only) line has been read
                line = line_b.decode("ascii", "replace")
                if line.startswith("+OK"):
                    if self._read_message:
                        self._message_parser = FeedParser() # proceed to message parsing
                        self._message_encoding = None
                        continue
                    else:
                        return line[3:].strip() # the successful one-line response
                elif line.startswith("-ERR"):
                    raise Exception(line[4:].strip())
                else:
                    raise Exception(line.strip())

            # hack: convert bytes to str using deduced encoding and detect encoding change

            line = line_b.decode(self._message_encoding or "ascii", "replace")

            line_lc = line.lower()
            charset_match = self._content_type_charset.match(line_lc) or \
                            self._folded_charset.match(line_lc)
            if charset_match:
                charset = charset_match.groups()[0]
                try:
                    assert b"123".decode(charset) == "123"
                except:
                    pass
                else:
                    self._message_encoding = charset

            # message lines are fed into the parser one by one

            if line != ".":
                self._message_parser.feed(line + "\r\n")
            else:
                return self._message_parser.close() # return the parsed message

###################################

class MimeMessage:

    def __init__(self, from_addr, to_addr, subject, text, headers,
                 attached_files, alternatives, *, encoding):

        self._encoding = encoding
        self._message = BytesIO()
        self._boundary = []
        self._headers = odict(headers)

        self._append_header("MIME-Version", "1.0")
        self._append_header("From", _encode_addr(from_addr, self._encoding))
        self._append_header("To", _encode_addr(to_addr, self._encoding))
        self._append_header("Subject", _encode_value(subject, self._encoding))
        if alternatives and not attached_files:
            self._append_header("Content-Disposition", "inline")
            self._append_boundary("multipart/alternative")
        else:
            self._append_boundary("multipart/mixed")
        self._write_first_boundary()

        if alternatives:
            if attached_files:
                self._append_header("Content-Disposition", "inline")
                self._append_boundary("multipart/alternative")
                self._write_first_boundary()
            self._write_text(text)
            self._write_alternatives(alternatives)
            if attached_files:
                self._write_last_boundary()
        else:
            self._append_header("Content-Disposition", "inline")
            self._write_text(text)

        if attached_files:
            self._write_files(attached_files, "attachment")

        self._write_last_boundary()

    ###################################

    data = property(lambda self: self._message.getvalue())

    ###################################

    def _generate_content_ids(self, files):
        return { filename: self._guid() for filename, mime_type, content in files }

    ###################################

    def _append_header(self, name, value):
        self._headers[name] = value

    ###################################

    def _write_text(self, text):
        self._write_content("text/plain", text)

    ###################################

    def _write_alternatives(self, alternatives):
        for mime_type, content, related_files in alternatives:
            self._write_next_boundary()
            if related_files:
                self._append_boundary("multipart/related")
                self._write_first_boundary()
                content_ids = self._generate_content_ids(related_files)
                self._write_content(mime_type, content, None, content_ids)
                self._write_files(related_files, "related", content_ids)
                self._write_last_boundary()
            else:
                self._write_content(mime_type, content)

    ###################################

    def _write_files(self, files, disposition, content_ids = {}):
        for filename, mime_type, content in files:
            self._write_next_boundary()
            self._write_file(mime_type, content, filename,
                             disposition, content_ids.get(filename))

    ###################################

    def _write_file(self, mime_type, content, filename, disposition, content_id):
        content_disposition = "{0:s}; filename=\"{1:s}\"".\
            format(disposition, _encode_value(filename, self._encoding))
        self._append_header("Content-Disposition", content_disposition)
        if content_id:
            self._append_header("Content-ID", "<{0:s}>".format(content_id))
        self._write_content(mime_type, content, filename)

    ###################################

    def _write_content(self, mime_type, content, filename = None, content_ids = {}):
        mime_type, content = self._content_bytes(mime_type, content, content_ids)
        content_type = mime_type
        if filename: content_type += "; name=\"{0:s}\"".format(_encode_value(filename, self._encoding))
        self._append_header("Content-Type", content_type)
        self._append_header("Content-Transfer-Encoding", "base64")
        self._write_headers()
        for i in range(0, len(content), 57):
            self._message.write(b64encode(content[i:i+57]) + b"\r\n")

    ###################################

    def _content_bytes(self, mime_type, content, content_ids):
        if mime_type.startswith("text/"):
            if isinstance(content, str):
                mime_type += "; charset={0:s}".format(self._encoding)
                content = content.encode(self._encoding)
            for filename, content_id in content_ids.items():
                cid_regex = "cid:\\{{{0:s}\\}}".format(escape(filename))
                cid_url = "cid:{0:s}".format(content_id)
                content = sub(cid_regex.encode(self._encoding),
                              cid_url.encode("ascii"), content)
        if not isinstance(content, bytes):
            raise Exception("content of type {0:s} should be bytes".format(mime_type))
        return mime_type, content

    ###################################

    def _write_headers(self):
        for name, value in self._headers.items():
            value = _encode_value(value.strip(), self._encoding)
            self._message.write("{0:s}: {1:s}\r\n".\
                format(name.strip(), value).encode("ascii"))
        self._message.write(b"\r\n")
        self._headers.clear()

    ###################################

    def _append_boundary(self, content_type):
        boundary = self._guid()
        self._append_header("Content-Type", "{0:s}; boundary=\"{1:s}\"".format(content_type, boundary))
        self._boundary.append(boundary.encode("ascii"))

    ###################################

    def _guid(self):
        return b2a_hex(urandom(16)).decode("ascii")

    ###################################

    def _write_first_boundary(self):
        self._write_headers()
        self._write_next_boundary()

    ###################################

    def _write_next_boundary(self):
        self._message.write(b"--" + self._boundary[-1] + b"\r\n")

    ###################################

    def _write_last_boundary(self):
        self._message.write(b"--" + self._boundary[-1] + b"--\r\n")
        self._boundary.pop()

###############################################################################

class Resource(TransactionalResource): # email (SMTP) resource

    valid_mime_type = by_regex("^[A-Za-z0-9~`!#$%^&*+|{}'._-]+/[A-Za-z0-9~`!#$%^&*+|{}'._-]+$")

    @typecheck
    def __init__(self, name: str, *,
                 server_address: (str, int),
                 connect_timeout: float,
                 ssl_key_cert_file: optional(os_path.isfile),
                 ssl_ca_cert_file: optional(os_path.isfile),
                 ssl_ciphers: optional(str) = None,
                 ssl_protocol: optional(one_of("SSLv23", "TLSv1", "TLSv1_1", "TLSv1_2", "TLS")) = None,
                 ssl_server_hostname: optional(str) = None,
                 ssl_ignore_hostname: optional(bool) = False,
                 encoding: str,
                 helo: str,
                 auth_method: optional(one_of("PLAIN", "LOGIN")) = "PLAIN",
                 username: optional(str),
                 password: optional(str)):

        TransactionalResource.__init__(self, name)

        self._encoding = encoding
        self._helo = helo
        self._auth_method = auth_method if (username is not None and password is not None) else None
        self._username = username
        self._password = password

        self._smtp = \
            pmnc.protocol_tcp.TcpResource(name,
                                          server_address = server_address,
                                          connect_timeout = connect_timeout,
                                          ssl_key_cert_file = ssl_key_cert_file,
                                          ssl_ca_cert_file = ssl_ca_cert_file,
                                          ssl_ciphers = ssl_ciphers,
                                          ssl_protocol = ssl_protocol,
                                          ssl_server_hostname = ssl_server_hostname,
                                          ssl_ignore_hostname = ssl_ignore_hostname)

    ###################################

    def connect(self):
        TransactionalResource.connect(self)
        self._smtp.connect()
        self._send_line(None, { 220 })
        self._send_line("HELO {0:s}".format(self._helo), { 250 })
        if self._auth_method == "PLAIN":
            auth_plain = _encode_auth_plain(self._username, self._password)
            self._send_line("AUTH PLAIN {0:s}".format(auth_plain), { 235 })
        elif self._auth_method == "LOGIN":
            self._send_line("AUTH LOGIN", { 334 })
            auth_username = _encode_auth_login(self._username)
            self._send_line(auth_username, { 334 })
            auth_password = _encode_auth_login(self._password)
            self._send_line(auth_password, { 235 })
        self._graceful_close = True

    ###################################

    def begin_transaction(self, *args, **kwargs):
        TransactionalResource.begin_transaction(self, *args, **kwargs)
        self._message_sent = False

    ###################################

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def send(self, from_addr: str, to_addr: str, subject: str, text: str,
             headers: optional(dict_of(str, str)) = None,
             attached_files: optional(tuple_of((str, valid_mime_type, either(str, bytes)))) = None,
             alternatives: optional(tuple_of((valid_mime_type, either(str, bytes), tuple_of((str, valid_mime_type, either(str, bytes)))))) = None):

        try:

            message_b = MimeMessage(from_addr, to_addr, subject, text,
                                    headers or {}, attached_files or (), alternatives or (),
                                    encoding = self._encoding).data

            pmnc.log.info("sending e-mail message \"{0:s}\" for {1:s}, {2:d} byte(s)".\
                          format(subject, format_addr(to_addr), len(message_b)))
            try:
                self._send_line("MAIL FROM:<{0:s}>".format(_parse_addr(from_addr)[1]), { 250 })
                self._send_line("RCPT TO:<{0:s}>".format(_parse_addr(to_addr)[1]), { 250 })
                self._send_line("DATA", { 354 })
                self._message_sent = True
                self._send_bytes(message_b)
            except:
                pmnc.log.warning("sending e-mail message \"{0:s}\" for {1:s} failed: {2:s}".\
                                 format(subject, format_addr(to_addr), exc_string()))
                raise

        except ResourceError: # server response with unexpected code
            raise
        except:
            ResourceError.rethrow(recoverable = True) # no irreversible changes

    ###################################

    def commit(self):
        self._send_line(".", { 250 })
        pmnc.log.info("e-mail message has been sent")

    ###################################

    def rollback(self):
        self._graceful_close = False
        self.expire() # there is no other way to interrupt message sending but to disconnect

    ###################################

    def disconnect(self):
        try:
            try:
                if self._graceful_close:
                    self._send_line("QUIT", { 221 })
                elif self._message_sent:
                    pmnc.log.info("e-mail message has been aborted")
            finally:
                self._smtp.disconnect()
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            TransactionalResource.disconnect(self)

    ###################################

    _response_line_parser = regex("^([0-9]+)([ -])(.*)$")

    # this callback method is invoked by TcpResource as server response is read

    @typecheck
    def response_handler(self, data: bytes) -> optional((int, str)):

        self._response.seek(0, SEEK_END)
        self._response.write(data.decode("ascii", "replace"))

        while True: # SMTP response can consist of multiple lines

            self._response.seek(self._response_offset)
            line = self._response.readline()

            if line.endswith("\r\n"):
                line = line[:-2]
            elif line.endswith("\n"):
                line = line[:-1]
            else:
                return None # no complete line at the end of buffer

            self._response_offset = self._response.tell()

            try:
                retcode, delim, message = \
                    self._response_line_parser.findall(line)[0]
            except:
                raise Exception("invalid server response: {0:s}".format(line))

            if delim == " ": # this is the last line of the response
                return int(retcode), message

    ################################### NETWORK-RELATED METHODS

    def _send_bytes(self, data, positive_retcodes = None):

        self._response = StringIO() # SMTP server responses are treated as ascii-only
        self._response_offset = 0

        if positive_retcodes:
            retcode, message = self._smtp.send_request(data, self.response_handler)
            if pmnc.log.debug:
                pmnc.log.debug("<< {0:d} {1:s}".format(retcode, message))
            if retcode not in positive_retcodes:
                raise ResourceError(code = retcode, description = message, recoverable = True)
        else:
            if pmnc.log.debug:
                pmnc.log.debug(">> ...message body...")
            self._smtp.send_request(data)

    ###################################

    def _send_line(self, line, positive_retcodes):

        if line:
            if pmnc.log.debug:
                pmnc.log.debug(">> {0:s}".format(line))

        data = "{0:s}\r\n".format(line).encode("ascii") if line else b""
        self._send_bytes(data, positive_retcodes)

###############################################################################

def self_test():

    from expected import expected
    from time import sleep
    from interlocked_queue import InterlockedQueue
    from pmnc.self_test import active_interface

    ###################################

    test_interface_config = dict \
    (
    protocol = "email",
    server_address = ("mail.domain.com", 110),
    connect_timeout = 3.0,
    ssl_key_cert_file = None,
    ssl_ca_cert_file = None,
    ssl_ciphers = None,
    ssl_protocol = None,
    ssl_server_hostname = None,
    ssl_ignore_hostname = False,
    username = "recipient@domain.com", # recipient's POP3 username
    password = "password", # recipient's POP3 password
    interval = 3.0,
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    from_addr = "sender@domain.com" # sender's address
    to_addr = "recipient@domain.com"   # recipient's address

    russian = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"

    ###################################

    def _decode_message_part(m):
        if m.get_filename():
            if m.get_content_maintype() == "text":
                return m.get_filename(), m.get_payload(decode = True).decode(m.get_content_charset("ascii"))
            else:
                return m.get_filename(), m.get_content_type(), m.get_payload(decode = True)
        else:
            if m.get_content_maintype() == "text":
                return m.get_payload(decode = True).decode(m.get_content_charset("ascii"))
            else:
                return m.get_content_type(), m.get_payload(decode = True)

    ###################################

    def test_encoding_routines():

        assert _encode_value("", "windows-1251") == ""
        assert _encode_value("foo@bar", "windows-1251") == "foo@bar"
        assert _encode_value(russian, "windows-1251") == \
               "=?windows-1251?B?wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva" \
               "3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX29/j5/Pv6/f7/?="
        assert _encode_value("foo@bar", "cp866") == "foo@bar"
        assert _encode_value(russian, "cp866") == \
               "=?cp866?B?gIGCg4SF8IaHiImKi4yNjo+QkZKTlJWWl5iZnJuan" \
               "Z6foKGio6Sl8aanqKmqq6ytrq/g4eLj5OXm5+jp7Ovq7e7v?="

        assert decode_value("") == ""
        assert decode_value("foo@bar") == "foo@bar"
        assert decode_value("=?windows-1251?B?wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva"
                            "3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX29/j5/Pv6/f7/?=") == russian
        assert decode_value("=?cp866?B?gIGCg4SF8IaHiImKi4yNjo+QkZKTlJWWl5iZnJuan"
                            "Z6foKGio6Sl8aanqKmqq6ytrq/g4eLj5OXm5+jp7Ovq7e7v?=") == russian

        assert decode_value("=?windows-1251?B?gIGCg4SF8IaHiImKi4yNjo+QkZKTlJWWl5iZnJuan"
                            "Z6foKGio6Sl8aanqKmqq6ytrq/g4eLj5OXm5+jp7Ovq7e7v?=") == "?"
        r1 = "АБВ"; r2 = "абв"
        r = "foo " + _encode_value(r1, "windows-1251") + " " + _encode_value(r2, "cp866")
        assert pmnc.protocol_email.decode_value(r) == "foo АБВ абв"

        assert decode_value("=?windows-1251?Q?=C0=C1=C2?=") == "АБВ"
        assert decode_value("=?ascii?Q?=FF=FF=FF?=") == "?"
        assert decode_value("=?never-existed?Q?foo?=") == "?"

        assert _parse_addr("foo@bar") == _parse_addr("<foo@bar>") == ("", "foo@bar")
        assert _parse_addr("Foo <foo@bar>") == ("Foo", "foo@bar")
        r = _encode_value(russian, "windows-1251")
        assert _parse_addr("Foo " + r + " <foo@bar>") == ("Foo " + russian, "foo@bar")

        assert _encode_auth_plain("user", "pass") == "AHVzZXIAcGFzcw=="

        assert _encode_addr("", "windows-1251") == ""
        assert _encode_addr("foo@bar", "windows-1251") == _encode_addr("<foo@bar>", "cp866") == "foo@bar"
        assert _encode_addr("Foo <foo@bar>", "windows-1251") == "Foo <foo@bar>"
        assert _encode_addr(russian + " <foo@bar>", "windows-1251") == \
               "=?windows-1251?B?wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva" \
               "3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX29/j5/Pv6/f7/?= <foo@bar>"

        assert format_addr("") == "<>"
        assert format_addr("foo@bar") == "foo@bar"
        assert format_addr("Foo <foo@bar>") == "Foo <foo@bar>"
        assert pmnc.protocol_email.format_addr("=?windows-1251?B?wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva"
                                               "3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX29/j5/Pv6/f7/?= <foo@bar>") == \
               russian + " <foo@bar>"

    test_encoding_routines()

    ###################################

    def test_message_formatting():

        def test_message(*args, **kwargs):
            _guid = MimeMessage._guid
            try:
                boundary_count = 0
                def guid(self):
                    nonlocal boundary_count
                    boundary_count += 1
                    return "guid/{0:d}".format(boundary_count)
                MimeMessage._guid = guid
                return MimeMessage(*args, **kwargs).data.replace(b"\r\n", b"\n")
            finally:
                MimeMessage._guid = _guid

        ############################### empty message

        assert test_message("", "", "", "", {},
                            (), (), encoding = "windows-1251") == b"""\
MIME-Version: 1.0
From:\x20
To:\x20
Subject:\x20
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: text/plain; charset=windows-1251
Content-Transfer-Encoding: base64

--guid/1--
"""

        ############################### simple text message

        assert test_message("from@foo", "to@bar", "subject", "text",
                            {}, (), (), encoding = "windows-1251") == b"""\
MIME-Version: 1.0
From: from@foo
To: to@bar
Subject: subject
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: text/plain; charset=windows-1251
Content-Transfer-Encoding: base64

dGV4dA==
--guid/1--
"""

        ############################### text message with international characters

        assert test_message("Отправитель <sender>", "Получатель <recipient>",
                            "Тема", russian, { "X-Rus": russian, "X-Ascii": "value" },
                            (), (), encoding = "windows-1251") == b"""\
X-Rus: =?windows-1251?B?wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX29/j5/Pv6/f7/?=
X-Ascii: value
MIME-Version: 1.0
From: =?windows-1251?B?zvLv8ODi6PLl6/w=?= <sender>
To: =?windows-1251?B?z+7r8/fg8uXr/A==?= <recipient>
Subject: =?windows-1251?B?0uXs4A==?=
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: text/plain; charset=windows-1251
Content-Transfer-Encoding: base64

wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX2
9/j5/Pv6/f7/
--guid/1--
"""

        ############################### same as previous, different encoding

        assert test_message("Отправитель <sender>", "Получатель <recipient>",
                            "Тема", russian, { "X-Rus": russian, "X-Ascii": "value" },
                            (), (), encoding = "cp866") == b"""\
X-Rus: =?cp866?B?gIGCg4SF8IaHiImKi4yNjo+QkZKTlJWWl5iZnJuanZ6foKGio6Sl8aanqKmqq6ytrq/g4eLj5OXm5+jp7Ovq7e7v?=
X-Ascii: value
MIME-Version: 1.0
From: =?cp866?B?juKv4KCiqOKlq+w=?= <sender>
To: =?cp866?B?j66r4+eg4qWr7A==?= <recipient>
Subject: =?cp866?B?kqWsoA==?=
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: text/plain; charset=cp866
Content-Transfer-Encoding: base64

gIGCg4SF8IaHiImKi4yNjo+QkZKTlJWWl5iZnJuanZ6foKGio6Sl8aanqKmqq6ytrq/g4eLj5OXm
5+jp7Ovq7e7v
--guid/1--
"""

        ############################### text message with attachment

        assert test_message("from", "to", "subject", "text", {},
                            (("foo.jpg", "image/jpeg", b"JPEG"), ), (),
                            encoding = "ascii") == b"""\
MIME-Version: 1.0
From: from
To: to
Subject: subject
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: text/plain; charset=ascii
Content-Transfer-Encoding: base64

dGV4dA==
--guid/1
Content-Disposition: attachment; filename="foo.jpg"
Content-Type: image/jpeg; name="foo.jpg"
Content-Transfer-Encoding: base64

SlBFRw==
--guid/1--
"""

        ############################### text message with html alternative

        assert test_message("from@foo.com", "to@bar.com", "subject", "text", {}, (),
                            (("text/html", "<html/>", ()), ),
                            encoding = "windows-1251") == b"""\
MIME-Version: 1.0
From: from@foo.com
To: to@bar.com
Subject: subject
Content-Disposition: inline
Content-Type: multipart/alternative; boundary="guid/1"

--guid/1
Content-Type: text/plain; charset=windows-1251
Content-Transfer-Encoding: base64

dGV4dA==
--guid/1
Content-Type: text/html; charset=windows-1251
Content-Transfer-Encoding: base64

PGh0bWwvPg==
--guid/1--
"""

        ############################### text message with html alternative with files

        assert test_message("from@foo.com", "to@bar.com", "subject", "text", {}, (),
                            (("text/html", "<html><img src=\"cid:{foo.jpg}\" alt=\"фотка\"></html>",
                             (("foo.jpg", "image/jpeg", b"JPEG"), )), ),
                            encoding = "utf-8") == b"""\
MIME-Version: 1.0
From: from@foo.com
To: to@bar.com
Subject: subject
Content-Disposition: inline
Content-Type: multipart/alternative; boundary="guid/1"

--guid/1
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: base64

dGV4dA==
--guid/1
Content-Type: multipart/related; boundary="guid/2"

--guid/2
Content-Type: text/html; charset=utf-8
Content-Transfer-Encoding: base64

PGh0bWw+PGltZyBzcmM9ImNpZDpndWlkLzMiIGFsdD0i0YTQvtGC0LrQsCI+PC9odG1sPg==
--guid/2
Content-Disposition: related; filename="foo.jpg"
Content-ID: <guid/3>
Content-Type: image/jpeg; name="foo.jpg"
Content-Transfer-Encoding: base64

SlBFRw==
--guid/2--
--guid/1--
"""

        ############################### text message with html alternative and attachment

        assert test_message("from@foo.com", "to@bar.com", "subject", "text", {},
                            (("файл.zip", "application/zip", b"ZIP"), ),
                            (("text/html", "<html/>", ()), ),
                            encoding = "windows-1251") == b"""\
MIME-Version: 1.0
From: from@foo.com
To: to@bar.com
Subject: subject
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: multipart/alternative; boundary="guid/2"

--guid/2
Content-Type: text/plain; charset=windows-1251
Content-Transfer-Encoding: base64

dGV4dA==
--guid/2
Content-Type: text/html; charset=windows-1251
Content-Transfer-Encoding: base64

PGh0bWwvPg==
--guid/2--
--guid/1
Content-Disposition: attachment; filename="=?windows-1251?B?9ODp6y56aXA=?="
Content-Type: application/zip; name="=?windows-1251?B?9ODp6y56aXA=?="
Content-Transfer-Encoding: base64

WklQ
--guid/1--
"""

        ############################### text message with html alternative with files and attachment

        assert test_message("from@foo.com", "to@bar.com", "subject", "text", {},
                            (("attachment.zip", "application/zip", b"ZIP"), ),
                            (("text/html", "<html><img src=\"cid:{foo.jpg}\" alt=\"фотка\"></html>",
                             (("foo.jpg", "image/jpeg", b"JPEG"), )), ),
                            encoding = "windows-1251") == b"""\
MIME-Version: 1.0
From: from@foo.com
To: to@bar.com
Subject: subject
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: multipart/alternative; boundary="guid/2"

--guid/2
Content-Type: text/plain; charset=windows-1251
Content-Transfer-Encoding: base64

dGV4dA==
--guid/2
Content-Type: multipart/related; boundary="guid/3"

--guid/3
Content-Type: text/html; charset=windows-1251
Content-Transfer-Encoding: base64

PGh0bWw+PGltZyBzcmM9ImNpZDpndWlkLzQiIGFsdD0i9O7y6uAiPjwvaHRtbD4=
--guid/3
Content-Disposition: related; filename="foo.jpg"
Content-ID: <guid/4>
Content-Type: image/jpeg; name="foo.jpg"
Content-Transfer-Encoding: base64

SlBFRw==
--guid/3--
--guid/2--
--guid/1
Content-Disposition: attachment; filename="attachment.zip"
Content-Type: application/zip; name="attachment.zip"
Content-Transfer-Encoding: base64

WklQ
--guid/1--
"""

        ############################### similar to previous but with str/bytes reversed

        assert test_message("from@foo.com", "to@bar.com", "subject", "text", {},
                            (("attachment.txt", "text/plain", russian), ),
                            (("text/plain", russian.encode("cp866"),
                             (("foo.txt", "text/plain", russian), )), ),
                            encoding = "windows-1251") == b"""\
MIME-Version: 1.0
From: from@foo.com
To: to@bar.com
Subject: subject
Content-Type: multipart/mixed; boundary="guid/1"

--guid/1
Content-Disposition: inline
Content-Type: multipart/alternative; boundary="guid/2"

--guid/2
Content-Type: text/plain; charset=windows-1251
Content-Transfer-Encoding: base64

dGV4dA==
--guid/2
Content-Type: multipart/related; boundary="guid/3"

--guid/3
Content-Type: text/plain
Content-Transfer-Encoding: base64

gIGCg4SF8IaHiImKi4yNjo+QkZKTlJWWl5iZnJuanZ6foKGio6Sl8aanqKmqq6ytrq/g4eLj5OXm
5+jp7Ovq7e7v
--guid/3
Content-Disposition: related; filename="foo.txt"
Content-ID: <guid/4>
Content-Type: text/plain; charset=windows-1251; name="foo.txt"
Content-Transfer-Encoding: base64

wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX2
9/j5/Pv6/f7/
--guid/3--
--guid/2--
--guid/1
Content-Disposition: attachment; filename="attachment.txt"
Content-Type: text/plain; charset=windows-1251; name="attachment.txt"
Content-Transfer-Encoding: base64

wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva3d7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX2
9/j5/Pv6/f7/
--guid/1--
"""

    test_message_formatting()

    ###################################

    def test_invalid_parameters():

        fake_request(10.0)

        with expected(ResourceInputParameterError):
            pmnc.transaction.email_1.send("", "", "", b"") # bytes for text ?!

    test_invalid_parameters()

    ###################################

    def drain_mailbox():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):
            while loopback_queue.pop(10.0) is not None:
                pass

    drain_mailbox()

    ###################################

    def test_send_text():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):

            fake_request(10.0)

            pmnc.transaction.email_1.send(from_addr, to_addr, "subject", ".")

            m = loopback_queue.pop(10.0)

        assert m["Subject"] == "subject"

        mps = [ _decode_message_part(mp) for mp in m.walk() if not mp.is_multipart() ]
        assert len(mps) == 1
        assert mps[0] == "."

        assert loopback_queue.pop(30.0) is None

    test_send_text()

    ###################################

    def test_send_attachments():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):

            fake_request(10.0)

            pmnc.transaction.\
                email_1.send("Отправитель <{0:s}>".format(from_addr),
                             "Получатель <{0:s}>".format(to_addr),
                             "тема", "текст" * 1000, { "X-Foo": "Bar" },
                             (("текст", "text/plain", russian),
                              ("image.jpg", "image/jpeg", b"\x00\x00\x00")))

            m = loopback_queue.pop(10.0)

        assert decode_value(m["Subject"]) == "тема"
        assert m["X-Foo"] == "Bar"

        mps = [ _decode_message_part(mp) for mp in m.walk() if not mp.is_multipart() ]
        assert len(mps) == 3
        assert mps[0] == "текст" * 1000
        assert mps[1] == ("=?windows-1251?B?8uXq8fI=?=", russian)
        assert mps[2] == ("image.jpg", "image/jpeg", b"\x00\x00\x00")

    test_send_attachments()

    ###################################

    def test_send_alternatives():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):

            fake_request(10.0)

            pmnc.transaction.\
                email_1.send(from_addr, to_addr, "subject", "text", {}, (),
                             (("text/html", "<html/>", ()), ))

            m = loopback_queue.pop(10.0)

        assert decode_value(m["Subject"]) == "subject"

        mps = [ _decode_message_part(mp) for mp in m.walk() if not mp.is_multipart() ]
        assert len(mps) == 2
        assert mps[0] == "text"
        assert mps[1] == "<html/>"

    test_send_alternatives()

    ###################################

    def test_send_related():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):

            fake_request(10.0)

            pmnc.transaction.\
                email_1.send(from_addr, to_addr, "subject", "text", {}, (),
                             (("text/html", "<html><img src=\"cid:{foo.jpg}\"></html>",
                               (("foo.jpg", "image/jpeg", b"JPEG"), )), ))

            m = loopback_queue.pop(10.0)

        assert decode_value(m["Subject"]) == "subject"

        mps = [ _decode_message_part(mp) for mp in m.walk() if not mp.is_multipart() ]
        assert len(mps) == 3
        assert mps[0] == "text"
        assert by_regex("^<html><img src=\"cid:[0-9a-f]{32}\"></html>$")(mps[1])
        assert mps[2] ==  ("foo.jpg", "image/jpeg", b"JPEG")

    test_send_related()

    ###################################

    def test_sending_failure():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):

            fake_request(10.0)

            try:
                pmnc.transaction.email_1.send(from_addr, "", "subject", "text")
            except ResourceError as e:
                assert e.code == 501 # syntax error in parameters or arguments
                assert e.recoverable and e.terminal

            assert loopback_queue.pop(10.0) is None

    test_sending_failure()

    ###################################

    def test_processing_failure():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            1 / 0

        fake_request(10.0)

        pmnc.transaction.email_1.send(from_addr, to_addr, "subject", "text")

        with active_interface("email_1", **interface_config(process_request = process_request)):
            assert loopback_queue.pop(10.0) is None

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):
            assert loopback_queue.pop(10.0) is not None

    test_processing_failure()

    ###################################

    def test_processing_timeout():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            sleep(pmnc.request.remain + 1.0)

        fake_request(10.0)

        pmnc.transaction.email_1.send(from_addr, to_addr, "subject", "text")

        with active_interface("email_1", **interface_config(process_request = process_request)):
            assert loopback_queue.pop(10.0) is None

        def process_request(request, response):
            loopback_queue.push(request["message"])

        with active_interface("email_1", **interface_config(process_request = process_request)):
            assert loopback_queue.pop(10.0) is not None

    test_processing_timeout()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
