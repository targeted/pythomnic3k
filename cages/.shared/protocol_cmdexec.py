#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module contains an implementation of resource for running OS processes
# and communicating to them via stdin/stdout/stderr. Each single process invocation
# is considered a separate request to the resource.
#
# Sample cmdexec resource configuration (config_resource_cmdexec_1.py)
#
# config = dict \
# (
# protocol = "cmdexec",                          # meta
# executable = "/usr/local/bin/foo",             # cmdexec
# arguments = ("default_arg1", "default_arg2"),  # cmdexec, optional tuple
# environment = { "NAME": "VALUE" },             # cmdexec, optional dict
# )
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.cmdexec_1.execute("arg1", "arg2", stdin = b"input data",
#                      environment = { "FOO": "BAR" })
# retcode, stdout, stderr = xa.execute()[0]
#
# or if the only transaction participant:
#
# retcode, stdout, stderr = pmnc.transaction.\
#     cmdexec_1.execute("arg1", "arg2", stdin = b"input data",
#                       environment = { "FOO": "BAR" })
#
# This effectively invokes
# /usr/local/bin/foo default_arg1 default_arg2 arg1 arg2 < "input data"
# with additional environment parameters NAME=VALUE and FOO=BAR
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

import os; from os import environ
import io; from io import BytesIO

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, typecheck_with_exceptions, \
                                        optional, tuple_of, with_attr, dict_of, anything
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, \
                                                          ResourceInputParameterError
import pmnc.popen; from pmnc.popen import popen

###############################################################################

class Resource(TransactionalResource): # resource for executing OS commands

    @typecheck
    def __init__(self, name: str, *,
                 executable: str,
                 arguments: optional(tuple_of(anything)) = (),
                 environment: optional(dict_of(str, anything)) = {}):

        TransactionalResource.__init__(self, name)
        self._executable = executable
        self._arguments = [ str(arg) for arg in arguments ]
        self._environment = os.environ.copy()
        self._environment.update({ k: str(v) for k, v in environment.items() })

    ###################################

    def connect(self):
        TransactionalResource.connect(self)
        self.expire() # this is a one-time resource, it expires upon every use
        self._process = None

    ###################################

    @typecheck
    def wu_write_stdin(self, pipe: with_attr("write", "close"), data: bytes):
        try:
            try:
                stdin = BytesIO(data)
                data = stdin.read(512)
                while data:
                    pipe.write(data)
                    data = stdin.read(512)
            finally:
                pipe.close()
        except:
            pmnc.log.error(exc_string()) # log and rethrow
            raise

    ###################################

    @typecheck
    def wu_read_stdout(self, pipe: with_attr("read")) -> bytes:
        try:
            stdout = BytesIO()
            data = pipe.read(512)
            while data:
                stdout.write(data)
                data = pipe.read(512)
        except:
            pmnc.log.error(exc_string()) # log and rethrow
            raise
        else:
            return stdout.getvalue()

    ###################################

    @typecheck
    def wu_read_stderr(self, pipe: with_attr("read")) -> bytes:
        try:
            stderr = BytesIO()
            data = pipe.read(512)
            while data:
                stderr.write(data)
                data = pipe.read(512)
        except:
            pmnc.log.error(exc_string()) # log and rethrow
            raise
        else:
            return stderr.getvalue()

    ###################################

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def execute(self, *args, stdin: optional(bytes) = None,
                environment: optional(dict_of(str, anything)) = None) -> (int, bytes, bytes):

        # note that for each request _three_ work units are enqueued
        # to a separate private thread pool, the size of the private
        # thread pool should therefore be thrice as large

        thread_pool = pmnc.shared_pools.get_private_thread_pool(
                        self.pool_name, self.pool_size * 3)

        pmnc.log.info("starting process {0:s} with {1:d} stdin byte(s)".\
                      format(self._executable, len(stdin or b"")))
        try:

            # the environment dict is updated in place, which is ok
            # since it's a one-time instance

            if environment: self._environment.update(environment)
            self._process = popen(self._executable, *(self._arguments + list(args)),
                                  env = self._environment)

            # allocate work units on the private thread pool

            wu_stdin = \
                thread_pool.enqueue(pmnc.request.clone(), self.wu_write_stdin,
                                    (self._process.stdin, stdin or b""), {})

            wu_stdout = \
                thread_pool.enqueue(pmnc.request.clone(), self.wu_read_stdout,
                                    (self._process.stdout, ), {})

            wu_stderr = \
                thread_pool.enqueue(pmnc.request.clone(), self.wu_read_stderr,
                                    (self._process.stderr, ), {})

            # failure in any of those, as well as a timeout will abort the execution

            assert wu_stdin.wait() is None
            stdout = wu_stdout.wait()
            stderr = wu_stderr.wait()

            # after all communications have completed, wait for the process to complete

            retcode = self._process.wait()

        except:
            pmnc.log.warning("process {0:s} failed: {1:s}".\
                             format(self._executable, exc_string()))
            raise
        else:
            pmnc.log.info("process returned exit code {0:d} with {1:d} stdout byte(s) and "
                          "{2:d} stderr byte(s)".format(retcode, len(stdout), len(stderr)))

        # note that even non-zero retcode is treated as valid,
        # the interpretation is left to the transaction owner

        return retcode, stdout, stderr

    ###################################

    def disconnect(self):
        try:
            if self._process and self._process.poll() is None:
                pmnc.log.warning("killing runaway process {0:s} (pid {1:d})".\
                                 format(self._executable, self._process.pid))
                self._process.kill()
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            TransactionalResource.disconnect(self)

################################################################################

def self_test():

    from decimal import Decimal
    from threading import current_thread, Thread
    from expected import expected
    from time import sleep
    from tempfile import mkstemp
    from os import close, path as os_path, remove, O_RDONLY, getenv
    from sys import platform

    from pmnc.request import fake_request
    from pmnc.thread_pool import WorkUnitTimedOut
    from pmnc.popen import fopen

    ###################################

    def test_resource_void():

        fake_request(1.0)

        na = Resource("na/1", executable = "never#existed",
                      arguments = (), environment = {})
        na.connect()
        try:
            assert na.expired
            na.set_pool_info("na", 1)
            with expected(Exception):
                na.execute()
        finally:
            na.disconnect()

    test_resource_void()

    ###################################

    def test_resource_echo():

        fake_request(1.0)

        echo = Resource("echo/1", executable = "echo",
                        arguments = (), environment = {})
        echo.connect()
        try:
            assert echo.expired
            echo.set_pool_info("echo", 1)
            retcode, stdout, stderr = \
                echo.execute("foo", 123, Decimal("1.00"))
            assert retcode == 0
            assert stdout.rstrip() == b"foo 123 1.00"
            assert stderr == b""
        finally:
            echo.disconnect()

    test_resource_echo()

    ###################################

    def test_resource_environment():

        fake_request(1.0)

        path = getenv("PATH")

        if platform == "win32":
            cmd = getenv("COMSPEC")
            args = ("/c", "echo", "%PATH%", "%FOO%", "%BIZ%")
        else:
            cmd = getenv("SHELL")
            args = ("-c", "echo $PATH $FOO $BIZ")

        echo = Resource("cmd/1", executable = cmd,
                        arguments = args, environment = { "FOO": "BAR" })
        echo.connect()
        try:
            assert echo.expired
            echo.set_pool_info("echo", 1)
            retcode, stdout, stderr = echo.execute(environment = { "BIZ": "BAZ" })
            assert retcode == 0
            assert stdout.rstrip() == path.encode("ascii") + b" BAR BAZ"
            assert stderr == b""
        finally:
            echo.disconnect()

    test_resource_environment()

    ###################################

    def test_resource_cat():

        fake_request(3.0)

        cat = Resource("cat/1", executable = "cat",
                       arguments = (), environment = {})
        cat.connect()
        try:
            cat.set_pool_info("cat", 1)
            b = b"*" * 262144
            assert cat.execute(stdin = b) == (0, b, b"")
        finally:
            cat.disconnect()

    test_resource_cat()

    ###################################

    def test_resource_err():

        fake_request(1.0)

        cat = Resource("cat/1", executable = "cat",
                       arguments = ("never#existed", ), environment = {})
        cat.connect()
        try:
            cat.set_pool_info("cat", 1)
            retcode, stdout, stderr = cat.execute()
            assert retcode != 0 and stdout == b"" and b"never#existed" in stderr
        finally:
            cat.disconnect()

    test_resource_err()

    ###################################

    def test_resource_kill():

        fake_request(2.0)

        ping = Resource("sleep/1", executable = "sleep",
                        arguments = (), environment = {})
        ping.connect()
        try:
            ping.set_pool_info("sleep", 1)
            with expected(WorkUnitTimedOut):
                ping.execute(4)
        finally:
            ping.disconnect()

    test_resource_kill()

    ###################################

    def test_handle_inheritance():

        fake_request(6.0)

        h, f = mkstemp(); close(h)
        h = fopen(f, O_RDONLY)

        def remove_thread_proc():
            sleep(2.0)
            close(h)
            remove(f)

        try:

            p_sleep = Resource("sleep/1", executable = "sleep",
                               arguments = (), environment = {})
            p_sleep.connect()
            try:
                p_sleep.set_pool_info("sleep", 1)
                th = Thread(target = remove_thread_proc)
                th.daemon = 1; th.start()
                try:
                    p_sleep.execute("4")
                finally:
                    th.join()
            finally:
                p_sleep.disconnect()

        finally:
            assert not os_path.exists(f)

    test_handle_inheritance()

    ###################################

    def test_transaction():

        fake_request(10.0)

        # cmdexec_1 is configured as just "cat"

        retcode, stdout, stderr = pmnc.transaction.cmdexec_1.execute(stdin = b"foo")
        assert retcode == 0 and stdout == b"foo" and stderr == b""

        retcode, stdout, stderr = pmnc.transaction.cmdexec_1.execute("never#existed")
        assert retcode != 0 and stdout == b""
        assert b"never#existed" in stderr

    test_transaction()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

################################################################################
# EOF
