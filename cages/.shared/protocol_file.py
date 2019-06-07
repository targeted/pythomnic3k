#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This module contains an implementation of file interface (periodic polling
# of a shared directory) and resource (reading/writing files in a shared directory).
#
# The interface processes the files in alphabetical order. If more files
# appear during one pass, they will only be picked up at next pass.
#
# The resource saves the data to temporary files first and renames them at
# commit. If the target file already exists it may be overwritten depending
# on the overwrite parameter.
#
# It would be polite to provide input files for the interface in the same
# atomic fashion, otherwise it could pick up an incomplete file.
#
# Sample file interface configuration (config_interface_file_1.py):
#
# config = dict \
# (
# protocol = "file",                       # meta
# request_timeout = None,                  # meta, optional
# source_directory = "/tmp",               # file
# filename_regex = "[A-Za-z0-9_]+\\.msg",  # file
# interval = 10.0,                         # file
# )
#
# Sample processing module (interface_file_1.py):
#
# def process_request(request, response):
#   file_name = request["file_name"]
#   with open(file_name, "rb") as f:
#     data = f.read()
#
# Sample file resource configuration (config_resource_file_1.py)
#
# config = dict \
# (
# protocol = "file",              # meta
# target_directory = "/tmp",      # file, optional directory name
# temp_directory = None,          # file, optional directory name for temporary files
# temp_extension = "tmp",         # file
# file_permissions = "rw-rw----", # file (permissions to set on saved files)
# )
#
# Sample resource usage (anywhere):
#
# xa = pmnc.transaction.create()
# xa.file_1.write("foo.msg", b"data")
# assert xa.execute()[0] == "/tmp/foo.msg"
#
# xa = pmnc.transaction.create()
# xa.file_1.write("supports/subdirector.ies", b"too"[, overwrite = True])
# assert xa.execute()[0] == "/tmp/supports/subdirector.ies" # subdirectories are created
#
# or if the only transaction participant:
#
# filename = pmnc.transaction.file_1.write(...)
#
# Reading files using a resource is done like this:
#
# xa = pmnc.transaction.create()
# xa.file_1.read("supports/subdirector.ies"[, remove = True]) # the file can be removed after reading
# assert xa.execute()[0] == b"data" returns bytes
#
# or if the only transaction participant:
#
# data = pmnc.transaction.file_1.read("filename") # returns bytes
#
# Reading files through a resource has two benefits compared
# to a regular file operations. First, it respects request deadline
# and will properly time out. Second, if you configure the resource
# pool with caching, you get configurable file cache easily.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface", "Resource" ]

###############################################################################

import os; from os import path as os_path, fdopen, remove, rename, \
                          makedirs, fsync, chmod, stat as os_stat
import stat; from stat import S_IRUSR, S_IWUSR, S_IXUSR, S_IRGRP, \
             S_IWGRP, S_IXGRP, S_IROTH, S_IWOTH, S_IXOTH
import tempfile; from tempfile import mkstemp
import threading; from threading import current_thread
import time; from time import time, sleep
import io; from io import BytesIO

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, typecheck_with_exceptions, \
                                        optional, by_regex, nothing
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, \
                                ResourceError, ResourceInputParameterError
import pmnc.threads; from pmnc.threads import HeavyThread
import pmnc.thread_pool; from pmnc.thread_pool import WorkUnitTimedOut

###############################################################################

def _retry_remove_file(fn, t):
    if os_path.isfile(fn):
        try:
            chmod(fn, os_stat(fn).st_mode | S_IWUSR) # in case a temporary file has been
            remove(fn)                               # saved read-only and is being removed
        except:
            sleep(t)   # this second attempt compensates for
            remove(fn) # sporadic errors in a loaded filesystem

###############################################################################

class Interface: # file-loading interface

    @typecheck
    def __init__(self, name: str, *,
                 source_directory: os_path.isdir,
                 filename_regex: str,
                 interval: float,
                 request_timeout: optional(float) = None,
                 **kwargs): # this kwargs allows for extra application-specific
                            # settings in config_interface_file_X.py

        self._name = name
        self._source_directory = source_directory
        self._valid_filename = by_regex("^{0:s}$".format(filename_regex))
        self._interval = interval

        self._request_timeout = request_timeout or \
            pmnc.config_interfaces.get("request_timeout") # this is now static

        # this set contains names of files that have been processed
        # but not deleted due to filesystem deletion failure

        self._processed_files = set()

        if pmnc.request.self_test == __name__: # self-test
            self._process_request = kwargs["process_request"]

    name = property(lambda self: self._name)

    ###################################

    def start(self):
        self._spooler = HeavyThread(target = self._spooler_proc,
                                    name = "{0:s}:spool".format(self._name))
        self._spooler.start()

    def cease(self):
        self._spooler.stop()

    def stop(self):
        pass

    ###################################

    # this thread periodically scans the configured directory for files

    def _spooler_proc(self):

        interval = self._interval

        while not current_thread().stopped(interval):
            try:

                # enumerate matching files in the source directory

                file_names = [ os_path.join(self._source_directory, file_name)
                               for file_name in os.listdir(self._source_directory)
                               if self._valid_filename(file_name) and
                                  os_path.isfile(os_path.join(self._source_directory, file_name)) ]

                if not file_names:
                    interval = self._interval # no files, rescan after a delay
                    continue

                # sort and process the file names in alphabetical order

                file_names.sort()
                for file_name in file_names:
                    if current_thread().stopped(): # processing of all files may take a long time
                        break # for
                    elif file_name in self._processed_files: # just remove the already processed file
                        self._remove_file(file_name)
                    elif self._process_file(file_name): # remove the file if it has been processed
                        self._remove_file(file_name)
                    else:
                        interval = self._interval # processing of some file failed,
                        break # for               # rescan after a delay
                else:              # all the files have been processed, rescan immediately,
                    interval = 0.0 # hoping for the new files to appear in the meantime

            except:
                pmnc.log.error(exc_string())
                interval = self._interval

    ###################################

    def _remove_file(self, file_name):
        try:
            _retry_remove_file(file_name, 2.0)
        except:
            pmnc.log.warning(exc_string())
        else:
            self._processed_files.remove(file_name)

    ###################################

    def _process_file(self, file_name):

        request = pmnc.interfaces.begin_request(
                    timeout = self._request_timeout,
                    interface = self._name, protocol = "file",
                    parameters = dict(auth_tokens = dict()),
                    description = "file {0:s}".format(file_name))

        # enqueue the request and wait for its completion

        try:
            pmnc.interfaces.enqueue(request, self.wu_process_request,
                                    (file_name, )).wait()
        except WorkUnitTimedOut:
            pmnc.log.error("file processing timed out")
            success = None
        except:
            pmnc.log.error("file processing failed: {0:s}".format(exc_string()))
            success = False
        else:
            if pmnc.log.debug:
                pmnc.log.debug("file processing succeeded")
            self._processed_files.add(file_name)
            success = True
        finally:
            pmnc.interfaces.end_request(success, request)
            return success == True

    ###################################

    # this method is a work unit executed by one of the interface pool threads
    # if this method fails, the exception is rethrown in _process_file in wait()

    @typecheck
    def wu_process_request(self, file_name: str):

        # see for how long the request was on the execution queue up to this moment
        # and whether it has expired in the meantime, if it did there is no reason
        # to proceed and we simply bail out

        if pmnc.request.expired:
            pmnc.log.error("request has expired and will not be processed")
            return

        try:
            with pmnc.performance.request_processing():
                request = dict(file_name = os_path.join(self._source_directory, file_name))
                self._process_request(request, {})
        except:
            pmnc.log.error(exc_string()) # don't allow an exception to be silenced
            raise                        # when this work unit is not waited upon

    ###################################

    def _process_request(self, request, response):
        handler_module_name = "interface_{0:s}".format(self._name)
        pmnc.__getattr__(handler_module_name).process_request(request, response)

###############################################################################

class Resource(TransactionalResource): # file-saving resource

    @typecheck
    def __init__(self, name: str, *,
                 target_directory: optional(str) = None,
                 temp_directory: optional(str) = None,
                 temp_extension: str,
                 file_permissions: by_regex("^(?:[r-][w-][x-]){3}$")):

        TransactionalResource.__init__(self, name)

        self._target_directory = target_directory
        self._temp_directory = temp_directory
        self._temp_suffix = ".{0:s}".format(temp_extension)

        self._perm_mask = 0
        for i, perm_flag in enumerate((S_IRUSR, S_IWUSR, S_IXUSR, S_IRGRP, S_IWGRP,
                                       S_IXGRP, S_IROTH, S_IWOTH, S_IXOTH)):
            if file_permissions[i] != "-":
                self._perm_mask |= perm_flag

    ###################################

    def begin_transaction(self, *args, **kwargs):
        TransactionalResource.begin_transaction(self, *args, **kwargs)
        self._temp_filename = None
        self._overwrite = self._remove = None

    ###################################

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def write(self, target_filename: str, data_b: bytes, *, overwrite: optional(bool) = True):

        try:

            self._overwrite = overwrite or False
            self._target_filename, target_directory = self._normalize_filename(target_filename)

            # determine the location of temporary files
            # and create the temporary directory if necessary

            temp_directory = os_path.normpath(self._temp_directory or target_directory)
            if not os_path.isdir(temp_directory):
                self._create_directory(temp_directory)

            # write data to a temporary file and make sure
            # it has been persistently stored to disk

            h, self._temp_filename = mkstemp(dir = temp_directory,
                                             suffix = self._temp_suffix)

            pmnc.log.info("writing {0:d} byte(s) to a temporary file {1:s}".\
                          format(len(data_b), self._temp_filename))
            try:
                with fdopen(h, "wb") as f:
                    f.write(data_b)
                    f.flush()
                    fsync(h)
            except:
                pmnc.log.warning("writing {0:d} byte(s) to a temporary file {1:s} failed: " \
                                 "{2:s}".format(len(data_b), self._temp_filename, exc_string()))
                raise

            # explicitly set permissions on the temporary file

            chmod(self._temp_filename, self._perm_mask)

            # this is a pessimistic safety check, if the target file already exists,
            # we are most likely unable to commit the transaction anyway, therefore
            # fail early to cause rollback

            if os_path.exists(self._target_filename) and not self._overwrite:
                raise Exception("file {0:s} already exists".format(self._target_filename))

        except:
            ResourceError.rethrow(recoverable = True) # no irreversible changes

    ###################################

    @typecheck_with_exceptions(input_parameter_error = ResourceInputParameterError)
    def read(self, target_filename: str, *, remove: optional(bool) = None, frag_size: optional(int) = None) -> bytes:

        try:

            self._remove = remove or False
            self._target_filename, _ = self._normalize_filename(target_filename)

            file_size = os_path.getsize(self._target_filename)
            frag_size = frag_size or 65536

            pmnc.log.info("reading {0:d} byte(s) from file {1:s}".\
                          format(file_size, self._target_filename))
            try:

                data = BytesIO()

                with open(self._target_filename, "rb") as f:
                    while not pmnc.request.expired:
                        portion = f.read(frag_size)
                        if not portion:
                            break
                        data.write(portion)
                    else:
                        raise Exception("request deadline reading data from file {0:s}".\
                                        format(self._target_filename))

            except:
                pmnc.log.info("reading {0:d} byte(s) from file {1:s} failed: {2:s}".\
                              format(file_size, self._target_filename, exc_string()))
                raise
            else:
                return data.getvalue()

        except:
            ResourceError.rethrow(recoverable = True) # no irreversible changes

    ###################################

    # commit fails if the target file exists but could not or should not be removed

    def commit(self):

        if self._overwrite is not None: # after write

            if os_path.exists(self._target_filename): # the file could have appeared in the meantime
                if not self._overwrite:
                    raise Exception("file {0:s} already exists".format(self._target_filename))
                _retry_remove_file(self._target_filename, min(pmnc.request.remain, 2.0))
            rename(self._temp_filename, self._target_filename)

            pmnc.log.info("temporary file has been renamed into {0:s}".\
                          format(self._target_filename))

        elif self._remove is not None: # after read

            if os_path.exists(self._target_filename) and self._remove: # the file could have disappeared in the meantime
                _retry_remove_file(self._target_filename, min(pmnc.request.remain, 2.0))

            pmnc.log.info("file {0:s} has been removed".\
                          format(self._target_filename))

    ###################################

    def rollback(self):
        if self._temp_filename and os_path.isfile(self._temp_filename):
            _retry_remove_file(self._temp_filename, min(pmnc.request.remain, 2.0))
            pmnc.log.info("temporary file has been removed")

    ###################################

    # this utility method ensures the directory exists,
    # directory creation attempt is exclusively interlocked

    def _create_directory(self, directory):
        makedirs_lock = pmnc.shared_locks.get("{0:s}.Resource._create_directory".\
                                              format(__name__))
        pmnc.request.acquire(makedirs_lock)
        try:
            if not os_path.isdir(directory):
                makedirs(directory)
        finally:
            makedirs_lock.release()

    ###################################

    def _normalize_filename(self, target_filename: str):

        # determine the location of the target file
        # and create the target directory if necessary

        if self._target_directory:
            target_filename = os_path.join(self._target_directory, target_filename)
        target_filename = os_path.normpath(target_filename)

        target_directory = os_path.dirname(target_filename)
        if not os_path.isdir(target_directory):
            self._create_directory(target_directory)

        return target_filename, target_directory

###############################################################################

def self_test():

    from sys import platform
    from os import urandom, rmdir
    from stat import S_IRWXU, S_IRWXG, S_IRWXO
    from binascii import b2a_hex
    from expected import expected
    from typecheck import InputParameterError
    from interlocked_queue import InterlockedQueue
    from random import shuffle
    from pmnc.request import fake_request
    from pmnc.self_test import active_interface
    from pmnc.resource_pool import TransactionCommitError, TransactionExecutionError

    ###################################

    test_interface_config = dict \
    (
    protocol = "file",
    source_directory = "/tmp",
    filename_regex = "[A-Za-z0-9_]+\\.msg",
    interval = 2.0,
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    def random_filename():
        return b2a_hex(urandom(4)).decode("ascii")

    write_prefix = random_filename()

    def write_file(filename, data):
        filename = os_path.join(pmnc.config_interface_file_1.get("source_directory"),
                                write_prefix + filename)
        with open(filename, "wb") as f:
            f.write(data)
        return filename

    ###################################

    def test_interface_start_stop():

        def process_request(request, response):
            pass

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg")):
            sleep(3.0)

    test_interface_start_stop()

    ###################################

    def test_interface_success():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request)

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg")):
            assert loopback_queue.pop(3.0) is None
            file_name = write_file(random_filename() + ".msg", b"request")
            assert os_path.isfile(file_name)
            assert loopback_queue.pop(3.0) == dict(file_name = file_name)
            sleep(3.0)
            assert not os_path.exists(file_name)
            assert loopback_queue.pop(3.0) is None

    test_interface_success()

    ###################################

    def test_interface_skip():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request)

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg")):

            assert loopback_queue.pop(3.0) is None
            file_name = write_file(random_filename() + ".tmp", b"data")
            assert os_path.isfile(file_name)
            assert loopback_queue.pop(3.0) is None
            assert os_path.isfile(file_name)
            remove(file_name)

    test_interface_skip()

    ###################################

    def test_interface_failure():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            not_defined

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg")):

            assert loopback_queue.pop(3.0) is None
            file_name = write_file(random_filename() + ".msg", b"data")
            assert os_path.isfile(file_name)
            assert loopback_queue.pop(3.0) is None
            assert os_path.isfile(file_name)
            remove(file_name)

    test_interface_failure()

    ###################################

    def test_interface_timeout():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            sleep(4.0)
            loopback_queue.push(request)

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg", request_timeout = 3.0)):

            assert loopback_queue.pop(3.0) is None
            file_name = write_file(random_filename() + ".msg", b"data")
            assert os_path.isfile(file_name)
            assert loopback_queue.pop(3.0) is None
            assert os_path.isfile(file_name)
            assert loopback_queue.pop(4.0) == dict(file_name = file_name)
            assert os_path.isfile(file_name)
            remove(file_name)

    test_interface_timeout()

    ###################################

    def test_interface_ordering():

        file_names = [ "{0:08d}.msg".format(i) for i in range(100) ]
        shuffle(file_names)

        for file_name in file_names:
            write_file(file_name, b"data")

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            sleep(0.1)
            loopback_queue.push(request)

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg")):

            for i in range(100):
                file_name = loopback_queue.pop(3.0)["file_name"]
                assert os_path.basename(file_name).endswith("{0:08d}.msg".format(i))
                if i % 10 == 9:
                    write_file("{0:08d}.msg".format(i // 10), b"data")

            for i in range(10):
                file_name = loopback_queue.pop(3.0)["file_name"]
                assert os_path.basename(file_name).endswith("{0:08d}.msg".format(i))

            assert loopback_queue.pop(3.0) is None

    test_interface_ordering()

    ###################################

    def test_interface_remove():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            remove(request["file_name"])
            loopback_queue.push(request)

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg")):

            file_name = write_file(random_filename() + ".msg", b"data")
            assert loopback_queue.pop(3.0) == dict(file_name = file_name)
            assert loopback_queue.pop(3.0) is None

            assert not os_path.exists(file_name)

    test_interface_remove()

    ###################################

    def test_deletion_failure():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            loopback_queue.push(request)

        with active_interface("file_1", **interface_config(process_request = process_request,
                              filename_regex = write_prefix + "[0-9a-f]{8}\\.msg")) as ifc:

            ifc._remove_file_ = ifc._remove_file
            ifc._remove_file = lambda file_name: 1 / 0

            file_name = write_file(random_filename() + ".msg", b"data")
            assert loopback_queue.pop(3.0) == dict(file_name = file_name)
            assert loopback_queue.pop(3.0) is None
            assert os_path.isfile(file_name)
            assert ifc._processed_files == { file_name }

            ifc._remove_file = ifc._remove_file_
            assert loopback_queue.pop(3.0) is None
            assert not os_path.exists(file_name)
            assert ifc._processed_files == set()

    test_deletion_failure()

    ###################################

    def target_filename(s):
        target_directory = pmnc.config_resource_file_1.get("target_directory")
        return os_path.normpath(os_path.join(target_directory, s))

    ###################################

    def test_resource_success():

        # also check the directories creation

        dn = random_filename()
        assert not os_path.isdir(target_filename(dn))
        fn = random_filename()
        dfn = os_path.join(dn, fn)

        fake_request(3.0)

        pmnc.transaction.file_1.write(dfn, b"\x00")

        with open(target_filename(dfn), "rb") as f:
            assert f.read() == b"\x00"

        # check file permissions

        file_mode = os_stat(target_filename(dfn)).st_mode

        if platform == "win32":
            assert file_mode & S_IRUSR != 0
            assert file_mode & S_IWUSR == 0
        else:
            assert file_mode & (S_IRWXU | S_IRWXG | S_IRWXO) == \
                   S_IRUSR | S_IROTH | S_IWOTH | S_IXOTH

        chmod(target_filename(dfn), file_mode | S_IWUSR)
        remove(target_filename(dfn))

        assert os_path.isdir(target_filename(dn))
        rmdir(target_filename(dn))

    test_resource_success()

    ###################################

    def test_resource_failure():

        # an attempt to write non-bytes fails

        fn = random_filename()
        assert not os_path.isfile(target_filename(fn))

        fake_request(3.0)

        with expected(ResourceInputParameterError):
            pmnc.transaction.file_1.write(fn, "should have been bytes")

        assert not os_path.isfile(target_filename(fn))

        # bytes should have been written

        fn = random_filename()
        assert not os_path.isfile(target_filename(fn))

        fake_request(3.0)

        pmnc.transaction.file_1.write(fn, b"bytes should be ok")

        assert os_path.isfile(target_filename(fn))

        # an attempt to overwrite an existing file fails if no overwrite

        fake_request(3.0)

        with expected(ResourceError, "^file .*[\\\\/]{0:s} already exists$".format(fn)):
            pmnc.transaction.file_1.write(fn, b"should fail this time", overwrite = False)

        assert os_path.isfile(target_filename(fn)) # the file still exists

        # but with overwrite it succeeds

        fake_request(3.0)

        pmnc.transaction.file_1.write(fn, b"different bytes", overwrite = True) # this is also a default

        assert os_path.isfile(target_filename(fn)) # the file still exists
        with open(target_filename(fn), "rb") as f:
            assert f.read() == b"different bytes" # but contains new data

        # deadlock causes rollback

        fn = random_filename()
        assert not os_path.isfile(target_filename(fn))

        fake_request(3.0)

        xa = pmnc.transaction.create()
        xa.file_1.write(fn, b"data")
        xa.state.set("foo", "bar1")
        xa.state.set("foo", "bar2")
        try:
            xa.execute()
        except (ResourceError, TransactionExecutionError): # depends on who times out first
            pass
        else:
            assert False
        assert not os_path.isfile(target_filename(fn))

    test_resource_failure()

    ###################################

    def test_resource_read():

        fn = random_filename()

        fake_request(3.0)

        pmnc.transaction.file_1.write(fn, b"foobar")

        assert pmnc.transaction.file_1.read(fn) == b"foobar"
        assert os_path.isfile(target_filename(fn))

        assert pmnc.transaction.file_1.read(fn, remove = True, frag_size = 4) == b"foobar"
        assert not os_path.isfile(target_filename(fn))

        with expected(ResourceError, "^.*never_existed.*$"):
            pmnc.transaction.file_1.read("never_existed")

        pmnc.transaction.file_1.write(fn, b"\x00" * 33554432)
        fake_request(0.1)
        with expected(Exception, "^request deadline.*$"):
            pmnc.transaction.file_1.read(fn, frag_size = 1)

    test_resource_read()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
