#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# ModuleLocator helper class implements the lookup of module files within a cage.
# ModuleLoader uses a single global instance of ModuleLocator per cage.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "ModuleLocator" ]

###############################################################################

import os; from os import path as os_path, listdir
import threading; from threading import Lock

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, by_regex
import pmnc.timeout; from pmnc.timeout import Timeout

###############################################################################

class ModuleLocator:

    @typecheck
    def __init__(self, cage_directory: os_path.isdir, cache_timeout: float, settle_timeout: float):
        self._cage_directory = os_path.normpath(cage_directory)
        shared_directory = os_path.normpath(os_path.join(cage_directory, "..", ".shared"))
        self._shared_directory = os_path.isdir(shared_directory) and shared_directory or None
        self._timeout = Timeout(cache_timeout)
        self._settle_timeout_sec = settle_timeout
        self._modules = self._settle_modules = self._settle_timeout = None
        self._lock = Lock()

    ###################################

    @staticmethod
    def _listdir(s):
        try:
            return listdir(s)
        except:
            return []

    ###################################

    def _read_modules(self):
        modules = { module_name: os_path.join(self._shared_directory, module_name)
                    for module_name in self._listdir(self._shared_directory) }
        modules.update({ module_name: os_path.join(self._cage_directory, module_name)
                         for module_name in self._listdir(self._cage_directory) })
        return modules

    ###################################

    def _get_modules(self):

        with self._lock:

            if self._modules is None: # initial state, the current directories contents is unknown

                modules = self._read_modules()
                self._modules = modules

            elif self._settle_timeout: # a change has been detected previously and is currently being settled

                if self._settle_timeout.expired:
                    modules = self._read_modules()
                    if modules != self._settle_modules: # another change occured since last time, keep settling
                        self._settle_timeout.reset()
                        self._settle_modules = modules
                    else: # directories contents seems to have settled
                        self._timeout.reset()
                        self._modules = modules
                        self._settle_modules = self._settle_timeout = None

            elif self._timeout.expired: # cached contents is refreshed

                self._timeout.reset()
                modules = self._read_modules()
                if modules != self._modules: # change detected, switch to settling
                    if self._settle_timeout_sec > 0.0:
                        self._settle_modules = modules
                        self._settle_timeout = Timeout(self._settle_timeout_sec)
                    else:
                        self._modules = modules

            return self._modules

    ###################################

    @typecheck
    def locate(self, module_name: by_regex("^[A-Za-z0-9_-]{1,128}\\.pyc?$")):
        return self._get_modules().get(module_name)

###############################################################################

if __name__ == "__main__":

    print("self-testing module module_locator.py: ")

    ###################################

    from tempfile import mkdtemp
    from os import mkdir, remove
    from shutil import rmtree
    from time import sleep

    from expected import expected
    from typecheck import InputParameterError

    ###################################

    cages_directory = mkdtemp()

    shared_directory = os_path.join(cages_directory, ".shared")
    mkdir(shared_directory)

    cage_directory = os_path.join(cages_directory, "test")
    mkdir(cage_directory)

    ###################################

    def create_shared_file(fn):
        fn = os_path.join(shared_directory, fn)
        open(fn, "w").close()
        return fn

    def create_cage_file(fn):
        fn = os_path.join(cage_directory, fn)
        open(fn, "w").close()
        return fn

    def remove_cage_file(fn):
        remove(os_path.join(cage_directory, fn))

    ###################################

    ml = ModuleLocator(cage_directory, 0.1, 0.5)

    with expected(InputParameterError):
        ModuleLocator(cage_directory + "notthere", 0.1, 0.5)

    ###################################

    with expected(InputParameterError):
        ml.locate("foo.txt")

    with expected(InputParameterError):
        ml.locate("foo..py")

    sfoo = create_shared_file("foo.py")
    sbar = create_shared_file("bar.py")
    cfoo = create_cage_file("foo.py")

    # files that initially existed

    assert ml.locate("bar.py") == sbar
    assert ml.locate("foo.py") == cfoo

    # a new file appears in a cage directory

    cbar = create_cage_file("bar.py")
    assert ml.locate("bar.py") == sbar
    sleep(0.2)
    assert ml.locate("bar.py") == sbar
    sleep(0.4)
    assert ml.locate("bar.py") == sbar
    sleep(0.2)
    assert ml.locate("bar.py") == cbar

    # an existing file is removed from a cage directory

    assert ml.locate("foo.py") == cfoo
    remove_cage_file("foo.py")
    assert ml.locate("foo.py") == cfoo
    sleep(0.2)
    assert ml.locate("foo.py") == cfoo
    sleep(0.4)
    assert ml.locate("foo.py") == cfoo
    sleep(0.2)
    assert ml.locate("foo.py") == sfoo

    # several files (dis)appear in a cage directory

    assert ml.locate("biz1.py") is None
    cbiz1 = create_cage_file("biz1.py")
    assert ml.locate("biz1.py") is None
    sleep(0.2)
    assert ml.locate("biz1.py") is None
    sleep(0.2)
    assert ml.locate("biz1.py") is None
    create_cage_file("biz2.py")
    sleep(0.2)
    assert ml.locate("biz1.py") is None
    cbiz3 = create_cage_file("biz3.py")
    sleep(0.2)
    assert ml.locate("biz1.py") is None
    remove_cage_file("biz2.py")
    sleep(0.2)
    assert ml.locate("biz1.py") is None
    sleep(0.2)
    assert ml.locate("biz1.py") is None
    sleep(0.2)
    assert ml.locate("biz1.py") is None
    sleep(0.6)
    assert ml.locate("biz1.py") == cbiz1
    assert ml.locate("biz2.py") is None
    assert ml.locate("biz3.py") == cbiz3

    # a file is removed entirely

    remove_cage_file("biz3.py")
    assert ml.locate("biz3.py") == cbiz3
    sleep(0.2)
    assert ml.locate("biz3.py") == cbiz3
    sleep(0.5)
    assert ml.locate("biz3.py") is None

    ###################################

    rmtree(cages_directory)

    print("ok")

###############################################################################
# EOF
