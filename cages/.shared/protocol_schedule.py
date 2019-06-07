#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# This module contains an implementation of schedule interface (periodic
# request execution at specified times).
#
# Note that this interface does not wait for the requests it initiates.
#
# Sample schedule interface configuration (config_interface_schedule_1.py):
#
# config = dict \
# (
# protocol = "schedule",     # meta
# request_timeout = None,    # meta, optional
# format = "%H:%M",          # schedule (argument to strftime)
# match = "12:30",           # schedule (regular expression to match)
# )
#
# Sample processing module (interface_schedule_1.py):
#
# def process_request(request, response):
#     invocation_time = request["invocation_time"]
#     pmnc.log("invoked at {0:s}".format(invocation_time.strftime("%H:%M")))
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Interface" ]

###############################################################################

import threading; from threading import current_thread
import datetime; from datetime import datetime
import time; from time import time, mktime
import math; from math import modf

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, optional, by_regex
import pmnc.threads; from pmnc.threads import HeavyThread

###############################################################################

class Interface: # schedule interface

    @typecheck
    def __init__(self, name: str, *,
                 format: str,
                 match: str,
                 request_timeout: optional(float) = None,
                 **kwargs): # this kwargs allows for extra application-specific
                            # settings in config_interface_schedule_X.py

        self._name = name
        self._format = format
        self._match = by_regex("^{0:s}$".format(match))

        self._request_timeout = request_timeout or \
            pmnc.config_interfaces.get("request_timeout") # this is now static

        self._last_tick = Interface._get_current_tick()

        if pmnc.request.self_test == __name__: # self-test
            self._process_request = kwargs["process_request"]

    name = property(lambda self: self._name)

    ###################################

    def start(self):
        self._scheduler = HeavyThread(target = self._scheduler_proc,
                                      name = "{0:s}:sch".format(self._name))
        self._scheduler.start()

    def cease(self):
        self._scheduler.stop()

    def stop(self):
        pass

    ###################################

    # this thread keeps track of time and initiates requests as appropriate

    def _scheduler_proc(self):

        while not current_thread().stopped(1.1 - modf(time())[0]):
            try:

                current_tick = Interface._get_current_tick()
                try:
                    for tick in range(self._last_tick + 1, current_tick + 1):
                        invocation_time = datetime.fromtimestamp(tick)
                        if self._match(invocation_time.strftime(self._format)):
                            try:
                                self._fire_request(invocation_time)
                            except:
                                pmnc.log.error(exc_string()) # log and ignore
                finally:
                    self._last_tick = tick

            except:
                pmnc.log.error(exc_string()) # log and ignore

    ###################################

    def _fire_request(self, invocation_time):

        request = pmnc.interfaces.begin_request(
                    timeout = self._request_timeout,
                    interface = self._name, protocol = "schedule",
                    parameters = dict(auth_tokens = dict()),
                    description = invocation_time.strftime("at %Y-%m-%d %H:%M:%S"))

        # note that this interface does not wait for its requests to complete

        pmnc.interfaces.enqueue(request, self.wu_process_request, (invocation_time, ), {})

    ###################################

    @typecheck
    def wu_process_request(self, invocation_time: datetime):

        try:

            # see for how long the request was on the execution queue up to this moment
            # and whether it has expired in the meantime, if it did there is no reason
            # to proceed and we simply bail out

            if pmnc.request.expired:
                pmnc.log.error("request has expired and will not be processed")
                success = False
                return # goes through finally section below

            with pmnc.performance.request_processing():
                request = dict(invocation_time = invocation_time)
                self._process_request(request, {})

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

    @staticmethod
    def _get_current_tick():
        return int(mktime(datetime.now().timetuple()))

###############################################################################

def self_test():

    from time import sleep
    from expected import expected
    from interlocked_queue import InterlockedQueue
    from pmnc.self_test import active_interface
    from pmnc.request import fake_request

    ###################################

    test_interface_config = dict \
    (
    protocol = "schedule",
    format = "%S",
    match = "00|03|06|09|12|15|18|21|24|27|30|33|36|39|42|45|48|51|54|57", # every 3 seconds
    )

    def interface_config(**kwargs):
        result = test_interface_config.copy()
        result.update(kwargs)
        return result

    ###################################

    def test_interface_success():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            sleep(0.1)
            loopback_queue.push("ok")

        with active_interface("schedule_1", **interface_config(process_request = process_request)):
            assert loopback_queue.pop(4.0) == "ok"
            assert loopback_queue.pop(1.0) is None
            assert loopback_queue.pop(3.0) == "ok"
            assert loopback_queue.pop(1.0) is None
            assert loopback_queue.pop(3.0) == "ok"

    test_interface_success()

    ###################################

    def test_interface_failure():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            not_defined

        with active_interface("schedule_1", **interface_config(process_request = process_request)):
            assert loopback_queue.pop(4.0) is None

    test_interface_failure()

    ###################################

    def test_interface_no_wait():

        loopback_queue = InterlockedQueue()

        def process_request(request, response):
            sleep(5.0)
            loopback_queue.push(request["invocation_time"])

        with active_interface("schedule_1", **interface_config(process_request = process_request)):
            dt1 = loopback_queue.pop(9.0)
            dt2 = loopback_queue.pop(6.0)
            dt3 = loopback_queue.pop(6.0)

        assert (dt3 - dt2).seconds == (dt2 - dt1).seconds == 3

    test_interface_no_wait()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
