#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module manages all the machinery required for the interfaces to work,
# most notably it contains the main request processing thread pool.
#
# All activity on starting/stopping interfaces is presumably performed
# by the single "cage" thread, which is also responsible for restarting
# interfaces in case their configuration has changed. Therefore there is
# no interlocking in start/stop methods.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "start", "stop", "reload", "begin_request", "end_request", "enqueue",
            "get_interface", "set_fake_interface", "delete_fake_interface",
            "get_activity_stats" ]
__reloadable__ = False

###############################################################################

import threading; from threading import current_thread

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import typecheck, callable, optional, tuple_of, with_attr
import exc_string; from exc_string import exc_string
import interlocked_factory; from interlocked_factory import InterlockedFactory
import pmnc.resource_pool; from pmnc.resource_pool import RegisteredResourcePool
import pmnc.request; from pmnc.request import Request
import pmnc.samplers; from pmnc.samplers import RateSampler

###############################################################################

# module-level state => not reloadable

_interfaces = {}
_request_factory = None
_health_monitor = None
_request_rate_sampler = RateSampler(10.0)

###############################################################################

class AbstractLoadedInterface: # mere placeholder

    def __init__(self, interface):
        self._interface = interface

    interface = property(lambda self: self._interface)

###############################################################################

class LoadedInterface(AbstractLoadedInterface):

    def __init__(self, name):
        self._name = name
        config, self._version = self._config()
        AbstractLoadedInterface.__init__(self, self._create(config) if config else None)

    ###################################

    def _config(self):
        module_properties = {}
        config_module_name = "config_interface_{0:s}".format(self._name)
        try:
            config = pmnc.__getattr__(config_module_name).\
                          copy(__module_properties = module_properties)
        except:
            pmnc.log.error(exc_string())
            return None, None
        else:
            return config, module_properties["version"]

    ###################################

    def _create(self, config): # does not throw
        try:
            return pmnc.interface.create(self._name, **config)
        except:
            pmnc.log.error("interface {0:s} could not be created: {1:s}".\
                           format(self._name, exc_string())) # log and return None

    ###################################

    def start(self): # does not throw

        if not self._interface: # ignore
            return

        pmnc.log.message("starting interface {0:s}".format(self._name))
        try:
            self._interface.start()
        except:
            pmnc.log.error("interface {0:s} failed to start: {1:s}".\
                           format(self._name, exc_string())) # log and ignore
        else:
            pmnc.log.message("interface {0:s} has started".format(self._name))

    ###################################

    def cease(self): # does not throw

        if not self._interface: # ignore
            return

        pmnc.log.message("ceasing interface {0:s}".format(self._name))
        try:
            self._interface.cease()
        except:
            pmnc.log.error("interface {0:s} failed to cease: {1:s}".\
                           format(self._name, exc_string())) # log and ignore
        else:
            pmnc.log.message("interface {0:s} has ceased".format(self._name))

    ###################################

    def stop(self): # does not throw

        if not self._interface: # ignore
            return

        pmnc.log.message("stopping interface {0:s}".format(self._name))
        try:
            self._interface.stop()
        except:
            pmnc.log.error("interface {0:s} failed to stop: {1:s}".\
                           format(self._name, exc_string())) # log and ignore
        else:
            pmnc.log.message("interface {0:s} has stopped".format(self._name))
        finally:
            self._interface = None

    ###################################

    def reload(self): # does not throw

        config, version = self._config()

        if config is None: # current configuration is broken, do nothing
            return

        if self._version is not None and version <= self._version: # config module is the same, do nothing
            return

        pmnc.log.message("restarting interface {0:s} due to "
                         "configuration change".format(self._name))

        self.cease()
        self.stop()

        # now a new interface instance will be active

        self._interface = self._create(config)
        self.start()

        self._version = version

###############################################################################

@typecheck
def _start_thread_pools(thread_count: int, sweep_period: float):

    # initialize the entire thread pool machinery

    RegisteredResourcePool.start_pools(sweep_period)

###############################################################################

def _stop_thread_pools():

    # clean up and stop all the pools

    RegisteredResourcePool.stop_pools()

###############################################################################

def _start_interfaces(interface_names):

    # prepare the global request factory shared across all interfaces
    # see begin_request method below

    global _request_factory
    _request_factory = InterlockedFactory(Request)

    pmnc.log.message("as of this moment requests can be created")

    # the configured interfaces are started one by one and kept on file,
    # restarted automatically if the configuration files are changed

    _update_interfaces(interface_names)

###############################################################################

@typecheck
def _update_interfaces(interface_names: tuple_of(str)):

    started_interfaces = set(_interfaces.keys()) # currently running interfaces

    for interface_name in interface_names:
        if interface_name not in started_interfaces: # start a newly mentioned interface
            interface = LoadedInterface(interface_name)
            interface.start()
            _interfaces[interface_name] = interface
        else:
            started_interfaces.remove(interface_name) # ignore the already running interface

    # stop the interfaces that are no longer mentioned

    for interface_name in started_interfaces:
        interface = _interfaces.pop(interface_name)
        interface.cease()
        interface.stop()

###############################################################################

def _stop_interfaces():

    # first, we make sure the interfaces cease to accept new
    # incoming requests but may keep processing already accepted

    for interface in _interfaces.values():
        interface.cease()

    # then we stop the request factory to stress the fact
    # that no more requests can be created any more

    _request_factory.stop()

    request_count = _request_factory.count
    pmnc.log.message("no more requests can be created{0:s}".\
                     format(request_count > 0
                            and ", {0:d} request(s) are still active".\
                                format(request_count)
                            or ""))

    # then we wait for all existing requests to complete for
    # at most one full timeout, after which they are dead anyway

    request_timeout = pmnc.config.get("request_timeout")
    if not _request_factory.wait(request_timeout):
        request_count = _request_factory.count
        if request_count > 0:
            pmnc.log.warning("proceeding with shutdown although {0:d} request(s) "
                             "are still active".format(request_count))

    # finally the interfaces are stopped completely
    # and no further processing is performed

    for interface in _interfaces.values():
        interface.stop()

    _interfaces.clear()

###############################################################################

def _start_health_monitor():

    # if this cage is called health_monitor, start health monitor

    if __cage__ == "health_monitor":
        pmnc.log.message("starting health monitor")
        try:
            health_monitor = pmnc.health_monitor.HealthMonitor()
            health_monitor.start()
        except:
            pmnc.log.error("health monitor failed to start: {0:s}".\
                           format(exc_string()))
        else:
            global _health_monitor
            _health_monitor = health_monitor
            pmnc.log.message("health monitor has started")

###############################################################################

def _stop_health_monitor():

    # stop health monitor if it has been started

    if _health_monitor:
        pmnc.log.message("stopping health monitor")
        try:
            _health_monitor.stop()
        except:
            pmnc.log.error("health monitor failed to stop: {0:s}".\
                           format(exc_string()))
        else:
            pmnc.log.message("health monitor has stopped")

###############################################################################

def start():

    # thread pools are started before anything else

    thread_count = pmnc.config.get("thread_count")
    sweep_period = pmnc.config.get("sweep_period")

    _start_thread_pools(thread_count, sweep_period)

    # now the interfaces can be started as they have worker threads to delegate requests to

    interface_names = ()
    if not pmnc.request.self_test:
        try:
            interface_names = pmnc.config.get("interfaces")
        except:
            pmnc.log.error(exc_string()) # log and ignore, empty tuple remains

    try:
        _start_interfaces(interface_names)
    except:
        pmnc.log.error(exc_string()) # log and ignore

    # finally, if we are the health monitor

    _start_health_monitor()

###############################################################################

def stop():

    _stop_health_monitor()
    _stop_interfaces()
    _stop_thread_pools()

###############################################################################

def reload():

    # start new/stop missing interfaces

    try:
        interface_names = pmnc.config.get("interfaces")
    except:
        pmnc.log.error(exc_string()) # log and ignore
    else:
        try:
            _update_interfaces(interface_names)
        except:
            pmnc.log.error(exc_string()) # log and ignore

    # restart reconfigured interfaces

    for interface in _interfaces.values():
        interface.reload()

###############################################################################
# the following two methods are used by all interfaces
# to create and register completion of client requests

def begin_request(**kwargs) -> Request:

    try:
        request, request_count = _request_factory.create(**kwargs)
    except TypeError:
        raise Exception("request cannot begin, request factory has been shut down")

    pmnc.performance.event("interface.{0:s}.request_rate".format(request.interface))

    active_requests = request_count > 1 and ", {0:d} request(s) are now active".format(request_count) or ""
    if pmnc.log.noise:
        pmnc.log.noise("request {0:s} is created{1:s}".format(request.description, active_requests))

    return request

###############################################################################

def end_request(success: optional(bool), request: optional(Request) = None):

    outcome = success and "success" or "failure"
    request = request or pmnc.request

    response_ms = int(request.elapsed * 1000)
    pmnc.performance.sample("interface.{0:s}.response_time.{1:s}".\
                            format(request.interface, outcome), response_ms)
    pmnc.performance.event("interface.{0:s}.response_rate.{1:s}".\
                           format(request.interface, outcome))

    request_count = _request_factory.destroyed() # we don't care exactly which request is being destroyed

    active_requests = request_count > 0 and ", {0:d} request(s) are still active".format(request_count) or ""
    request_description = "{0:s} ".format(request.description) if request is not current_thread()._request else ""
    request_outcome = "ends with {0:s}".format(outcome) if success is not None else "is being abandoned"

    if pmnc.log.noise:
        pmnc.log.noise("request {0:s}{1:s}{2:s}".\
                       format(request_description, request_outcome, active_requests))

###############################################################################
# the default private thread pool for module interfaces processes all the requests

def _get_main_thread_pool():

    return pmnc.shared_pools.get_private_thread_pool()

###############################################################################
# this method posts the request to the global request processing pool thread

def enqueue(request: Request, f: callable, args: optional(tuple) = (), kwargs: optional(dict) = {}):

    _request_rate_sampler.tick()

    main_thread_pool = _get_main_thread_pool()
    return main_thread_pool.enqueue(request, f, args, kwargs)

###############################################################################
# this hook is used by other modules to access interfaces by name, such as "rpc"

def get_interface(interface_name: str):
    try:
        return _interfaces[interface_name].interface
    except KeyError:
        return None

###############################################################################
# this method is used by self-tests to inject a fake implementation
# of some interface or other

def set_fake_interface(interface_name: str, interface):

    assert pmnc.request.self_test and interface_name not in _interfaces
    _interfaces[interface_name] = AbstractLoadedInterface(interface)

###############################################################################
# this method is used by self-tests to remove the injected fake implementation

def delete_fake_interface(interface_name: str):

    assert pmnc.request.self_test and interface_name in _interfaces
    del _interfaces[interface_name]

###############################################################################
# this method is called from interface_performance.py to report on a web page

def get_activity_stats() -> (int, int, float): # returns active, pending, rate

    main_thread_pool = _get_main_thread_pool()
    return main_thread_pool.busy, main_thread_pool.over, _request_rate_sampler.avg

###############################################################################
# EOF
