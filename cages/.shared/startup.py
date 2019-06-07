#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################

__all__ = [ "start", "wait", "maintenance", "exit", "stop" ]
__reloadable__ = False

################################################################################

import threading; from threading import Event

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string

###############################################################################

# module-level state => not reloadable

_stop = Event()

###############################################################################

def _update_log_level(): # pick up log_level from config_interfaces.py
    try:
        pmnc._loader.set_log_level(pmnc.config_interfaces.get("log_level", "LOG"))
    except:
        pmnc.log.error(exc_string()) # log and ignore

###############################################################################

def start():

    _update_log_level()

    pmnc.state.start()
    pmnc.performance.start()
    pmnc.interfaces.start()

###############################################################################

def wait(timeout: float) -> bool:

    _stop.wait(timeout) # this may spend waiting slightly less, but it's ok
    return _stop.is_set()

###############################################################################

def maintenance(): # periodic housekeeping, should not throw

    _update_log_level()

    pmnc.interfaces.reload()

    pmnc.log.flush()

###############################################################################

def exit():

    _stop.set()

###############################################################################

def stop():

    pmnc.interfaces.stop()
    pmnc.performance.stop()
    pmnc.state.stop()

###############################################################################
# EOF