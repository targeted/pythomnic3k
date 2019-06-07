#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module is a dispenser of named queues shared across modules.
# A module can get itself a queue using
#
# queue = pmnc.shared_queues.get("queue_name")
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "get" ]
__reloadable__ = False

################################################################################

import threading; from threading import Lock

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import interlocked_queue; from interlocked_queue import InterlockedQueue

###############################################################################

# module-level state => not reloadable

_shared_queues = {}
_shared_queues_lock = Lock()

###############################################################################

def get(name: str) -> InterlockedQueue:

    with _shared_queues_lock:
        queue = _shared_queues.get(name)
        if queue is None:
            queue = InterlockedQueue()
            _shared_queues[name] = queue

    return queue

###############################################################################

def self_test():

    def test_get():

        q1 = pmnc.shared_queues.get("foo")
        assert pmnc.shared_queues.get("foo") is q1

        assert _shared_queues["foo"] is q1

        q2 = pmnc.shared_queues.get("bar")
        assert q2 is not q1

        assert _shared_queues["bar"] is q2

        q1.push("biz")
        assert q1.pop() == "biz"

        assert q2.pop(1.0) is None

    test_get()

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF