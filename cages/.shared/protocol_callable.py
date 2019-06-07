#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements an simple passthrough kind of resource which
# is used by supplying callable hooks for all the resource actions,
# such as connect, disconnect, execute, commit, rollback etc.
#
# Sample callable resource configuration (config_resource_callable_1.py)
#
# connect and disconnect hooks are supplied in resource configuration file:
#
# def connect(resource):
#     resource._connection = connect(resource._config["server_address"])
#
# def disconnect(resource):
#     resource._connection.close()
#
# config = dict \
# (
# protocol = "callable",              # meta
# connect = lambda resource: None,    # callable, this gets executed for connect()
# disconnect = lambda resource: None, # callable, this gets executed for disconnect()
# )
#
# Sample resource usage (anywhere):
#
# def begin_transaction(resource, *args, **kwargs):
#     resource._connection.begin_transaction()
#
# def execute(resource, *args, **kwargs):
#     resource._connection.execute(*args, **kwargs)
#
# def commit(resource):
#     resource._connection.commit()
#
# def rollback(resource):
#     resource._connection.rollback()
#
# xa = pmnc.transaction.create()
# xa.callable(begin_transaction = begin_transaction, # this gets executed for begin_transaction()
#             execute = execute,                     # this gets executed for execute()
#             commit = commit,                       # this gets executed for commit()
#             rollback = rollback).\                 # this gets executed for rollback()
#    execute(*args, **kwargs)
# result = xa.execute()[0]
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

import time; from time import sleep

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource

###############################################################################

class Resource(TransactionalResource): # callable-hooks resource

    @typecheck
    def __init__(self, name: str, **config):
        TransactionalResource.__init__(self, name)
        self._connect = config.pop("connect", self._noop)
        self._disconnect = config.pop("disconnect", self._noop)
        self._config = config

    def connect(self):
        TransactionalResource.connect(self)
        self._connect(self)

    def begin_transaction(self, *args, **kwargs):
        self._begin_transaction = kwargs["resource_kwargs"].pop("begin_transaction", self._noop)
        self._execute = kwargs["resource_kwargs"].pop("execute", self._noop)
        self._commit = kwargs["resource_kwargs"].pop("commit", self._noop)
        self._rollback = kwargs["resource_kwargs"].pop("rollback", self._noop)
        self._begin_transaction(self, *args, **kwargs)

    def execute(self, *args, **kwargs):
        return self._execute(self, *args, **kwargs)

    def commit(self):
        self._commit(self)

    def rollback(self):
        self._rollback(self)

    def disconnect(self):
        try:
            self._disconnect(self)
        except:
            pmnc.log.error(exc_string()) # log and ignore
        finally:
            TransactionalResource.disconnect(self)

    def _noop(self, *args, **kwargs):
        pass

###############################################################################

def self_test():

    from interlocked_queue import InterlockedQueue
    from expected import expected
    from typecheck import by_regex
    from pmnc.request import fake_request
    from pmnc.resource_pool import ResourceError

    ###################################

    q = pmnc.config_resource_callable_1.get("trace_queue")

    def begin_transaction(resource, *args, **kwargs):
        resource._count += 1
        resource._q.push(("begin_transaction", resource._count, args, kwargs))

    def commit(resource):
        resource._count += 1
        resource._q.push(("commit", resource._count))

    def rollback(resource):
        resource._count += 1
        resource._q.push(("rollback", resource._count))

    ###################################

    def test_success():

        fake_request(10.0)

        def execute(resource, *args, **kwargs):
            resource._count += 1
            resource._q.push(("execute", resource._count, args, kwargs))
            return "ok"

        # success sequence: connect, begin_transaction, execute, commit (then the instance is put back to the pool)

        xa = pmnc.transaction.create(biz = "baz")
        xa.callable_1("abc", begin_transaction = begin_transaction, execute = execute,
                      commit = commit, rollback = rollback, foo = "bar").execute(1, 2, eee = "fff")
        assert xa.execute() == ("ok", )

        # now check the trace

        assert q.pop(0.0) == ("connect", 0, { "param1": "value1", "param2": "value2" })
        m, c, args, kwargs = q.pop(0.0)
        assert m == "begin_transaction" and c == 1
        assert args == (xa._xid, )
        assert kwargs == dict(transaction_options = { "biz": "baz" }, source_module_name = __name__,
                              resource_args = ("abc", ), resource_kwargs = { "foo": "bar" })
        assert q.pop(0.0) == ("execute", 2, (1, 2), { "eee": "fff" })
        assert q.pop(0.0) == ("commit", 3) # commit is waited upon, therefore "commit" is in the queue
        assert q.pop(1.0) is None

    test_success()

    ###################################

    def test_failure():

        fake_request(10.0)

        def execute(resource, *args, **kwargs):
            1 / 0

        # failure sequence (the instance is reused): begin_transaction, execute, rollback, disconnect

        xa = pmnc.transaction.create(biz = "baz")
        xa.callable_1("abc", begin_transaction = begin_transaction, execute = execute,
                      commit = commit, rollback = rollback, foo = "bar").execute(1, 2, eee = "fff")
        try:
            xa.execute()
        except ResourceError as e:
            assert by_regex("^(?:int )?division (?:or modulo )?by zero$")(str(e))
            assert not e.recoverable and e.terminal
        else:
            assert False

        # now check the trace

        m, c, args, kwargs = q.pop(0.0)
        assert m == "begin_transaction" and c == 4
        assert args == (xa._xid, )
        assert kwargs == dict(transaction_options = { "biz": "baz" }, source_module_name = __name__,
                              resource_args = ("abc", ), resource_kwargs = { "foo": "bar" })
        assert q.pop(1.0) == ("rollback", 5) # rollback is not waited upon, therefore "rollback" may not appear in the queue immediately
        assert q.pop(1.0) == ("disconnect", 6)
        assert q.pop(1.0) is None

    test_failure()

    ###################################

    def test_cache_1():

        def execute(resource, *args, **kwargs):
            return ", ".join(arg.format(**kwargs) for arg in args)

        def to_cache(cache, key, value, **kwargs):
            cache.put(key, value + " to cache", **kwargs)

        def from_cache(cache, key):
            value = cache.get(key)
            if value is not None:
                return value + " from cache"

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo",
                pool__cache_key = "key for foo", pool__cache_update = to_cache)
        assert xa.execute() == ("foo", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo",
                pool__cache_key = "key for foo", pool__cache_lookup = from_cache)
        assert xa.execute() == ("foo to cache from cache", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo", pool__cache_key = "some other key")
        assert xa.execute() == ("foo", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo", pool__cache_lookup = lambda cache, key: key)
        assert xa.execute() == ((("execute", ), ("{a}", ), frozenset({ ("a", "foo") })), )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar", pool__cache_ttl = 1.0)
        assert xa.execute() == ("foo, bar", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar", pool__cache_lookup = from_cache)
        assert xa.execute() == ("foo, bar from cache", )

        def fail(*args, **kwargs):
            1/0

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar", pool__cache_key = None, pool__cache_lookup = fail, pool__cache_update = fail)
        assert xa.execute() == ("foo, bar", )

        sleep(1.5)

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar", pool__cache_lookup = from_cache)
        assert xa.execute() == ("foo, bar", )

    test_cache_1()

    ###################################

    def test_cache_2():

        def execute(resource, *args, **kwargs):
            class C: pass # tag
            return C

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute()
        C1 = xa.execute()[0]

        sleep(1.25)

        # default ttl for pool 5 is 2 seconds therefore the entry is not expired

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute()
        C2 = xa.execute()[0]
        assert C2 is C1

        sleep(1.25)

        # default ttl for pool 5 is 2 seconds therefore the entry is expired

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute(pool__cache_ttl = 0.75)
        C3 = xa.execute()[0]
        assert C3 is not C2

        sleep(1.25)

        # entry ttl is 0.75 second therefore it is expired

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute(pool__cache_weight = None, pool__cache_ttl = None)
        C4 = xa.execute()[0]
        assert C4 is not C3

        sleep(1.5)

        # entry ttl is None therefore it never expires

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute()
        C5 = xa.execute()[0]
        assert C5 is C4

        # note that C4 = C5 is cached forever, which affects the following test

        return C5

    C5 = test_cache_2()

    ###################################

    def test_cache_3():

        def execute(resource, *args, **kwargs):
            class C:
                index = args[0]
            return C

        fake_request(30.0)

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute(1, pool__cache_weight = 0.2, pool__cache_ttl = 20.0)
        xa.callable_5(execute = execute).execute(2, pool__cache_weight = None, pool__cache_ttl = 30.0)
        xa.callable_5(execute = execute).execute(3, pool__cache_weight = 0.3, pool__cache_ttl = None)
        C1, C2, C3 = xa.execute()

        assert C1.index == 1
        assert C2.index == 2
        assert C3.index == 3

        sleep(4.0) # so that the next transaction initiates the eviction, assuming pool__cache_evict_period = 3 in config_resource_callable_5.py

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute(4, pool__cache_weight = None)
        C4 = xa.execute()[0]

        # cache size is 4 and we have 3 entries with no weight including
        # C5 from above and two entries with weight

        def execute(resource, *args, **kwargs):
            return "miss"

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute()
        xa.callable_5(execute = execute).execute(2)
        xa.callable_5(execute = execute).execute(3)
        xa.callable_5(execute = execute).execute(4)
        C5_, C2_, C3_, C4_ = xa.execute()

        assert C5_ is C5 and C2_ is C2 and C3_ is C3 and C4_ is C4

        xa = pmnc.transaction.create()
        xa.callable_5(execute = execute).execute(1)
        C1_ = xa.execute()[0]

        assert C1_ is not C1 and C1_ == "miss"

    test_cache_3()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF