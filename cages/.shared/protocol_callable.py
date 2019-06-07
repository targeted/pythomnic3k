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
# (c) 2005-2015, Dmitry Dvoinikov <dmitry@targeted.org>
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

    def test_cache():

        def execute(resource, *args, **kwargs):
            return ", ".join(arg.format(**kwargs) for arg in args)

        cache = {}

        def cache_get(key, **kwargs):
            value = cache.get(key)
            if value is not None:
                return value + " from cache"

        def cache_put(key, value, **kwargs):
            if value is not None:
                cache[key] = str(value) + " to cache"

        fake_request(10.0)

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo",
                pool__cache_key = "key for foo", pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("foo", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo",
                pool__cache_key = "key for foo", pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("foo to cache from cache", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo",
                pool__cache_key = "some other key", pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("foo", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", a = "foo",
                pool__cache_get = lambda key, **kwargs: key)
        assert xa.execute() == ((("execute", ), ("{a}", ), frozenset({ ("a", "foo") })), )

        # initial execution, fresh result

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar",
                pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("foo, bar", )

        # this time it is from cache

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar",
                pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("foo, bar to cache from cache", )

        def fail(*args, **kwargs):
            1 / 0

        # does not fail because cache key is None, the result is not from cache

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar",
                pool__cache_key = None, pool__cache_get = fail, pool__cache_put = fail)
        assert xa.execute() == ("foo, bar", )

        # and this time it is from cache again

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar",
                pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("foo, bar to cache from cache", )

        # if put fails, cached value is still returned

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "foo", b = "bar",
                pool__cache_get = cache_get, pool__cache_put = fail)
        assert xa.execute() == ("foo, bar to cache from cache", )

        # if get fails, a new value is returned and not cached

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "biz", b = "baz",
                pool__cache_get = fail, pool__cache_put = cache_put)
        assert xa.execute() == ("biz, baz", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "biz", b = "baz",
                pool__cache_get = cache_get, pool__cache_put = fail)
        assert xa.execute() == ("biz, baz", )

        # and since put failed, it is again not cached

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "biz", b = "baz",
                pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("biz, baz", )

        # and now it is

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute("{a}", "{b}", a = "biz", b = "baz",
                pool__cache_get = cache_get, pool__cache_put = cache_put)
        assert xa.execute() == ("biz, baz to cache from cache", )

    test_cache()

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
