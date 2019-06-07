#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements a generic resource pool. The allocated resources
# are connectable (have connect/disconnect method pair) and can expire, upon
# a timeout or perhaps upon failure. The pool performs connecting before
# it gives out an allocated resource, reuses released resources until they
# expire and disconnects resources when they expire. A pool may be configured
# to be kept warm, i.e. have some free instances always available.
#
# This module also contains implementations of three resource classes, whose
# descendants are to be used with pools. First, Resource is a minimal generic
# base class. Second, TransactionalResource is derived from Resource and
# equipped with transaction related methods such as commit. It is to be used
# with transactions in regular Pythomnic manner:
#
# xa = pmnc.transaction.create() # resource "foo" should be configured in
# xa.foo.do_stuff(...)           # config_resource_foo.py to be using some
# xa.execute()                   # protocol implemented with TransactionalResource
#
# Third, there is SQLResource class derived from TransactionalResource
# which is a good starting point for database connections. It implements
# batch execution of several SQL queries in one transaction and also data
# conversion from Python to SQL and vice versa, to be used like this:
#
# xa = pmnc.transaction.create()
# xa.some_db.execute("INSERT INTO t VALUES ({x})",         # query #1
#                    "UPDATE t SET a = {x} WHERE k = {k}", # query #2
#                    "SELECT f FROM t WHERE k = {k}",      # query #3
#                    x = "value", k = 123)                 # parameters
# rs = xa.execute()[0][2] # results of query #3
# for r in rs:            # list of SQLRecord instances
#     print(r["f"])       # prints field value
#
# Errors to be thrown from resource instances in the course of transactions
# are also implemented here, they are all derived from ParticipantError,
# which conveys information about which transaction participant has failed,
# and this exception's useful concrete descendants are:
# ResourceError - delivers error code, message and the fact of whether
# the encountered error had effects that cannot be recovered,
# SQLResourceError - same as previous but equpped with SQL state property,
# RPCError - remote exception from an RPC call rethrown locally,
# TransactionExecutionError - thrown by the transaction itself rather
# than by one of its participants, for example because of a timeout,
# TransactionCommitError - thrown again by the transaction, in case one
# of the participants fails to commit, which is considered serious failure.
#
# Caching:
#
# Any resource pool can be instrumented with a read cache. Its behaviour
# is controlled by the following configuration parameters in the resource
# configuration file:
#
# >> pool__cache_size = N,
# Maximum number of cached entries to keep in the cache.
#
# >> pool__cache_policy = "...",
# Eviction policy, string literal, one of "lru", "lfu", "weight", "old" or "random":
# lru = evict entries least recently used
# lfu = evict entries least (often) used
# weight = evict entries that took least time to create (cheapest to regenerate)
# old = evict entries that are to expire soon (FIFO-like)
# random = evict entries at random
#
# >> pool__cache_default_ttl = N.N,
# Default cached entry time to live in float seconds. None (default) means forever.
#
# >> pool__cache_evict_period = N.N,
# Eviction can occur at most once in N.N float seconds. Default is 10.0 seconds.
#
# As soon as you specify pool__cache_size for a resource, a cache is enabled
# and all identical requests will return the cached results. Identical are
# two resource requests with identical methods, args and kwargs:
#
# xa.resource.foo(1, biz = "baz") # original
# xa.resource.foo(1, biz = "baz") # identical
# xa.resource.bar(1, biz = "baz") # different
# xa.resource.foo(2, biz = "baz") # different
# xa.resource.foo(1, baz = "biz") # different
# xa.resource.foo(1, biz = "buz") # different
#
# You can control the cache behaviour by supplying the following kwargs
# to the resource call:
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_ttl = M.M)
# The result of this particular call is cached for M.M seconds,
# explicit None value means forever.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_weight = K.K)
# The result of this particular call is marked with weight K.K,
# which is used with "weight" eviction policy.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_key = "key")
# The result of this particular call is cached under this literal key,
# thus changing the above "identical" semantics. As an alternative,
# a function could be passed for pool__cache_key, which then in turn
# returns the actual key value.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_lookup = lambda: cache, key: ...)
# Callable to override cache lookup behaviour altogether.
# The default behaviour is something like cache.get(key)
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_update = lambda: cache, key, value: ...)
# Callable to override cache update behaviour altogether.
# The default behaviour is something like cache.put(key, value)
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource", "TransactionalResource", "SQLResource", "SQLRecord",
            "ResourcePool", "RegisteredResourcePool", "ResourcePoolEmpty",
            "ResourcePoolStopped", "ResourceError", "ResourceInputParameterError",
            "SQLResourceError", "RPCError", "TransactionError",
            "TransactionCommitError", "TransactionExecutionError" ]

###############################################################################

import threading; from threading import Lock, Event, current_thread, Semaphore
import decimal; from decimal import Decimal, localcontext, Inexact
import datetime; from datetime import datetime
import collections; from collections import MutableMapping
import sys; from sys import exc_info
import time; from time import time
import inspect; from inspect import isfunction
import copy; from copy import deepcopy
import heapq; from heapq import nsmallest
import random; from random import randint

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, callable, tuple_of, optional, one_of
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.threads; from pmnc.threads import HeavyThread, LightThread
import pmnc.request; from pmnc.request import Request

###############################################################################

if __name__ == "__main__":
    class _BaseError(Exception): pass
else:
    _BaseError = Exception

# ParticipantError is a base exception thrown from transactions,
# at a minimum it may contain information about which resource
# failed in the transaction in its participant_index property

class ParticipantError(_BaseError):

    @typecheck
    def __init__(self, *,
                 description: optional(str) = None,
                 participant_index: optional(int) = None):

        self.__description = description
        self.__participant_index = participant_index

        _BaseError.__init__(self, str(self))

    description = property(lambda self: self.__description)

    @typecheck
    def _wparticipant_index(self, participant_index: int):
        self.__participant_index = participant_index

    participant_index = property(lambda self: self.__participant_index,
                                 lambda self, i: self._wparticipant_index(i))

    def __str__(self):
        return self.__description or ""

    @classmethod
    def snap_exception(cls, **kwargs):
        t, v, tb = exc_info()[:3]
        kwargs.setdefault("description", str(v))
        return cls(**kwargs).with_traceback(tb)

    @classmethod
    def rethrow(cls, **kwargs):
        raise cls.snap_exception(**kwargs)

# TransactionError and its descendants are thrown by the Transaction class
# in case the transaction could not be completed, if some participating
# resource did not return a result, failed to commit, or transaction results
# have not been accepted; application code should not throw such errors

class TransactionError(ParticipantError): pass
class TransactionExecutionError(TransactionError): pass
class TransactionCommitError(TransactionError): pass

# ResourceError and its descendants are thrown by the Resource classes
# whenever there is an execution problem; ResourceError can be recoverable
# i.e. if there have not been unrecoverable changes done in transaction
# at hand, and terminal i.e. it should result in resource instance being
# disconnected and not reused

class ResourceError(ParticipantError):

    @typecheck
    def __init__(self, *,
                 code: optional(int) = None,
                 recoverable: optional(bool) = None,
                 terminal: optional(bool) = None,
                 **kwargs):

        self.__code = code
        self.__recoverable = False if recoverable is None else recoverable
        self.__terminal = True if terminal is None else terminal

        ParticipantError.__init__(self, **kwargs)

    code = property(lambda self: self.__code)
    recoverable = property(lambda self: self.__recoverable)
    terminal = property(lambda self: self.__terminal)

    def __str__(self):
        code = "" if self.__code is None else str(self.__code)
        description = self.description or ""
        if code and description:
            code += ": "
        return "{0:s}{1:s}".format(code, description)

# ResourceInputParameterError is supposed to be used along with
# typecheck_with_exceptions on actual resource methods, so that
# if the caller provides wrong parameters the thrown ResourceError
# indicates that nothing has been done yet

class ResourceInputParameterError(ResourceError):

    def __init__(self, description):
        ResourceError.__init__(self, description = description,
                               recoverable = True, terminal = True)

# SQLResourceError is a descendant of ResourceError equipped
# with SQL_STATE property to be returned from SQL queries

class SQLResourceError(ResourceError):

    @typecheck
    def __init__(self, *, state: optional(str) = None, **kwargs):
        self.__state = state
        ResourceError.__init__(self, **kwargs)

    state = property(lambda self: self.__state)

    def __str__(self):
        code = "" if self.code is None else str(self.code)
        state = "[{0:s}]".format(self.__state) if self.__state else ""
        description = self.description or ""
        if code and (state or description):
            code += ": "
        if state and description:
            state += " "
        return "{0:s}{1:s}{2:s}".format(code, state, description)

# RPCError is thrown by RPC resource to reintroduce
# remote exceptions, see protocol_rpc.py

class RPCError(ResourceError): pass

###############################################################################

class Resource: # a minimal connectable resource implementation

    @typecheck
    def __init__(self, name: str):
        self.__name = name
        self.__expired = Event()

    name = property(lambda self: self.__name)

    def expire(self):
        self.__expired.set()

    def _expired(self):
        return self.__expired.is_set()

    expired = property (lambda self: self._expired())

    def connect(self):
        pass

    def disconnect(self):
        pass

###############################################################################

# usable resource with transactions-related methods, instances of subclasses
# of this class are allocated through pmnc.resource.create

class TransactionalResource(Resource):

    _default_idle_timeout = 30.0 # three times the default request timeout
    _default_max_age = 300.0     # ten times idle timeout
    _default_min_time = 0.0      # zero, to have no effect by default
    _default_max_time = None     # to have no effect by default

    @typecheck
    def __init__(self, name: str):
        Resource.__init__(self, name)
        self.__idle_timeout = Timeout(self._default_idle_timeout)
        self.__max_age = Timeout(self._default_max_age)
        self.__min_time = self._default_min_time
        self.__max_time = self._default_max_time

    # the following methods allow allocated instances
    # to expire or not to expire within some time

    @typecheck
    def set_idle_timeout(self, idle_timeout: float):
        self.__idle_timeout.reset(idle_timeout)

    def reset_idle_timeout(self):
        self.__idle_timeout.reset()

    @typecheck
    def set_max_age(self, max_age: float):
        self.__max_age.reset(max_age)

    def _expired(self):
        return self.__idle_timeout.expired or self.__max_age.expired or \
               Resource._expired(self)

    def _rttl(self):
        return min(self.__idle_timeout.remain, self.__max_age.remain)

    ttl = property(_rttl)

    @typecheck
    def set_min_time(self, min_time: float):
        self.__min_time = min_time

    min_time = property(lambda self: self.__min_time)

    @typecheck
    def set_max_time(self, max_time: float):
        self.__max_time = max_time

    max_time = property(lambda self: self.__max_time)

    # knowing name and size of the pool from which this resource instance
    # has been dispensed from is beneficial when you need to allocate
    # private thread pool within a resource instance's method, see
    # protocol_cmdexec.py for example, and I'm limiting this knowledge to
    # name and size only, refusing to insert a reference to the pool itself

    def set_pool_info(self, pool_name, pool_size):
        self.__pool_name, self.__pool_size = pool_name, pool_size

    pool_name = property(lambda self: self.__pool_name)
    pool_size = property(lambda self: self.__pool_size)

    # see .shared/transaction.py which uses these methods

    @typecheck
    def begin_transaction(self, xid: str, *, source_module_name: str,
                          transaction_options: dict,
                          resource_args: tuple, resource_kwargs: dict):
        self.__xid, self.__source_module_name = xid, source_module_name
        self.__transaction_options = transaction_options
        self.__resource_args, self.__resource_kwargs = resource_args, resource_kwargs

    xid = property(lambda self: self.__xid)
    source_module_name = property(lambda self: self.__source_module_name)
    transaction_options = property(lambda self: self.__transaction_options)
    resource_args = property(lambda self: self.__resource_args)
    resource_kwargs = property(lambda self: self.__resource_kwargs)

    def commit(self): # override
        pass

    def rollback(self): # override
        pass

    # default implementations of cache controlling methods,
    # those are tested not here but in transaction.py and
    # protocol_callable.py

    class _no_cache_key: pass # tag
    class _default_ttl: pass # tag
    class _default_weight: pass # tag

    def _cache_key(self, cache, attrs, args, kwargs):
        return (tuple(attrs), args, frozenset(kwargs.items()))

    # note that cache_key explicitly removes pool__cache_... parameters
    # from kwargs passed as by reference dict so that they do not appear
    # in actual resource execution

    def cache_key(self, cache, attrs, args, kwargs):

        cache_key = kwargs.pop("pool__cache_key", self._no_cache_key)
        self.__cache_lookup = kwargs.pop("pool__cache_lookup", self._cache_lookup)
        self.__cache_update = kwargs.pop("pool__cache_update", self._cache_update)
        self.__cache_ttl = kwargs.pop("pool__cache_ttl", self._default_ttl)
        self.__cache_weight = kwargs.pop("pool__cache_weight", self._default_weight)

        # this method is called from transaction even if caching is disabled for
        # a resource and it exits right after the meta parameters have been removed

        if not cache:

            # custom implementations of caching could be using their own custom
            # parameters, presumably named pool__cache_... therefore we remove
            # them in case caching is disabled

            custom_kwargs = { k for k in kwargs.keys() if k.startswith("pool__cache_") }
            for k in custom_kwargs:
                del kwargs[k]

            return None

        # a key could either be a literal value or a function returning one

        if isfunction(cache_key):
            cache_key = cache_key(cache, attrs, args, kwargs)

        if cache_key is not self._no_cache_key:
            return cache_key # this can return None if explicitly requested by the caller
        else:
            return self._cache_key(cache, attrs, args, kwargs)

    # note that cache_lookup does not see the removed pool__cache_... parameters

    def _cache_lookup(self, cache, key):
        return cache.get(key)

    def cache_lookup(self, cache, key):
        return self.__cache_lookup(cache, key)

    # note that cache_update sees the removed pool__cache_... parameters

    def _cache_update(self, cache, key, value, **kwargs):
        cache.put(key, value, **kwargs)

    def cache_update(self, cache, key, value, **kwargs):

        # parameters from cache_key are inserted back if there were specified

        if self.__cache_ttl is not self._default_ttl:
            kwargs["pool__cache_ttl"] = self.__cache_ttl

        if self.__cache_weight is not self._default_weight:
            kwargs["pool__cache_weight"] = self.__cache_weight

        self.__cache_update(cache, key, value, **kwargs)

###############################################################################

class SQLRecord(MutableMapping): # this is essentially a UserDict with case-insensitive keys

    @typecheck
    def __init__(self, data: dict):
        self._keys = { key_.upper(): key_ for key_ in data }
        self.data = { key_: data[key_] for key_ in self._keys.values() }

    def __len__(self):
        return len(self.data)

    @typecheck
    def __getitem__(self, key: str):
        key_ = self._keys.get(key.upper())
        if key_ in self.data:
            return self.data[key_]
        raise KeyError(key)

    @typecheck
    def __setitem__(self, key_: str, value):
        KEY = key_.upper()
        if KEY in self._keys:
            del self.data[self._keys[KEY]]
        self._keys[KEY] = key_
        self.data[key_] = value

    @typecheck
    def __delitem__(self, key: str):
        key_ = self._keys.pop(key.upper())
        del self.data[key_]

    def __iter__(self):
        return iter(self.data)

    @typecheck
    def __contains__(self, key: str):
        return key.upper() in self._keys

    def __repr__(self):
        return repr(self.data)

###############################################################################

class SQLResource(TransactionalResource):

    @typecheck
    def __init__(self, name: str, *, decimal_precision: (int, int)):
        TransactionalResource.__init__(self, name)
        self.__precision, self.__scale = decimal_precision
        self.__decimal_ref = Decimal(10 ** self.__precision - 1) / Decimal(10 ** self.__scale)

    precision = property(lambda self: self.__precision)
    scale = property(lambda self: self.__scale)

    ###################################

    def execute(self, *batch, **params):
        params = { k: self._py_to_sql(v) for k, v in params.items() } # note that all parameters are converted
        return tuple(self._execute(sql, params) for sql in batch)     # before any requests are executed

    @typecheck                                  # if this check fails, the resulting InputParameterError is
    def _execute(self, sql: str, params: dict): # converted into unrecoverable ResourceError in transaction
        result = []
        records = self._execute_sql(sql, params)
        for record in records:
            result.append(SQLRecord({ str(k): self._sql_to_py(v) for k, v in record.items() }))
        return result

    ###################################

    _supported_types = set((type(None), int, Decimal, bool, datetime, str, bytes))

    ###################################

    # py to sql conversions are done before any sql requests are executed,
    # therefore if there is a problem, no changes could have been made

    def _py_to_sql(self, v):
        t = type(v)
        if t not in self._supported_types:
            raise ResourceError(description = "type {0:s} is not supported".format(t.__name__),
                                recoverable = True, terminal = False)
        return getattr(self, "_py_to_sql_{0:s}".format(t.__name__))(v)

    def _py_to_sql_NoneType(self, v):
        return v

    def _py_to_sql_int(self, v):
        if -9223372036854775808 <= v <= 9223372036854775807:
            return v
        else:
            raise ResourceError(description = "integer value too large",
                                recoverable = True, terminal = False)

    def _py_to_sql_Decimal(self, v):
        return self._ensure_decimal_in_range(
            v, lambda description: ResourceError(description = description,
                                                 recoverable = True, terminal = False))

    def _py_to_sql_bool(self, v):
        return v

    def _py_to_sql_datetime(self, v):
        return v

    def _py_to_sql_str(self, v):
        return v

    def _py_to_sql_bytes(self, v):
        return v

    ###################################

    # sql to py conversions are done on each request's results after it returns,
    # therefore if there is a problem, changes could have been made

    def _sql_to_py(self, v):
        while True:
            pv = getattr(self, "_sql_to_py_{0:s}".format(type(v).__name__),
                         self._sql_to_py_unknown)(v)
            if pv is v: break
            v = pv
        if type(v) in self._supported_types:
            return v
        else:
            raise ResourceError(description = "type {0:s} is not supported".format(type(v).__name__),
                                recoverable = False, terminal = True)

    def _sql_to_py_NoneType(self, v):
        return v

    def _sql_to_py_int(self, v):
        if -9223372036854775808 <= v <= 9223372036854775807:
            return v
        else:
            raise ResourceError(description = "integer value too large",
                                recoverable = False, terminal = True)

    def _sql_to_py_Decimal(self, v):
        return self._ensure_decimal_in_range(
            v, lambda description: ResourceError(description = description,
                                                 recoverable = False, terminal = True))

    def _sql_to_py_bool(self, v):
        return v

    def _sql_to_py_datetime(self, v):
        return v

    def _sql_to_py_str(self, v):
        return v

    def _sql_to_py_bytes(self, v):
        return v

    def _sql_to_py_unknown(self, v):
        raise ResourceError(description = "type {0:s} cannot be converted".format(type(v).__name__),
                            recoverable = False, terminal = True)

    ###################################

    @typecheck
    def _ensure_decimal_in_range(self, v: Decimal, e: callable) -> Decimal:
        with localcontext() as ctx:
            ctx.traps[Inexact] = True
            try:
                v.quantize(self.__decimal_ref)
            except Inexact:
                raise e("decimal value too precise")
        if abs(v) > self.__decimal_ref:
            raise e("decimal value too large")
        return v

###############################################################################

class ResourcePoolEmpty(Exception): pass
class ResourcePoolStopped(Exception): pass

###############################################################################

class ResourcePoolCache:

    class CachedValue:

        @typecheck
        def __init__(self, value, *, ttl: optional(float) = None, weight: optional(float) = None):
            self._value = deepcopy(value)
            self._timeout = Timeout(ttl) if ttl else None
            self._weight = weight
            self._use_count = 0
            self.touch()

        value = property(lambda self: deepcopy(self._value))
        expired = property(lambda self: self._timeout.expired if self._timeout is not None else False)
        ttl = property(lambda self: self._timeout.remain if self._timeout is not None else None)
        weight = property(lambda self: self._weight)
        last_used = property(lambda self: self._last_used)
        use_count = property(lambda self: self._use_count)

        def touch(self):
            self._last_used = time() # note that usage has nothing to do with expiration
            self._use_count += 1

    _default_evict_period = 10.0

    @typecheck
    def __init__(self, name: str, *,
                 size: optional(int) = None,
                 policy: optional(one_of("lru", "lfu", "weight", "old", "random")) = None,
                 default_ttl: optional(float) = None,
                 evict_period: optional(float) = None):

        self._name = name
        self._size = size # can be None meaning unrestricted
        self._policy = policy or "lru"
        self._default_ttl = default_ttl # can be None meaning unspecified
        self._evict_period = evict_period or self._default_evict_period

        self._evict_key = getattr(self, "_evict_{0:s}".format(self._policy))
        self._evict_timeout = Timeout(self._evict_period)
        self._lock, self._cache = Lock(), {}
        self._sync_lock = Lock()

    name = property(lambda self: self._name)
    size = property(lambda self: self._size)
    policy = property(lambda self: self._policy)
    default_ttl = property(lambda self: self._default_ttl)
    evict_period = property(lambda self: self._evict_period)

    # utility method to set caching parameters on per-value basis

    def _wrap(self, value, **kwargs):
        return self.CachedValue(value,
            ttl = kwargs.get("pool__cache_ttl", self._default_ttl),
            weight = kwargs.get("pool__cache_weight"))

    def _evict(self):
        if self._evict_timeout.expired:
            try:
                if self._size:
                    evict_count = len(self._cache) - self._size
                    if evict_count > 0:
                        now = time()
                        for k, _ in nsmallest(evict_count, self._cache.items(),
                                        key = lambda k_v: self._evict_key(k_v[1], now)):
                            del self._cache[k]
            finally:
                self._evict_timeout.reset()

    # the following methods are comparison keys which return smaller values for preferred eviction

    # least recently used, remove entries that have not been used for a while

    def _evict_lru(self, v, now):
        return v.last_used - now

    # least (frequently) used, remove entries that have seldom been used

    def _evict_lfu(self, v, _):
        return v.use_count

    # remove cheapest entries (that it took least time to generate), note that values with unspecified weight get removed last

    def _evict_weight(self, v, _):
        return v.weight if v.weight is not None else float("inf")

    # remove entries that are to expire soon, note that values that never expire get removed last

    def _evict_old(self, v, _):
        return v.ttl if v.ttl is not None else float("inf")

    # remove entries at random

    def _evict_random(self, v, _):
        return randint(0, 2147483647)

    # protocol methods

    def __getitem__(self, k):
        with self._lock:
            self._evict()
            cv = self._cache[k]
            if cv.expired:
                del self._cache[k]
                raise KeyError(k)
        cv.touch()
        return cv.value

    def get(self, k, d = None):
        try:
            return self[k]
        except KeyError:
            return d

    def __setitem__(self, k, v):
        with self._lock:
            self._evict()
            if isinstance(v, self.CachedValue):
                self._cache[k] = v
            else:
                self._cache[k] = self._wrap(v)

    def put(self, k, v, **kwargs):
        self[k] = self._wrap(v, **kwargs)

    def __delitem__(self, k):
        with self._lock:
            self._evict()
            del self._cache[k]

    # called by the sweep thread

    def purge(self):
        with self._lock:
            self._evict()
            self._cache = { k: v for k, v in self._cache.items() if not v.expired }

    # used to make atomic accesses

    def lock(self):
        self._sync_lock.acquire()

    def unlock(self):
        self._sync_lock.release()

    def __enter__(self):
        self.lock()

    def __exit__(self, t, v, tb):
        self.unlock()

    # debugging aid

    def __iter__(self):
        with self._lock:
            return iter([ (k, v.value) for k, v in self._cache.items() ])

###############################################################################

class ResourcePool: # a fixed size pool of resources

    @typecheck
    def __init__(self, name: str, factory: callable, size: int = 2147483647,
                 standby: int = 0, cache: optional(ResourcePoolCache) = None):
        self._name, self._factory, self._size, self._standby = name, factory, size, min(size, standby)
        self._lock, self._stopped = Lock(), False
        self._free, self._busy, self._count = [], [], 0
        self._cache = cache

    name = property(lambda self: self._name)
    size = property(lambda self: self._size)
    cache = property(lambda self: self._cache)

    def rfree(self):
        with self._lock:
            return len(self._free)
    free = property(lambda self: self.rfree())

    def rbusy(self):
        with self._lock:
            return len(self._busy)
    busy = property(lambda self: self.rbusy())

    def _create(self):
        self._count += 1
        return self._factory("{0:s}/{1:d}".format(self._name, self._count))

    ###################################

    def _connect(self, resource):
        try:
            resource.connect()
        except:
            with self._lock:
                self._busy.remove(resource)
            raise

    def _disconnect(self, resource):
        try:
            resource.disconnect()
        except:
            pass
        with self._lock:
            try:
                self._busy.remove(resource)
            except ValueError:
                pass

    ###################################
    # this method returns a connected instance of a resource,
    # either from a free list, or freshly created, note how
    # the connection is established outside the lock

    @typecheck
    def allocate(self) -> Resource:
        resource = None
        while True:
            if resource:
                self._disconnect(resource)
            with self._lock:
                if self._stopped:
                    raise ResourcePoolStopped("resource pool is stopped")
                if self._free:
                    resource, connect = self._free.pop(), False
                    if resource.expired:
                        continue
                elif len(self._busy) < self._size:
                    resource, connect = self._create(), True
                else:
                    raise ResourcePoolEmpty("resource pool is empty")
                self._busy.append(resource)
                break
        if connect: self._connect(resource)
        self.warmup()
        return resource

    ###################################
    # this method puts a resource instance previously
    # given out to a client back to the free list

    @typecheck
    def release(self, resource: Resource):
        if not resource.expired:
            with self._lock:
                try:
                    self._busy.remove(resource)
                except ValueError:
                    pass
                else:
                    self._free.append(resource)
        else:
            self._disconnect(resource)
        self.warmup()

    ###################################
    # this method is called periodically by the watchdog thread
    # to remove the expired resources from the free pool

    def sweep(self):
        return self._spawn_thread(self._sweep)

    def _sweep(self):

        if self._sweep_sem.acquire(blocking = False): # allow only one thread to be sweeping the pool
            try:
                while True:
                    with self._lock:
                        for resource in self._free:
                            if resource.expired:
                                self._free.remove(resource)
                                self._busy.append(resource)
                                break # for
                        else:
                            break # while
                    self._disconnect(resource)
            finally:
                self._sweep_sem.release()

        # the same thread is also performing pool warmup

        self._warmup()

        # and cache purging

        if self._cache: self._cache.purge()

    _sweep_sem = Semaphore()

    ###################################
    # this method is called periodically by the watchdog thread
    # to keep a few free resources available in the free pool

    def warmup(self):
        with self._lock:
            if not self._warmup_required():
                return
        return self._spawn_thread(self._warmup)

    def _warmup(self):
        if self._warmup_sem.acquire(blocking = False): # allow only one thread to be warming the pool up
            try:
                while True:
                    with self._lock:
                        if self._stopped or not self._warmup_required():
                            return
                        resource = self._create()
                        self._busy.append(resource)
                    try:
                        self._connect(resource)
                    except:
                        return # connection failed, bail out
                    else:
                        self.release(resource)
            finally:
                self._warmup_sem.release()

    _warmup_sem = Semaphore()

    # self._lock must be held to use this

    def _warmup_required(self):
        return len(self._free) < self._standby and \
               len(self._free) + len(self._busy) < self._size

    ###################################
    # this method is called by the watchdog thread
    # to stop the pool at cage shutdown

    def stop(self):
        return self._spawn_thread(self._stop)

    def _stop(self):
        if self._stop_sem.acquire(blocking = False): # allow only one thread to be stopping the pool
            try:
                with self._lock:
                    self._stopped = True
                    for resource in self._free + self._busy:
                        resource.expire()
            finally:
                self._stop_sem.release()
            self._sweep() # the same thread is entering _sweep, only if it has succeeded with stopping

    _stop_sem = Semaphore()

    ###################################

    def _spawn_thread(self, proc):
        proc_name = "{0:s}:{1:s}".format(self.name, proc.__name__[1:])
        th = LightThread(target = proc, name = proc_name)
        th._request = Request(interface = "__pool__", protocol = "n/a", timeout = 30.0)
        th.start()
        return th

################################################################################

# all instances of this descendant class are registered in a global list
# and are periodically swept, having their expired resources disconnected

class RegisteredResourcePool(ResourcePool):

    _pools_lock = Lock()
    _pools = []

    def __init__(self, *args, **kwargs):
        with self._pools_lock:
            if not self._sweeper.is_alive():
                raise ResourcePoolStopped("all resource pools are stopped")
            ResourcePool.__init__(self, *args, **kwargs)
            self._pools.append(self)

    ################################### a separate thread schedules sweeping and stopping

    @classmethod
    def start_pools(cls, pools_sweep_period):
        cls._pools_sweep_period = pools_sweep_period
        cls._sweeper = HeavyThread(target = cls._sweeper_proc, name = "resource_pool:swp")
        cls._sweeper.start()

    @classmethod
    def stop_pools(cls):
        if hasattr(cls, "_sweeper"): # has started
            cls._sweeper.stop()

    @classmethod
    def _sweeper_proc(cls):
        timeout = cls._pools_sweep_period
        interval = timeout
        while not current_thread().stopped(interval): # keep periodically sweeping the registered pools
            with cls._pools_lock:
                interval = timeout / (len(cls._pools) or 1)
                if not cls._pools:
                    continue
                pool = cls._pools.pop(0)
            try:
                pool.sweep()
            finally:
                with cls._pools_lock:
                    cls._pools.append(pool)
        with cls._pools_lock: # stop the registered pools
            pools, cls._pools = cls._pools, []
        ths = [ pool.stop() for pool in pools ]
        timeout = Timeout(timeout)
        for th in ths:
            th.join(timeout.remain)
            if timeout.expired:
                break

################################################################################

if __name__ == "__main__":

    print("self-testing module resource_pool.py:")

    ###################################

    from time import sleep
    from random import random
    from expected import expected
    from typecheck import InputParameterError, ReturnValueError, by_regex
    from exc_string import exc_string
    from collections import OrderedDict

    ###################################

    # test cached value

    rpc = ResourcePoolCache("name", size = 1, policy = "lru", default_ttl = 10.0)

    ########

    cv = rpc._wrap("biz")

    assert cv.value == "biz"
    assert cv.weight is None
    assert not cv.expired
    assert cv.ttl > 9.0
    assert time() - cv.last_used < 0.5
    assert cv.use_count == 1

    sleep(1.5)

    assert not cv.expired
    assert cv.value == "biz"
    assert cv.weight is None
    assert cv.ttl > 7.0
    assert time() - cv.last_used < 2.5
    assert cv.use_count == 1

    ########

    cv = rpc._wrap("foo", pool__cache_ttl = 1.0, pool__cache_weight = 0.21)

    assert cv.value == "foo"
    assert cv.weight == 0.21
    assert not cv.expired
    assert 0.5 < cv.ttl < 1.5
    assert time() - cv.last_used < 0.5
    assert cv.use_count == 1

    sleep(1.5)

    assert cv.expired
    assert cv.value == "foo"
    assert cv.weight == 0.21
    assert cv.ttl == 0.0
    assert time() - cv.last_used > 1.0
    assert cv.use_count == 1

    cv.touch()

    assert cv.expired
    assert time() - cv.last_used < 0.5
    assert cv.use_count == 2

    ########

    cv = rpc._wrap("bar", pool__cache_ttl = None)

    assert cv.value == "bar"
    assert not cv.expired
    assert cv.ttl is None
    assert time() - cv.last_used < 0.5
    assert cv.use_count == 1

    sleep(1.5)

    assert not cv.expired
    assert cv.ttl is None
    assert time() - cv.last_used > 1.0
    assert cv.use_count == 1

    ###################################

    # test cache

    # basic expiration

    rpc = ResourcePoolCache("name", size = 3, policy = "lru")
    assert rpc.evict_period == ResourcePoolCache._default_evict_period

    assert rpc.name == "name"
    assert rpc.size == 3
    assert rpc.policy == "lru"

    with expected(KeyError("foo")):
        rpc["foo"]

    assert rpc.get("foo") is None
    assert rpc.get("foo", "default") == "default"

    rpc.put("foo", "bar", pool__cache_ttl = 1.0)
    assert rpc["foo"] == "bar"

    sleep(1.5)

    with expected(KeyError("foo")):
        rpc["foo"]

    rpc["biz"] = "baz"
    del rpc["biz"]

    with expected(KeyError("biz")):
        rpc["biz"]

    # test eviction

    def populate(policy):

        rpc = ResourcePoolCache("name", size = 3, policy = policy, default_ttl = 10.0, evict_period = 1.0)

        rpc[1] = 1
        rpc.put(2, 2, pool__cache_weight = 0.5)
        rpc.put(3, 3, pool__cache_weight = 0.25)
        rpc[4] = rpc._wrap(4, pool__cache_weight = 0.75)

        for i in range(4): assert rpc[1] == 1
        sleep(0.1)
        for i in range(3): assert rpc[2] == 2
        sleep(0.1)
        rpc[1]
        sleep(0.1)
        for i in range(2): assert rpc[3] == 3
        sleep(0.1)
        for i in range(1): assert rpc[4] == 4

        sleep(1.0)

        return rpc

    ###

    rpc = populate("lru")

    assert rpc[1] == 1
    with expected(KeyError(2)):
        rpc[2]
    assert rpc[3] == 3
    assert rpc[4] == 4

    ###

    rpc = populate("lfu")

    assert rpc[1] == 1
    assert rpc[2] == 2
    assert rpc[3] == 3
    with expected(KeyError(4)):
        rpc[4]

    ###

    rpc = populate("weight")

    assert rpc[1] == 1
    assert rpc[2] == 2
    with expected(KeyError(3)):
        rpc[3]
    assert rpc[4] == 4

    ###

    rpc = populate("old")

    with expected(KeyError(1)):
        rpc[1]
    assert rpc[2] == 2
    assert rpc[3] == 3
    assert rpc[4] == 4

    ###

    f = None

    for i in range(10):

        rpc = populate("random")
        for k in range(1, 5):
            try:
                rpc[k]
            except KeyError:
                if f is None:
                    f = k
                elif f != k:
                    break
        else:
            continue

        break

    else:
        assert False

    ###

    rpc = ResourcePoolCache("name", size = 1, policy = "old", evict_period = 1.0)
    assert rpc.default_ttl is None
    assert rpc.evict_period == 1.0

    rpc.put(1, rpc._wrap(1))
    rpc.put(2, rpc._wrap(2))

    assert len(rpc._cache) == 2

    rpc[1]; rpc[2]

    sleep(1.5)

    try:
        rpc[1]; rpc[2]
    except KeyError:
        pass

    assert len(rpc._cache) == 1

    # purging

    rpc = ResourcePoolCache("name", size = 3, policy = "lru", default_ttl = 10.0)

    rpc[1] = rpc._wrap(1, pool__cache_ttl = 1.0)
    rpc.put(2, 2, pool__cache_ttl = 1.0)
    rpc[3] = rpc._wrap("never expires 1", pool__cache_ttl = None)
    rpc.put(4, "never expires 2", pool__cache_ttl = None)
    assert len(rpc._cache) == 4

    rpc.purge()
    assert len(rpc._cache) == 4

    sleep(1.5)

    rpc.purge()
    assert len(rpc._cache) == 2

    # locking

    with rpc:
        rpc[1] = 1
        rpc[2] = 2

    # cached values are copied

    rpc = ResourcePoolCache("foo", size = 1)

    original_value = SQLRecord({ "mutable": "original value" })
    rpc["key"] = original_value

    original_value["mutable"] = "modified original value"
    assert original_value["mutable"] == original_value["MUTABLE"] == "modified original value"

    value1 = rpc["key"]
    assert isinstance(value1, SQLRecord)
    value2 = rpc["key"]
    assert isinstance(value2, SQLRecord)
    assert value1 is not value2 and value1._keys is not value2._keys and value1.data is not value2.data

    value1["mutable"] = "modified cached value"
    assert value1["mutable"] == value1["MUTABLE"] == "modified cached value"
    assert value2["mutable"] == value2["MUTABLE"] == "original value"

    # cached values iteration

    rpc = ResourcePoolCache("foo", size = 3)

    with expected(StopIteration):
        next(iter(rpc))

    rpc["key1"] = "foo"
    i = iter(rpc)
    rpc["key2"] = "bar"

    assert set(i) == { ("key1", "foo") }
    assert set(rpc) == { ("key1", "foo"), ("key2", "bar") }

    ###################################

    # test errors

    try:
        raise ResourceError()
    except ResourceError as e:
        assert str(e) == "" and e.recoverable == False and e.terminal == True
        assert e.code is None and e.description is None

    try:
        raise ResourceError(code = 1)
    except ResourceError as e:
        assert str(e) == "1"
        assert e.code == 1 and e.description is None

    try:
        raise ResourceError(description = "error")
    except ResourceError as e:
        assert str(e) == "error"
        assert e.code is None and e.description == "error"

    try:
        raise ResourceError(code = 1, description = "error")
    except ResourceError as e:
        assert str(e) == "1: error"
        assert e.code == 1 and e.description == "error"

    try:
        raise SQLResourceError()
    except SQLResourceError as e:
        assert str(e) == ""
        assert e.code is None and e.description is None and e.state is None

    try:
        raise SQLResourceError(code = 1, recoverable = False)
    except SQLResourceError as e:
        assert str(e) == "1" and e.recoverable == False
        assert e.code == 1 and e.description is None and e.state is None

    try:
        raise SQLResourceError(description = "error")
    except SQLResourceError as e:
        assert str(e) == "error"
        assert e.code is None and e.description == "error" and e.state is None

    try:
        raise SQLResourceError(code = 1, description = "error")
    except SQLResourceError as e:
        assert str(e) == "1: error" and e.participant_index is None
        assert e.code == 1 and e.description == "error" and e.state is None

    try:
        raise SQLResourceError(state = "P0001")
    except SQLResourceError as e:
        assert str(e) == "[P0001]"
        assert e.code is None and e.description is None and e.state == "P0001"

    try:
        raise SQLResourceError(code = 1, state = "P0001")
    except SQLResourceError as e:
        assert str(e) == "1: [P0001]"
        assert e.code == 1 and e.description is None and e.state == "P0001"

    try:
        raise SQLResourceError(description = "error", state = "P0001")
    except SQLResourceError as e:
        assert str(e) == "[P0001] error"
        assert e.code is None and e.description == "error" and e.state == "P0001"

    try:
        raise SQLResourceError(code = 1, description = "error", state = "P0001")
    except SQLResourceError as e:
        assert str(e) == "1: [P0001] error"
        assert e.code == 1 and e.description == "error" and e.state == "P0001"

    try:
        raise ResourceError(code = 1, description = "error", terminal = False, recoverable = True)
    except ResourceError as e:
        assert e.terminal == False and e.recoverable == True
        assert e.participant_index is None
        e.participant_index = 0
        assert e.participant_index == 0
        with expected(InputParameterError("_wparticipant_index() has got an incompatible "
                                          "value for participant_index: foo")):
            e.participant_index = "foo"

    try:
        raise TransactionExecutionError(description = "error", participant_index = 2)
    except TransactionExecutionError as e:
        assert str(e) == "error"
        assert e.participant_index == 2

    try:
        try:
            1 / 0
        except:
            ResourceError.rethrow(code = 123, participant_index = 0)
    except ResourceError as e:
        assert by_regex("^123: (?:int )?division (?:or modulo )?by zero$")(str(e))
        assert e.code == 123 and e.participant_index == 0
    else:
        assert False

    try:
        raise RPCError(description = "foo")
    except RPCError as e:
        assert e.description == "foo"

    ###################################

    # resources must be subclassed from Resource

    class NotAResource:
        def __init__(self, name):
            pass
        def connect(self):
            pass

    rp = ResourcePool("PoolName", NotAResource, 1)

    with expected(ReturnValueError("allocate() has returned an incompatible value: <__main__.NotAResource object")):
        rp.allocate()

    with expected(InputParameterError("release() has got an incompatible value for resource: <__main__.NotAResource object")):
        rp.release(NotAResource("foo"))

    ###################################

    class FooResource(Resource):

        def connect(self):
            sleep(random() * 0.1)

        def disconnect(self):
            sleep(random() * 0.1)

    ###################################

    # zero-sized pool will fail to allocate anything

    rp = ResourcePool("PoolName", FooResource, 0)

    with expected(ResourcePoolEmpty):
        rp.allocate()

    ###################################

    # resource instances name decoration

    rp = ResourcePool("PoolName", FooResource, 1)

    r = rp.allocate()
    assert r.name == "PoolName/1"
    rp.release(r)

    r = rp.allocate()
    assert r.name == "PoolName/1"
    r.expire()
    rp.release(r)

    r = rp.allocate()
    assert r.name == "PoolName/2"

    ###################################

    # resource reuse and overallocation

    rp = ResourcePool("PoolName", FooResource, 1)

    r1 = rp.allocate()
    rp.release(r1)

    r2 = rp.allocate()
    assert r2 is r1

    with expected(ResourcePoolEmpty):
        rp.allocate()

    ###################################

    # a pool can allocate an expired resource

    class BizResource(Resource):
        def connect(self):
            self.expire()

    rp = ResourcePool("PoolName", BizResource, 1)

    r1 = rp.allocate()
    assert r1.expired and rp.busy == 1
    rp.release(r1)
    assert rp.free == 0

    r2 = rp.allocate()
    assert r2 is not r1

    ###################################

    # the resources are reused in LIFO fashion

    rp = ResourcePool("PoolName", FooResource, 5)

    assert rp.free == 0 and rp.busy == 0

    r1 = rp.allocate()
    r2 = rp.allocate()
    r3 = rp.allocate()
    r4 = rp.allocate()
    r5 = rp.allocate()

    assert rp.free == 0 and rp.busy == 5

    rp.release(r4)
    rp.release(r3)
    rp.release(r5)
    rp.release(r1)
    rp.release(r2)

    assert rp.free == 5 and rp.busy == 0

    r2.expire()
    r3.expire()
    r5.expire()

    assert rp.free == 5 and rp.busy == 0

    assert rp.allocate() is r1

    assert rp.free == 3 and rp.busy == 1

    assert rp.allocate() is r4

    assert rp.free == 0 and rp.busy == 2

    assert rp.allocate() not in (r2, r3, r5)

    ###################################

    # stopped pool releases but does not allocate

    rp = ResourcePool("PoolName", FooResource, 2)

    r1 = rp.allocate()
    r2 = rp.allocate()

    rp.release(r1)

    assert rp.free == 1 and rp.busy == 1

    rp.stop().join()

    assert rp.free == 0 and rp.busy == 1

    rp.release(r2)

    assert rp.free == 0 and rp.busy == 0

    with expected(ResourcePoolStopped):
        rp.allocate()

    ###################################

    # sweeping disconnects expired resources

    rp = ResourcePool("PoolName", FooResource, 5)

    assert rp.free == 0 and rp.busy == 0

    r1 = rp.allocate()
    r2 = rp.allocate()
    r3 = rp.allocate()
    r4 = rp.allocate()
    r5 = rp.allocate()

    assert rp.free == 0 and rp.busy == 5

    rp.release(r4)
    rp.release(r3)
    rp.release(r5)
    rp.release(r1)
    rp.release(r2)

    assert rp.free == 5 and rp.busy == 0

    r2.expire()
    r3.expire()
    r5.expire()

    assert rp.free == 5 and rp.busy == 0

    rp.sweep().join()

    assert rp.free == 2 and rp.busy == 0

    ###################################

    # warming up keeps resources connected

    rp = ResourcePool("PoolName", FooResource, 5, 2)

    assert rp.free == 0 and rp.busy == 0

    rp.warmup().join()

    assert rp.free == 2 and rp.busy == 0

    r1 = rp.allocate()

    sleep(1.0) # warming up initiated in allocate is now underway

    assert rp.free == 2 and rp.busy == 1

    r2 = rp.allocate()
    r3 = rp.allocate()
    r4 = rp.allocate()

    sleep(1.0) # warming up initiated in allocate is now underway

    assert rp.free == 1 and rp.busy == 4

    assert rp.warmup() is None

    assert rp.free == 1 and rp.busy == 4

    r1.expire()

    rp.release(r1)
    rp.release(r3)

    sleep(1.0) # warming up initiated in release is now underway

    assert rp.free in (2, 3) and rp.busy == 2

    assert rp.warmup() is None

    assert rp.free in (2, 3) and rp.busy == 2

    r3.expire()

    rp.sweep().join() # note that sweeping also does warming up

    assert rp.free == 2 and rp.busy == 2

    assert rp.warmup() is None

    assert rp.free == 2 and rp.busy == 2

    ###################################

    # releasing an instance initiates warming up

    rp = ResourcePool("PoolName", FooResource, 1, 1)

    r1 = rp.allocate()

    r1.expire()
    rp.release(r1)

    sleep(1.0) # warming up initiated in release is now underway

    assert rp.free == 1 and rp.busy == 0

    ###################################

    # see how transactional resources are supposed to be used

    class BarResource(TransactionalResource): pass

    rp = ResourcePool("PoolName", BarResource, 1)

    r = rp.allocate()
    r.set_pool_info(rp.name, rp.size)
    assert r.pool_name == rp.name and r.pool_size == rp.size
    r.begin_transaction("xid", source_module_name = "module",
                        transaction_options = dict(foo = "bar"),
                        resource_args = (1, 2, 3), resource_kwargs = dict(biz = "baz"))
    assert r.xid == "xid"
    assert r.source_module_name == "module"
    assert r.transaction_options == dict(foo = "bar")
    assert r.resource_args == (1, 2, 3)
    assert r.resource_kwargs == dict(biz = "baz")
    r.commit()
    r.rollback()

    ###################################

    class FooTransactionalResource(TransactionalResource):

        _default_idle_timeout = 1.0
        _default_max_age = 3.5

    ftr = FooTransactionalResource("foores")

    assert ftr.min_time == 0.0
    ftr.set_min_time(1.0)
    assert ftr.min_time == 1.0

    assert ftr.max_time is None
    ftr.set_max_time(2.0)
    assert ftr.max_time == 2.0

    assert not ftr.expired    # +0.01 new
    sleep(0.6)                # +0.62
    assert not ftr.expired    # +0.63 fresh
    assert ftr.ttl <= 0.5     # +0.64
    sleep(0.6)                # +1.25
    assert ftr.expired        # +1.26 idle timeout
    assert ftr.ttl == 0.0     # +1.27

    ftr.reset_idle_timeout()  # +1.28

    assert not ftr.expired    # +1.29 fresh
    sleep(0.6)                # +1.90
    assert not ftr.expired    # +1.91 fresh
    assert ftr.ttl <= 0.5     # +1.92
    sleep(0.6)                # +2.53
    assert ftr.expired        # +2.54 idle timeout
    assert ftr.ttl == 0.0     # +2.55

    ftr.set_idle_timeout(0.5) # +2.56

    assert not ftr.expired    # +2.57 fresh
    sleep(0.6)                # +3.18
    assert ftr.expired        # +3.19 idle timeout
    assert ftr.ttl == 0.0     # +3.20

    ftr.reset_idle_timeout()  # +3.21

    assert not ftr.expired    # +3.22 fresh
    sleep(0.6)                # +3.83
    assert ftr.expired        # +3.84 idle and too old
    assert ftr.ttl == 0.0     # +3.85

    ftr.reset_idle_timeout()  # +3.86

    assert ftr.expired        # +3.87 too old
    assert ftr.ttl == 0.0     # +3.88

    ################################### and now for something completely different

    # connect() allocates another resource

    class Res(Resource):
        def connect(self):
            self._res = rp.allocate()

    # fails no matter what the size of the pool is

    rp = ResourcePool("PoolName", Res, 100)
    with expected(ResourcePoolEmpty("resource pool is empty")):
        rp.allocate()

    assert rp.free == 0 and rp.busy == 0

    ###################################

    # connect() releases itself

    class Res(Resource):
        def connect(self):
            rp.release(self)
        def disconnect(self):
            raise Exception("should not see me")

    rp = ResourcePool("PoolName", Res, 1)
    r = rp.allocate()

    assert rp.free == 1 and rp.busy == 0

    rp.release(r) # does nothing

    ###################################

    # connect() releases itself with expiration

    r_disconnected = Event()

    class Res(Resource):
        def connect(self):
            self.expire()
            rp.release(self)
        def disconnect(self):
            r_disconnected.set()

    rp = ResourcePool("PoolName", Res, 1)
    r = rp.allocate()

    assert rp.free == 0 and rp.busy == 0 and r_disconnected.is_set()

    rp.release(r) # does nothing

    ###################################

    # disconnect() releases itself

    class Res(Resource):
        def disconnect(self):
            rp.release(self)

    rp = ResourcePool("PoolName", Res, 1)
    r = rp.allocate()
    r.expire()

    # causes infinite recursion, but the exception is ignored in _disconnect

    rp.release(r)

    ###################################

    # see how pools are kept warm

    class WarmResource(Resource):
        def __init__(self, name, connect):
            Resource.__init__(self, name)
            self._connect = connect
        def connect(self):
            self._connect()

    RegisteredResourcePool.start_pools(3.0)
    try:

        sleep(1.5)
        warm_pool = RegisteredResourcePool("IdleWarmPool", lambda name: WarmResource(name, lambda: None), 5, 2)
        assert warm_pool.free == 0 and warm_pool.busy == 0

        sleep(3.0)
        assert warm_pool.free == 2 and warm_pool.busy == 0

        sleep(3.0)
        assert warm_pool.free == 2 and warm_pool.busy == 0

        r1 = warm_pool.allocate()

        sleep(1.0) # warming up initiated in allocate is now underway
        assert warm_pool.free == 2 and warm_pool.busy == 1
        sleep(2.0)

        warm_pool.release(r1)
        assert warm_pool.free == 3 and warm_pool.busy == 0

        sleep(3.0)
        assert warm_pool.free == 3 and warm_pool.busy == 0

    finally:
        RegisteredResourcePool.stop_pools()

    # see what happens if the resource fails to connect

    RegisteredResourcePool.start_pools(3.0)
    try:

        sleep(1.5)
        warm_pool = RegisteredResourcePool("FailWarmPool", lambda name: WarmResource(name, lambda: 1 / 0), 2, 2)
        assert warm_pool.free == 0 and warm_pool.busy == 0

        sleep(3.0)
        assert warm_pool.free == 0 and warm_pool.busy == 0

        sleep(3.0)
        assert warm_pool.free == 0 and warm_pool.busy == 0

    finally:
        RegisteredResourcePool.stop_pools()

    # see what happens if the resource hangs upon connection

    RegisteredResourcePool.start_pools(3.0)
    try:

        sleep(1.5)
        warm_pool = RegisteredResourcePool("HangWarmPool", lambda name: WarmResource(name, lambda: sleep(3600.0)), 3, 2)
        assert warm_pool.free == 0 and warm_pool.busy == 0

        sleep(3.0)
        assert warm_pool.free == 0 and warm_pool.busy == 1

        sleep(3.0)
        assert warm_pool.free == 0 and warm_pool.busy == 1

    finally:
        RegisteredResourcePool.stop_pools()
        RegisteredResourcePool._warmup_sem = Semaphore() # hack, to allow threads to enter _warmup again

    ###################################

    N = 50   # number of threads
    T = 30.0 # number of seconds

    class FailingProvider: # this class makes sure the pool never overcommits the resources

        _lock = Lock()
        _conn = 0

        @classmethod
        def connect(cls):

            sleep(random() * 0.25)
            if random() < 0.05:
                raise Exception("some exception")
            sleep(random() * 0.25)

            with cls._lock:
                assert cls._conn < N
                cls._conn += 1

        @classmethod
        def disconnect(cls):

            with cls._lock:
                cls._conn -= 1

            sleep(random() * 0.25)
            if random() < 0.1:
                raise Exception("some exception")
            sleep(random() * 0.25)

    p = FailingProvider()

    class FailingResource(Resource):

        def connect(self):
            p.connect()

        def disconnect(self):
            p.disconnect()

        def use(self):
            sleep(random() * 0.1)
            if random() < 0.05:
                self.expire()

    rp = ResourcePool("PoolName", FailingResource, N)

    def th_proc():
        t = Timeout(T)
        while not t.expired:
            try:
                r = rp.allocate()
            except Exception as e:
                assert str(e) == "some exception"
                continue
            try:
                r.use()
            finally:
                rp.release(r)

    ths = [ threading.Thread(target = th_proc) for i in range(N) ]

    for th in ths:
        th.start()

    # underway

    for th in ths:
        th.join()

    ###################################

    # registered resources are periodically swept

    RegisteredResourcePool.start_pools(0.5)
    try:

        class FooPool(RegisteredResourcePool):
            def __init__(self, *args, **kwargs):
                RegisteredResourcePool.__init__(self, *args, **kwargs)
                self._sweep_count = 0
            def _sweep(self):
                self._sweep_count += 1
                RegisteredResourcePool._sweep(self)

        foo_pool = FooPool("FooPool", Resource)

        class BarPool(RegisteredResourcePool):
            def __init__(self, *args, **kwargs):
                RegisteredResourcePool.__init__(self, *args, **kwargs)
                self._sweep_count = 0
            def _sweep(self):
                self._sweep_count += 1
                RegisteredResourcePool._sweep(self)

        bar_pool = BarPool("BarPool", Resource)

        sleep(2.0)

    finally:
        RegisteredResourcePool.stop_pools()

    assert foo_pool._sweep_count > 2 and bar_pool._sweep_count > 2
    assert foo_pool._stopped and bar_pool._stopped

    with expected(ResourcePoolStopped("all resource pools are stopped")):
        BarPool("BarPool", Resource)

    ###################################

    # sweeping can "overallocate" resources

    class SlowStopper(Resource):
        def disconnect(self):
            sleep(3.0)
            Resource.disconnect(self)

    RegisteredResourcePool.start_pools(1.0)
    try:

        class SlowStoppingPool(RegisteredResourcePool):
            pass

        rp = SlowStoppingPool("SlowStoppingPool", SlowStopper, 1)

        assert rp.free == 0 and rp.busy == 0
        r1 = rp.allocate()
        assert rp.free == 0 and rp.busy == 1
        rp.release(r1)
        assert rp.free == 1 and rp.busy == 0
        r1.expire()
        sleep(2.0)
        assert rp.free == 0 and rp.busy == 1 # the only resource is being disconnected
        with expected(ResourcePoolEmpty("resource pool is empty")):
            rp.allocate()
        sleep(2.0)

    finally:
        RegisteredResourcePool.stop_pools()

    ###################################

    # warming up can "overallocate" resources

    class SlowStarter(Resource):
        def connect(self):
            Resource.connect(self)
            sleep(3.0)

    RegisteredResourcePool.start_pools(1.0)
    try:

        class SlowStartingPool(RegisteredResourcePool):
            pass

        rp = SlowStartingPool("SlowStartingPool", SlowStarter, 1, 1)

        assert rp.free == 0 and rp.busy == 0
        sleep(2.0)
        assert rp.free == 0 and rp.busy == 1 # the only resource is being connected
        with expected(ResourcePoolEmpty("resource pool is empty")):
            rp.allocate()
        sleep(2.0)

    finally:
        RegisteredResourcePool.stop_pools()

    ###################################

    od = OrderedDict() # such complex initialization is required to ensure
    od["foo"] = "bar"  # that Biz is seen by SQLRecord's __init__ after BIz
    od["BIz"] = 123
    od["Biz"] = None

    r = SQLRecord(od)

    # the original case is preserved, but keys case-insensitively identical are coalesced

    assert len(r) == 2
    assert r == { "foo": "bar", "Biz": None }

    assert "foo" in r and "Foo" in r and "FOO" in r
    assert "biz" in r and "Biz" in r and "BIZ" in r

    assert r["foo"] == r["Foo"] == r["FOO"] == "bar"
    assert r["biz"] == r["Biz"] == r["BIZ"] == None

    assert r.get("foo") == r.get("Foo") == r.get("FOO") == "bar"
    assert r.get("biz") == r.get("Biz") == r.get("Biz") == None

    with expected(KeyError):
        r["baz"]

    del r["FOO"]
    with expected(KeyError):
        del r["FOO"]
    assert len(r) == 1

    assert "foo" not in r and "Foo" not in r and "FOO" not in r
    assert r.get("foo") == r.get("Foo") == r.get("FOO") == None

    assert r.pop("biz") is None
    with expected(KeyError):
        r.pop("biz")
    assert r.pop("biz", None) is None
    assert len(r) == 0

    assert str(r) == repr(r) == "{}"

    r["foo"] = 1
    assert str(r) == repr(r) == "{'foo': 1}"
    assert r["foo"] == r["fOo"] == 1

    r["fOo"] = 2
    assert str(r) == repr(r) == "{'fOo': 2}"
    assert r["foo"] == r["fOo"] == 2

    assert list(r.keys()) == [ "fOo" ]
    assert list(r.values()) == [ 2 ]
    assert list(r.items()) == [ ("fOo", 2) ]

    del r["foo"]
    assert not r

    ###################################

    class SomeSQLResource(SQLResource):
        def _execute_sql(self, sql, params):
            return eval(sql.format(**params))

    sqlr = SomeSQLResource("sqlres", decimal_precision = (5, 2))

    assert sqlr.precision == 5
    assert sqlr.scale == 2

    # basic behaviour and parameter ordering

    assert sqlr.execute() == ()
    assert sqlr.execute("[]") == ([],)
    assert sqlr.execute("[]", "[]") == ([], [])
    assert sqlr.execute("[{{1:2}}]") == ([{"1":2}],)
    assert sqlr.execute("[{{'foo': 'bar'}}]", notused = "123") == ([{"foo":"bar"}],)

    assert sqlr.execute("[dict(i={i})]", i=123) == ([{"i":123}],)
    assert sqlr.execute("[dict(i={i}, j={i})]", i=123) == ([{"i":123, "j":123}],)
    assert sqlr.execute("[dict(i={i}), dict(i={i})]", i=123) == ([{"i":123}, {"i":123}],)
    assert sqlr.execute("[dict(i={i1}), dict(i={i2})]", i1=123, i2=456) == ([{"i":123}, {"i":456}],)

    assert sqlr.execute("[dict(i={i})]", "[dict(i={i})]", i = 123) == ([{"i":123}], [{"i":123}])
    assert sqlr.execute("[dict(i={i})]", "[dict(i={j})]", i = 0, j = 1) == ([{"i":0}], [{"i":1}])
    assert sqlr.execute("[dict(i={i1}, j={j1})]", "[]", "[dict(i={i2}), dict(j={j2})]",
                       i1 = 1, i2 = 2, j1 = 3, j2 = 4) == ([{"i":1, "j":3}], [], [{"i":2}, {"j":4}])

    # NoneType

    assert sqlr.execute("[dict(n={n})]", n = None) == ([{"n":None}],)

    # int

    assert sqlr.execute("[dict(i={i})]", i = -9223372036854775808) == ([{"i":-9223372036854775808}],)
    assert sqlr.execute("[dict(i={i})]", i = 9223372036854775807) == ([{"i":9223372036854775807}],)
    with expected(_BaseError("integer value too large")):
        sqlr.execute("", i = -9223372036854775809)
    with expected(_BaseError("integer value too large")):
        sqlr.execute("", i = 9223372036854775808)
    with expected(_BaseError("integer value too large")):
        sqlr.execute("[dict(i=-9223372036854775809)]")
    with expected(_BaseError("integer value too large")):
        sqlr.execute("[dict(i=9223372036854775808)]")

    # Decimal

    assert sqlr.execute("[dict(d=Decimal('{d}'))]", d = Decimal("1.0")) == ([{"d":Decimal("1.0")}],)
    assert sqlr.execute("[dict(d=Decimal('{d}'))]", d = Decimal("999.99")) == ([{"d":Decimal("999.99")}],)
    assert sqlr.execute("[dict(d=Decimal('{d}'))]", d = Decimal("-999.99")) == ([{"d":Decimal("-999.99")}],)

    with expected(_BaseError("decimal value too large")):
        sqlr.execute("", d = Decimal("1000.0"))
    with expected(_BaseError("decimal value too large")):
        sqlr.execute("", d = Decimal("-1000.0"))
    with expected(_BaseError("decimal value too precise")):
        sqlr.execute("", d = Decimal("0.001"))
    with expected(_BaseError("decimal value too precise")):
        sqlr.execute("", d = Decimal("-0.001"))

    with expected(_BaseError("decimal value too large")):
        sqlr.execute("[dict(d=Decimal('1000.0'))]")
    with expected(_BaseError("decimal value too large")):
        sqlr.execute("[dict(d=Decimal('-1000.0'))]")
    with expected(_BaseError("decimal value too precise")):
        sqlr.execute("[dict(d=Decimal('0.001'))]")
    with expected(_BaseError("decimal value too precise")):
        sqlr.execute("[dict(d=Decimal('-0.001'))]")

    # bool

    assert sqlr.execute("[dict(b={b})]", b = True) == ([{"b":True}],)
    assert sqlr.execute("[dict(b={b})]", b = False) == ([{"b":False}],)

    # datetime

    dt = datetime.now()
    assert sqlr.execute("[dict(dt=datetime.strptime('{dt}', '%Y-%m-%d %H:%M:%S.%f'))]", dt = dt) == ([{"dt":dt}],)

    # str

    assert sqlr.execute("[dict(s='{s}')]", s = "foo") == ([{"s":"foo"}],)
    assert sqlr.execute("[dict(empty='{s}')]", s = "") == ([{"empty":""}],)

    # bytes

    assert sqlr.execute("[dict(b={b})]", b = b"bar") == ([{"b":b"bar"}],)
    assert sqlr.execute("[dict(empty={b})]", b = b"") == ([{"empty":b""}],)

    # unsupported type

    with expected(_BaseError("type float is not supported")):
        sqlr.execute("", f = 1.0)
    with expected(_BaseError("type float cannot be converted")):
        sqlr.execute("[dict(f=1.0)]")

    class SomeType:
        pass

    with expected(_BaseError("type SomeType is not supported")):
        sqlr.execute("", f = SomeType())
    with expected(_BaseError("type SomeType cannot be converted")):
        sqlr.execute("[{{None:SomeType()}}]")

    # custom + continuous conversion

    class TypeWrapper:
        def __init__(self, value):
            self._value = value
        value = property(lambda self: self._value)

    class OtherSQLResource(SQLResource):
        def _execute_sql(self, sql, params):
            return eval(sql.format(**params))
        def _sql_to_py_TypeWrapper(self, v):
            return v.value

    osqlr = OtherSQLResource("osqlres", decimal_precision = (3, 1))

    assert osqlr.execute("[{{None:TypeWrapper(Decimal('0.0'))}}]") == ([{"None":Decimal("0.0")}],)
    assert osqlr.execute("[{{None:TypeWrapper(Decimal('99.9'))}}]") == ([{"None":Decimal("99.9")}],)
    assert osqlr.execute("[{{None:TypeWrapper(Decimal('-99.9'))}}]") == ([{"None":Decimal("-99.9")}],)

    with expected(_BaseError("decimal value too large")):
        osqlr.execute("[{{None:TypeWrapper(Decimal('100.0'))}}]")
    with expected(_BaseError("decimal value too large")):
        osqlr.execute("[{{None:TypeWrapper(Decimal('-100.0'))}}]")
    with expected(_BaseError("decimal value too precise")):
        osqlr.execute("[{{None:TypeWrapper(Decimal('0.01'))}}]")
    with expected(_BaseError("decimal value too precise")):
        osqlr.execute("[{{None:TypeWrapper(Decimal('-0.01'))}}]")

    assert osqlr.execute("[{{0:TypeWrapper(0)}}]") == ([{"0":0}],)
    assert osqlr.execute("[{{1:TypeWrapper(2**63-1)}}]") == ([{"1":2**63-1}],)
    assert osqlr.execute("[{{2:TypeWrapper(-2**63)}}]") == ([{"2":-2**63}],)
    with expected(_BaseError("integer value too large")):
        osqlr.execute("[{{1:TypeWrapper(2**63)}}]")
    with expected(_BaseError("integer value too large")):
        osqlr.execute("[{{1:TypeWrapper(-2**63-1)}}]")

    # unsupported conversion

    with expected(_BaseError("type float cannot be converted")):
        osqlr.execute("[{{None:TypeWrapper(1.0)}}]")

    # sql resource input parameter checking

    with expected(InputParameterError("_execute() has got an incompatible "
                                      "value for sql: b'should be str'")):
        sqlr.execute("[]", b"should be str")

    ###################################

    print("ok")

################################################################################
# EOF
