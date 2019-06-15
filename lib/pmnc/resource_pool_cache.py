#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements caching for resource pools.
#
# Any resource pool can be instrumented with a read cache. Its behaviour
# is controlled by the following configuration parameters in the resource
# configuration file:
#
# >> pool__cache_size = N,
# Maximum number of cached entries to keep in the cache.
#
# As soon as you specify pool__cache_size for a resource, a cache is enabled
# and all identical requests will return the cached results. By default, "identical"
# are two resource requests with identical methods, args and kwargs:
#
# xa.resource.foo(1, biz = "baz") # original
# xa.resource.foo(1, biz = "baz") # identical
# xa.resource.bar(1, biz = "baz") # different
# xa.resource.foo(2, biz = "baz") # different
# xa.resource.foo(1, baz = "biz") # different
# xa.resource.foo(1, biz = "buz") # different
#
# essentially ( "foo", 1, { biz: "baz" } ) is a default cache key.
#
# >> pool__cache_policy = "...",
# Eviction policy, string literal, one of "lru", "lfu", "weight", "useless", "old" or "random":
# lru = evict entries least recently used
# lfu = evict entries least frequently used
# weight = evict entries that took least time to create (cheapest to regenerate)
# useless = evict entries that saved least time upon cache hits
# old = evict entries that are to expire soon (FIFO-like)
# random = evict entries at random
#
# >> pool__cache_default_ttl = N.N,
# Default cached entry time to live in float seconds. None (default) means forever.
#
# >> pool__cache_evict_period = N.N,
# Eviction can occur at most once in N.N float seconds. Default is 10.0 seconds.
#
# >> pool__cache_group_interval = N.N,
# Group weight statistics will be accumulated over last N.N seconds.
# With this option turned on, values are evicted from the cache based
# on the total valuability of the entire group they belong to, which
# is the average weight per value saved upon cache hits. This way few
# values with a lot of hits may save not just themself from being evicted
# but also all of their group members. Within groups chosen for eviction
# the policy order is maintained on individual value basis.
# See pool__cache_group below.
#
# You can control the cache behaviour by supplying the following kwargs
# to the resource call (see transaction.py for more transaction-specific
# kwargs):
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_key = "key")
# The result of this particular call is cached under this literal key,
# thus changing the above "identical" semantics. As an alternative,
# a function could be passed for pool__cache_key, which then in turn
# returns the actual key value. Passing None for a key makes the result
# to not be cached.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_ttl = M.M)
# The result of this particular call is cached for M.M seconds,
# explicitly passed None means forever.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_weight = K.K)
# The result of this particular call is marked with weight K.K,
# which is used with "weight" eviction policy and group weight
# accounting.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_group = some_value)
# The result of this call will be registered as belonging to such group
# and upon hits to this cached value its weight will be accounted
# for the group making all of its values more valuable and less prone
# to eviction.
#
# The following two kwargs control read/write concurrency semantics.
# Any request may be either a read or write but not both. Write
# requests are write-through and not cached. Only read requests'
# results are cached. To detect which write requests conflict and
# invalidate which read requests' cached results, each request
# declares the set of "keys" it accesses.
#
# >> xa.resource.db("SELECT * FROM t WHERE k = {k}", k = 123,
#                   pool__cache_read_keys = { "t", "t/123" })
# Set (or other iterable) of strings containing keys this request reads.
#
# >> xa.resource.db("UPDATE t SET c = c + 1 WHERE k = {k}", k = 123,
#                   pool__cache_write_keys = { "t/123" })
# >> xa.resource.db("DELETE FROM t",
#                   pool__cache_write_keys = { "t" })
# Set (or other iterable) of strings containing keys this request writes.
#
# As you can see, this approach is suitable unique key access,
# but problematic for range key access.
#
# Pythomnic3k project
# (c) 2005-2015, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "ResourcePoolCache", "ResourcePoolReadWriteCache" ]

###############################################################################

import threading; from threading import Lock, Event
import time; from time import time
import copy; from copy import deepcopy
import heapq; from heapq import nsmallest
import random; from random import randint

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import typecheck, optional, one_of
import interlocked_counter; from interlocked_counter import InterlockedCounter
import interlocked_queue; from interlocked_queue import InterlockedQueue
import pmnc.timeout; from pmnc.timeout import Timeout

###############################################################################

_positive_int = lambda i: isinstance(i, int) and i > 0
_non_negative_float = lambda f: isinstance(f, float) and f >= 0.0

###############################################################################
# built-in hash is undeterministic, djb2 is used instead for strings keys

def _hash(s: str) -> int:
    h = 5381
    for c in s:
        h = (h * 33 + ord(c)) & 0xffffffff
    return h

###############################################################################
# this class collects statistics on cache performance of a group of keys,
# which is an average time saved on hits of the group cache entries,
# over the given time in the past

class CacheGroupCounter:

    @typecheck
    def __init__(self, interval: _non_negative_float):
        self._interval = interval
        self._lock = Lock()
        self._queue = []
        self._data = {}
        self._sum = 0.0
        self._group_weight = 0.0

    group_weight = property(lambda self: self._rgroup_weight())

    def _rgroup_weight(self):
        with self._lock:
            self._trim()
            return self._group_weight

    def _trim(self):
        t = time()
        while self._queue:
            d, k = self._queue[0]
            if t < d:
                break
            self._queue.pop(0)
            w, c = self._data[k]
            if c > 1:
                self._data[k] = (w, c - 1)
            else:
                del self._data[k]
            self._sum -= w
            self._group_weight = self._sum / (len(self._data) or 1)

    def trim(self):
        with self._lock:
            self._trim()
            return len(self._data) == 0

    def hit(self, cv):
        d = time() + self._interval
        k = cv.key
        w = cv.weight or 0.0 # no weight is accounted as zero weight
        with self._lock:
            w_c = self._data.get(k)
            if w_c:
                assert w_c[0] == w
                self._data[k] = (w, w_c[1] + 1)
            else:
                self._data[k] = (w, 1)
            self._queue.append((d, k))
            self._sum += w
            self._group_weight = self._sum / (len(self._data) or 1)
            self._trim()

###############################################################################

class CachedValue: # instances of this class contain cached values

    _instance_count = InterlockedCounter() # we need to count cached values
                                           # so that they have unique ids

    def __init__(self, value, *, ttl = None, weight = None, group_counter = None):

        self._value = value

        self.key = self._instance_count.next()

        self._timeout = Timeout(ttl) if ttl else None
        self.weight = weight
        self._group_counter = group_counter

        self.hit_count = -1 # after call to touch becomes 0
        self.touch()

    value = property(lambda self: self._value)
    expired = property(lambda self: self._timeout.expired if self._timeout is not None else False)
    ttl = property(lambda self: self._timeout.remain if self._timeout is not None else None)
    group_weight = property(lambda self: self._group_counter.group_weight if self._group_counter else None)

    def touch(self):
        self.last_used = time() # note that usage has nothing to do with expiration
        self.hit_count += 1
        if self.hit_count > 0 and self._group_counter: # cache hit, register saved time
            self._group_counter.hit(self)

###############################################################################

class ResourcePoolCache:

    _default_evict_period = 10.0

    @typecheck
    def __init__(self, name: str, *,
                 size: optional(_positive_int) = None,
                 policy: optional(one_of("lru", "lfu", "weight", "useless", "old", "random")) = None,
                 default_ttl: optional(_non_negative_float) = None,
                 evict_period: optional(_non_negative_float) = None,
                 group_interval: optional(_non_negative_float) = None):

        self._name = name
        self._size = size # can be None meaning unrestricted
        self._policy = policy or "lru"
        self._default_ttl = default_ttl # can be None meaning unspecified

        self._evict_period = evict_period or self._default_evict_period
        self._evict_timeout = Timeout(self._evict_period)
        self._evict_order = getattr(self, "_evict_{0:s}".format(self._policy))

        self._group_interval = group_interval
        self._group_counters = {} if group_interval else None

        self._lock, self._cache = Lock(), {}

    name = property(lambda self: self._name)
    size = property(lambda self: self._size)
    policy = property(lambda self: self._policy)
    default_ttl = property(lambda self: self._default_ttl)
    evict_period = property(lambda self: self._evict_period)
    group_interval = property(lambda self: self._group_interval)

    # utility method to set caching parameters on per-value basis

    def _evict(self):

        if not self._size: # cache size is unrestricted
            return

        if self._evict_timeout.expired:
            try:

                evict_count = len(self._cache) - self._size
                if evict_count <= 0:
                    return

                now = time() # to have a uniform view of time and avoid additional calls

                if not self._group_counters: # entries are evicted on individual basis
                    evicted_items = nsmallest(evict_count, self._cache.items(),
                        key = lambda k_cv: self._evict_order(k_cv[1], now, 1.0))
                else: # entries are evicted accounting their group values
                    total_weight = sum(gc.group_weight for gc in self._group_counters.values()) or 1.0
                    evicted_items = nsmallest(evict_count, self._cache.items(),
                        key = lambda k_cv: self._evict_order(k_cv[1], now, k_cv[1].group_weight / total_weight))

                for k, cv in evicted_items:
                    del self._cache[k]

            finally:
                self._evict_timeout.reset()

    # the following methods are comparison keys which return smaller values for preferred eviction,
    # gw is a group weight correcting coefficient in 0-1 range with higher values for higher-ranked
    # groups

    # least recently used, remove entries that have not been used for a while

    def _evict_lru(self, cv, now, gw):
        return (cv.last_used - now) * gw

    # least frequently used, remove entries that have seldom been used

    def _evict_lfu(self, cv, now, gw):
        return cv.hit_count * gw

    # remove cheapest entries (that it took least time to generate),
    # note that values with unspecified weight get removed first

    def _evict_weight(self, cv, now, gw):
        return (cv.weight or 0.0) * gw

    # remove useless entries (that saved least time upon hits),
    # note that values with unspecified weight get removed first

    def _evict_useless(self, cv, now, gw):
        return (cv.weight or 0.0) * cv.hit_count * gw

    # remove entries that are to expire soon, note that values that never expire get removed last

    def _evict_old(self, cv, now, gw):
        return (cv.ttl * gw) if cv.ttl is not None else float("inf")

    # remove entries at random

    def _evict_random(self, cv, now, gw):
        return randint(0, 2147483647) * gw

    # protocol method

    def _get(self, k):
        self._evict()
        cv = self._cache.get(k)
        if cv is None:
            return None
        if cv.expired:
            del self._cache[k]
            return None
        cv.touch()
        return self._unwrap_value(cv)

    def get(self, k):
        with self._lock:
            return self._get(k)

    def __getitem__(self, k):
        v = self.get(k)
        if v is None:
            raise KeyError(k)
        return v

    # protocol method

    def _contains(self, k):
        self._evict()
        return k in self._cache

    def __contains__(self, k):
        with self._lock:
            return self._contains(k)

    # protocol method

    def _put(self, k, v, kwargs):
        assert v is not None
        self._evict()
        self._cache[k] = self._wrap_value(v, kwargs)

    def put(self, k, v, **kwargs):
        with self._lock:
            self._put(k, v, kwargs)

    __setitem__ = put

    # protocol method

    def _pop(self, k):
        self._evict()
        cv = self._cache.pop(k, None)
        if cv is not None:
            return self._unwrap_value(cv)

    def pop(self, k):
        with self._lock:
            return self._pop(k)

    def __delitem__(self, k):
        if self.pop(k) is None:
            raise KeyError(k)

    # utility methods to convert values

    def _wrap_value(self, v, kwargs):

        ttl = kwargs.get("pool__cache_ttl", self._default_ttl)
        weight = kwargs.get("pool__cache_weight")
        group = kwargs.get("pool__cache_group")

        if self._group_counters is not None:
            group_counter = self._group_counters.get(group)
            if not group_counter:
                group_counter = CacheGroupCounter(self._group_interval)
                self._group_counters[group] = group_counter
        else:
            group_counter = None

        return CachedValue(deepcopy(v),
                           ttl = ttl, weight = weight,
                           group_counter = group_counter)

    def _unwrap_value(self, cv):

        return deepcopy(cv.value)

    # called by the maintenance thread periodically

    def _purge(self):
        self._cache = { k: cv for k, cv in self._cache.items() if not cv.expired }
        self._evict()
        if self._group_counters:
            self._group_counters = { g: gc for g, gc in self._group_counters.items() if not gc.trim() }

    def purge(self):
        with self._lock:
            self._purge()

################################################################################

class KeySet:

    def __init__(self):
        self._lock = Lock()
        self._keys = {}

    # adds multiple key -> cache_key references

    def add_key(self, keys, cache_key):
        with self._lock:
            for k in keys:
                s = self._keys.get(k)
                if s is None:
                    s = set()
                    self._keys[k] = s
                s.add(cache_key)

    # returns a set of cache_key's for a given set of keys,
    # the references are discarded as they are returned

    def pop_conflicting_keys(self, keys):
        cache_keys = set()
        with self._lock:
            for k in keys:
                s = self._keys.pop(k, None)
                if s is not None:
                    cache_keys |= s
        return cache_keys

    # filters out cache keys that are no longer valid for whatever reason

    def revalidate(self, valid_cache_key):
        with self._lock:
            keys = {}
            for k, s in self._keys.items():
                s = set(filter(lambda cache_key: valid_cache_key(cache_key), s))
                if s: keys[k] = s
            self._keys = keys

################################################################################

class ResourcePoolReadWriteCache(ResourcePoolCache): # transaction-aware descendant
                                                     # for read/write dependency tracking
    def __init__(self, *args, invalidated_keys: optional(InterlockedQueue) = None, **kwargs):

        ResourcePoolCache.__init__(self, *args, **kwargs)
        self._xacts = {}
        self._cached = KeySet()
        self._read_reqs = {}
        self._write_reqs = {}
        self._invalidated_keys = invalidated_keys

    def _extract_kwargs(self, kwargs):

        xid = kwargs["pool__cache_transaction_id"]
        read_keys = kwargs.get("pool__cache_read_keys")
        if read_keys:
            read_keys = set(_hash(s) for s in read_keys)
        write_keys = kwargs.get("pool__cache_write_keys")
        if write_keys:
            write_keys = set(_hash(s) for s in write_keys)
        assert read_keys is None or write_keys is None # can't specify both read and write keys

        return xid, read_keys, write_keys

    def get(self, key, **kwargs):

        xid, read_keys, write_keys = self._extract_kwargs(kwargs)

        # write requests are not cached at all, but have invalidating effect on
        # all conflicting read requests that are being executed at the same time

        if write_keys is not None:
            with self._lock:
                self._write_reqs[xid] = write_keys
                self._invalidate_read_reqs(write_keys)
            return None

        # read requests are filtered against concurrent write request so as not
        # to cache results returned by requests executed in a conflict situation

        if read_keys is not None:
            with self._lock:
                for write_keys_ in self._write_reqs.values():
                    if write_keys_.intersection(read_keys):
                        break
                else:
                    self._read_reqs[xid] = read_keys # this request currently has no conflicts

        # read requests are executed so that only one request can proceed
        # for any given cache key, the others will wait and get the cached
        # result once the first request returns

        timeout = Timeout(kwargs["pool__cache_timeout"])

        while True: # this is a do-while loop as we need at least one pass
            with self._lock:
                value = self._get(key)
                if value is not None:
                    return value # always returns a cached value
                e_xid = self._xacts.get(key)
                if not e_xid:                         # register this transaction as the one
                    self._xacts[key] = (Event(), xid) # responsible for actually executing the call
                    return None
            e_xid[0].wait(timeout.remain)             # and the others will wait for it
            if timeout.expired:
                return Timeout # timed out, do not attempt to execute

    # the following method caches and registers results of read
    # transactions, releases transactions waiting in get, invalidates
    # cached read against conflicting write transactions

    def put(self, key, value, **kwargs):

        xid, read_keys, write_keys = self._extract_kwargs(kwargs)

        # write request has returned, its invalidating effect is raised,
        # but all the conflicting cached values are momentarily removed

        if write_keys is not None:
            with self._lock:
                if self._invalidated_keys is not None: # notify the owner
                    self._invalidated_keys.push(write_keys)
                self._invalidate(write_keys)
                del self._write_reqs[xid]
            return

        # read request has returned, its result is cached if
        # 1. the request did not fail and returned a brand new previously uncached value
        # 2. the request has not been invalidated while it has been executing

        # note that a value of None is an indication of failure and cannot be cached,
        # therefore caching should not be enabled on resources which can return None
        # as a valid execution result

        with self._lock:

            read_keys_ = self._read_reqs.pop(xid, None) if read_keys is not None else None
            cache_value = (value is not None) and ((read_keys is None) or (read_keys_ is not None))

            e_xid = self._xacts.get(key)
            if e_xid:
                if e_xid[1] == xid: # this request has been executing the actual resource call
                    del self._xacts[key]
                    if cache_value:
                        assert not self._contains(key)
                        self._put(key, value, kwargs)
                    e_xid[0].set()
                else: # this request has timed out waiting for result to appear in the cache
                    assert value is None
            else: # this request got the cached result
                assert value is None

            # no matter what was the outcome of the request, we keep track of the dependencies

            if read_keys is not None:
                self._cached.add_key(read_keys, key)

    # the owner instructs to invalidate such write keys because of external event,
    # it has the same effect as an infinitely fast write request with the same set of keys

    def invalidate(self, write_keys):
        write_keys = set(_hash(s) for s in write_keys)
        with self._lock:
            self._invalidate_read_reqs(write_keys)
            self._invalidate(write_keys)

    # this is a local invalidation due to completed write request

    def _invalidate(self, write_keys):
        for cache_key in self._cached.pop_conflicting_keys(write_keys):
            self._pop(cache_key)

    # remove conflicting read requests currently being executed from
    # the list so that their results are not cached when they return

    def _invalidate_read_reqs(self, write_keys):
        self._read_reqs = \
            dict(filter(lambda xid_read_keys: xid_read_keys[1].isdisjoint(write_keys),
                        self._read_reqs.items()))

    # remove all the cache keys that are not actually in the cache

    def purge(self):
        with self._lock:
            self._purge()
            self._cached.revalidate(lambda cache_key: self._contains(cache_key))

################################################################################

if __name__ == "__main__":

    print("self-testing module resource_pool_cache.py:")

    ###################################

    from time import sleep
    from random import random, normalvariate
    from expected import expected
    from threading import Thread

    ###################################

    # test cached value

    rpc = ResourcePoolCache("name", size = 1, policy = "lru", default_ttl = 10.0)

    ########

    cv = rpc._wrap_value("biz", {})

    assert cv.value == "biz"
    assert cv.weight is None
    assert not cv.expired
    assert cv.ttl > 9.0
    assert time() - cv.last_used < 0.5
    assert cv.hit_count == 0
    assert cv.key == 0

    sleep(1.5)

    assert not cv.expired
    assert cv.value == "biz"
    assert cv.weight is None
    assert cv.ttl > 7.0
    assert time() - cv.last_used < 2.5
    assert cv.hit_count == 0
    assert cv.key == 0

    ########

    cv = rpc._wrap_value("foo", dict(pool__cache_ttl = 1.0, pool__cache_weight = 0.21))

    assert cv.value == "foo"
    assert cv.weight == 0.21
    assert not cv.expired
    assert 0.5 < cv.ttl < 1.5
    assert time() - cv.last_used < 0.5
    assert cv.hit_count == 0
    assert cv.key == 1

    sleep(1.5)

    assert cv.expired
    assert cv.value == "foo"
    assert cv.weight == 0.21
    assert cv.ttl == 0.0
    assert time() - cv.last_used > 1.0
    assert cv.hit_count == 0
    assert cv.key == 1

    cv.touch()

    assert cv.expired
    assert time() - cv.last_used < 0.5
    assert cv.hit_count == 1

    ########

    cv = rpc._wrap_value("bar", dict(pool__cache_ttl = None))

    assert cv.value == "bar"
    assert not cv.expired
    assert cv.ttl is None
    assert time() - cv.last_used < 0.5
    assert cv.hit_count == 0

    sleep(1.5)

    assert not cv.expired
    assert cv.ttl is None
    assert time() - cv.last_used > 1.0
    assert cv.hit_count == 0

    ###################################

    # cache group count

    cgc = CacheGroupCounter(1.0)
    cv1 = rpc._wrap_value("foo", dict(pool__cache_weight = 1.0))
    cv2 = rpc._wrap_value("foo", dict(pool__cache_weight = 2.0))

    cgc.hit(cv1)
    assert cgc.group_weight == 1.0 / 1

    sleep(0.2)

    cgc.hit(cv1)
    assert cgc.group_weight == (1.0 + 1.0) / 1

    sleep(0.2)

    cgc.hit(cv2)
    assert (1.0 + 1.0 + 2.0) / 3

    sleep(0.2)

    cgc.hit(cv2)
    assert cgc.group_weight == (1.0 + 1.0 + 2.0 + 2.0) / 2

    sleep(0.5)

    cgc.hit(cv2)
    assert cgc.group_weight == (1.0 + 2.0 + 2.0 + 2.0) / 2

    sleep(1.2)

    assert cgc.group_weight == 0.0

    ###################################

    # cache groups

    # this cache doesn't support groups at all therefore nothing happens

    rpc = ResourcePoolCache("name", size = 1)
    assert rpc._group_counters is None

    cv = rpc._wrap_value("foo", dict(pool__cache_group = "grp"))
    assert rpc._group_counters is None

    cv.touch()

    # and this cache does therefore groups counters are available

    rpc = ResourcePoolCache("name", size = 1, group_interval = 1.0)
    assert rpc._group_counters == {}

    cv1 = rpc._wrap_value("foo", dict(pool__cache_weight = 1.0, pool__cache_group = "grp"))
    assert "grp" in rpc._group_counters

    gc = rpc._group_counters["grp"]
    assert gc.group_weight == 0.0

    cv2 = rpc._wrap_value("foo", dict(pool__cache_weight = 2.0, pool__cache_group = "grp"))
    assert rpc._group_counters["grp"] is gc
    assert gc.group_weight == 0.0

    cv1.touch()
    assert gc.group_weight == 1.0

    cv3 = rpc._wrap_value("foo", dict(pool__cache_weight = 3.0, pool__cache_group = "GRP"))
    assert "GRP" in rpc._group_counters
    gc2 = rpc._group_counters["GRP"]
    assert gc2 is not gc
    assert gc2.group_weight == 0.0

    cv3.touch()
    assert gc2.group_weight == 3.0

    cv2.touch()
    assert gc.group_weight == 1.5

    sleep(1.5)

    assert gc.group_weight == gc2.group_weight == 0.0

    # note that cached values may outlive the effect they had on statistics

    # now see what happens if the value lacks group

    cv4 = rpc._wrap_value("foo", dict(pool__cache_weight = 5.0))
    assert None in rpc._group_counters # None = unspecified default group

    gc3 = rpc._group_counters[None]
    assert gc3.group_weight == 0.0

    cv4.touch()
    assert gc3.group_weight == 5.0

    # or weight

    cv3.touch()
    assert gc2.group_weight == 3.0

    cv5 = rpc._wrap_value("foo", dict(pool__cache_group = "GRP"))

    cv5.touch()
    assert gc2.group_weight == 1.5 # no weight is accounted as zero weight

    sleep(1.5)

    assert len(rpc._group_counters) == 3
    rpc.purge()
    assert len(rpc._group_counters) == 0

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

    def populate(policy, size = 3):

        rpc = ResourcePoolCache("name", size = size, policy = policy,
                                default_ttl = 10.0, evict_period = 1.0)

        rpc[1] = 1
        rpc.put(2, 2, pool__cache_weight = 0.5)
        rpc.put(3, 3, pool__cache_weight = 0.25)
        rpc.put(4, 4, pool__cache_weight = 0.75)

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

    with expected(KeyError(1)):
        rpc[1]
    assert rpc[2] == 2
    assert rpc[3] == 3
    assert rpc[4] == 4

    ###

    rpc = populate("useless", 2)

    with expected(KeyError(1)):
        rpc[1]
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

    # test eviction with group weight

    def populate_group(policy):

        rpc = ResourcePoolCache("name", size = 2, policy = policy, default_ttl = 2.0,
                                evict_period = 1.0, group_interval = 1.4)

        # group 1 contains less valuable values, but with more hits

        rpc.put(1, 1, pool__cache_weight = 1.0, pool__cache_group = "g1", pool__cache_ttl = None); rpc[1]; rpc[1]; rpc[1]; rpc[1]; rpc[1]; rpc[1]
        sleep(0.1)
        rpc.put(2, 2, pool__cache_weight = 2.0, pool__cache_group = "g1"); rpc[2]; rpc[2]; rpc[2]; rpc[2]; rpc[2]; rpc[2]; rpc[2]; rpc[2]
        sleep(0.1)

        assert rpc._group_counters["g1"].group_weight == 22 / 2

        # group 2 contains more valuable values, but with less hits,
        # therefore its values are preferred candidates for eviction

        rpc.put(3, 3, pool__cache_weight = 4.0, pool__cache_group = "g2", pool__cache_ttl = 2.1); rpc[3]
        sleep(0.1)
        rpc.put(4, 4, pool__cache_weight = 3.0, pool__cache_group = "g2", pool__cache_ttl = 2.2); rpc[4]; rpc[4]
        sleep(0.1)

        assert rpc._group_counters["g2"].group_weight == 10 / 2

        # effective weight for key 1: 1.0 * 11/16 = 11/16
        # effective weight for key 2: 2.0 * 11/16 = 22/16
        # effective weight for key 3: 4.0 * 5/16  = 20/16
        # effective weight for key 4: 3.0 * 5/16  = 15/16

        sleep(0.8)

        return rpc

    ###

    rpc = populate_group("lru")

    with expected(KeyError):
        rpc[1]
    with expected(KeyError):
        rpc[2]
    assert rpc[3] == 3
    assert rpc[4] == 4

    ###

    rpc = populate_group("lfu")

    assert rpc[1] == 1
    assert rpc[2] == 2
    with expected(KeyError):
        rpc[3]
    with expected(KeyError):
        rpc[4]

    ###

    rpc = populate_group("weight")

    with expected(KeyError):
        rpc[1]
    assert rpc[2] == 2
    assert rpc[3] == 3
    with expected(KeyError):
        rpc[4]

    ###

    rpc = populate_group("useless")

    assert rpc[1] == 1
    assert rpc[2] == 2
    with expected(KeyError):
        rpc[3]
    with expected(KeyError):
        rpc[4]

    ###

    rpc = populate_group("old")

    assert rpc[1] == 1
    assert rpc[2] == 2
    with expected(KeyError):
        rpc[3]
    with expected(KeyError):
        rpc[4]

    ###

    mm = set()

    for i in range(10):

        rpc = populate_group("random")

        m = set()

        try:
            rpc[1]
        except KeyError:
            m.add(1)
        try:
            rpc[2]
        except KeyError:
            m.add(2)
        try:
            rpc[3]
        except KeyError:
            m.add(3)
        try:
            rpc[4]
        except KeyError:
            m.add(4)

        assert len(m) == 2
        mm.add(frozenset(m))
        if len(mm) > 1:
            break

    else:
        assert False

    ###

    rpc = populate_group("old")

    sleep(0.6)

    # once the statistics disappears all the weight quotients become 1

    assert rpc[1] == 1
    with expected(KeyError):
        rpc[2]
    with expected(KeyError):
        rpc[3]
    assert rpc[4] == 4

    ###

    rpc = ResourcePoolCache("name", size = 1, policy = "old", evict_period = 1.0)
    assert rpc.default_ttl is None
    assert rpc.evict_period == 1.0

    rpc.put(1, 1)
    rpc.put(2, 2)

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

    rpc.put(1, 1, pool__cache_ttl = 1.0)
    rpc.put(2, 2, pool__cache_ttl = 1.0)
    rpc.put(3, "never expires 1", pool__cache_ttl = None)
    rpc.put(4, "never expires 2", pool__cache_ttl = None)
    assert len(rpc._cache) == 4

    rpc.purge()
    assert len(rpc._cache) == 4

    sleep(1.5)

    rpc.purge()
    assert len(rpc._cache) == 2

    # cached values are copied on entry and exit

    rpc = ResourcePoolCache("foo", size = 1)

    v1 = [ "mutable" ]
    cv = rpc._wrap_value(v1, {})
    assert cv.value == v1 and cv.value is not v1
    v2 = cv.value
    assert v2 == v1 and v2 is cv.value
    v3 = rpc._unwrap_value(cv)
    assert v3 == v1 and v3 is not cv.value

    original_value = { "value": { "mutable": "original value" } }
    rpc["key"] = original_value

    original_value["value"]["mutable"] = "modified original value"
    assert original_value["value"]["mutable"] == "modified original value"

    value1 = rpc["key"]
    assert isinstance(value1, dict)
    value2 = rpc["key"]
    assert isinstance(value2, dict)
    assert value1 is not value2 and value1["value"] is not value2["value"]

    value1["value"]["mutable"] = "modified cached value"
    assert value1["value"]["mutable"] == "modified cached value"
    assert value2["value"]["mutable"] == "original value"

    # performance

    def test_performance():

        def thingy(d): # generates a random immutable something, used for keys and values
            if d == 0:
                if random() < 0.25:
                    return "x" * randint(0, 8)
                elif random() < 0.5:
                    return randint(0, 1000)
                elif random() < 0.75:
                    return random()
                else:
                    return None
            else:
                return tuple(thingy(d - 1) for _ in range(0, randint(1, 4)))

        def test_loading_simple_keys(rpc):

            print("loading simple keys:")

            start = time()

            for k in range(rpc.size):
                rpc.put(k, str(k))

            print("{0:.03f} ms / key".format((time() - start) * 1000.0 / rpc.size))

            print("getting simple keys:")

            start = time()

            for k in range(rpc.size):
                assert rpc.get(k) == str(k)

            print("{0:.03f} ms / key".format((time() - start) * 1000.0 / rpc.size))

        def test_loading_complex_keys(rpc):

            print("loading complex keys:")

            random_keys = [ (i, thingy(randint(0, 2))) for i in range(rpc.size) ]
            random_values = [ (i, thingy(randint(0, 3))) for i in range(rpc.size) ]

            start = time()

            for i in range(rpc.size):
                rpc.put(random_keys[i], random_values[i])

            print("{0:.03f} ms / key".format((time() - start) * 1000.0 / rpc.size))

            print("getting complex keys:")

            start = time()

            for i in range(rpc.size):
                assert rpc.get(random_keys[i])[0] == random_values[i][0]

            print("{0:.03f} ms / key".format((time() - start) * 1000.0 / rpc.size))

        def test_eviction(rpc):

            random_keys = [ thingy(randint(0, 2)) for i in range(rpc.size) ]
            random_values = [ thingy(randint(0, 3)) for i in range(rpc.size) ]

            print("eviction, wait for", rpc.default_ttl * 2, "second(s):")

            # initial loading

            for i in range(rpc.size):
                r = randint(0, 10000000)
                rpc.put((r, random_keys[i]), (r, random_values[i]), pool__cache_weight = random(),
                        pool__cache_ttl = random() * rpc.default_ttl * 2)

            # now we insert at full speed

            ins = 0
            run_time = Timeout(rpc.default_ttl * 2)
            while not run_time.expired:
                r = randint(0, 10000000)
                i = randint(0, rpc.size - 1)
                rpc.put((r, random_keys[i]), (r, random_values[i]), pool__cache_weight = random(),
                        pool__cache_ttl = random() * rpc.default_ttl * 2)
                ins += 1

            print("evicted", ins - len(rpc._cache), "key(s)")

        def test_group_eviction(rpc):

            random_keys = [ thingy(randint(0, 2)) for i in range(rpc.size) ]
            random_values = [ thingy(randint(0, 3)) for i in range(rpc.size) ]

            print("group eviction, wait for", rpc.default_ttl * 2, "second(s):")

            # insert keys interleaving with occassional get's

            n = 0
            group_count = 25

            run_time = Timeout(rpc.default_ttl * 2)
            while not run_time.expired:

                # insert 10 x groups sequential keys

                for i in range(n, n + group_count * 10):
                    g = i % group_count
                    rpc.put((i, random_keys[i % rpc.size]), (i, random_values[i % rpc.size]), pool__cache_group = g,
                            pool__cache_weight = abs(normalvariate(1000 + 100 * g, 100))) # higher group has higher average key weight
                n += group_count * 10

                # touch 10 existing keys from each group, as only the not evicted keys affect
                # the statistics, this has strong positive feedback on the winning groups

                for i in range(10):
                    for g in range(group_count):
                        i = randint(0, n // group_count - 1) * group_count + g
                        rpc.get((i, random_keys[i % rpc.size]))

            # count the entire cache population by group

            group_keys = { g: 0 for g in range(group_count) }
            for k, cv in rpc._cache.items():
                group_keys[k[0] % group_count] += 1

            # see the contents of the groups

            for g, gc in rpc._group_counters.items():
                print("group #{0:02d} avg. weight {1:d} sample {2:d} total {3:d} key(s)".\
                      format(g, int(gc.group_weight), len(gc._data), group_keys[g]))

        rpc = ResourcePoolCache("rpc1", size = 100000)
        test_loading_simple_keys(rpc)

        rpc = ResourcePoolCache("rpc2", size = 100000)
        test_loading_complex_keys(rpc)

        rpc = ResourcePoolCache("rpc3", size = 100000, policy = "weight", default_ttl = 30.0, evict_period = 5.0)
        test_eviction(rpc)

        rpc = ResourcePoolCache("rpc4", size = 100000, policy = "lfu", default_ttl = 60.0, evict_period = 10.0, group_interval = 30.0)
        test_group_eviction(rpc)

    test_performance()

    ###################################

    ks = KeySet()

    ks.add_key({ "foo", 1 }, "FOO/1")
    ks.add_key({ "bar" }, "BAR")
    assert ks.pop_conflicting_keys({}) == set()
    assert ks.pop_conflicting_keys({ "foo" }) == { "FOO/1" }
    assert ks.pop_conflicting_keys({ "foo" }) == set()
    assert ks.pop_conflicting_keys({ "bar", 2 }) == { "BAR" }
    assert ks.pop_conflicting_keys({ "bar", 2 }) == set()

    ks.add_key({ "foo", 1 }, "FOO/1")
    ks.add_key({ "bar" }, "BAR")
    assert ks.pop_conflicting_keys({ "foo", 1 }) == { "FOO/1" }
    assert ks.pop_conflicting_keys({ "foo", 1 }) == set()
    assert ks.pop_conflicting_keys({ "bar", 1 }) == { "BAR" }

    ks.add_key({ "foo", 1 }, "FOO/1")
    ks.add_key({ "bar" }, "BAR")
    assert ks.pop_conflicting_keys({ "foo", "bar" }) == { "FOO/1", "BAR" }
    assert ks.pop_conflicting_keys({ "foo", "bar" }) == set()

    ###################################

    rwc = ResourcePoolReadWriteCache("name", size = 1)

    ###

    v = rwc.get("k", pool__cache_transaction_id = "xa1", pool__cache_timeout = 1.0)
    assert rwc._xacts["k"][1] == "xa1"
    assert v is None

    rwc.put("k", "v", pool__cache_transaction_id = "xa1", pool__cache_weight = 1.0)
    assert not rwc._xacts

    ###

    t = time()
    v = rwc.get("k2", pool__cache_transaction_id = "xa2", pool__cache_timeout = 1.0)
    assert time() - t < 0.1
    assert v is None

    t = time()
    v = rwc.get("k2", pool__cache_transaction_id = "xa3", pool__cache_timeout = 1.0)
    assert 0.9 < time() - t < 1.1
    assert v is Timeout

    with expected(AssertionError):
        rwc.put("k2", "???", pool__cache_transaction_id = "xa4???", pool__cache_weight = 1.0)

    v2 = ["v2"]
    rwc.put("k2", v2, pool__cache_transaction_id = "xa2", pool__cache_weight = 1.0)

    t = time()
    v = rwc.get("k2", pool__cache_transaction_id = "xa6", pool__cache_timeout = 1.0)
    assert time() - t < 0.1
    assert v == v2 and v is not v2

    ###

    rwc = ResourcePoolReadWriteCache("name", size = 1)

    run = Event()
    results = []

    def xa(xid):
        run.wait()
        v, att = None, 1
        while not v:
            v = rwc.get("k", pool__cache_transaction_id = xid, pool__cache_timeout = 2.0)
            if v is None:
                sleep(random() / 10)
                if random() < 0.2:
                    v = str(xid) + "/" + str(att)
                else:
                    att += 1
                rwc.put("k", v, pool__cache_transaction_id = xid, pool__cache_weight = 1.0)
            else:
                rwc.put("k", None, pool__cache_transaction_id = xid, pool__cache_weight = 1.0)
        results.append((xid, v))

    N = 10

    ths = []
    for i in range(N):
        th = Thread(target = xa, args = (i, ))
        ths.append(th)
        th.start()

    t = time()
    run.set()
    for th in ths:
        th.join()
    assert time() - t < 1.5

    assert(len(results)) == N

    xids, vxids, atts = set(), set(), set()
    for xid, vxid_att in results:
        xids.add(xid)
        vxid, att = map(int, vxid_att.split("/"))
        vxids.add(vxid)
        atts.add(att)

    assert xids == set(range(N))
    assert len(vxids) == 1 and 0 <= vxids.pop() < N
    assert len(atts) == 1 and 1 <= atts.pop() < 15

    ###################################

    ik = InterlockedQueue()
    rwc = ResourcePoolReadWriteCache("name", size = 1, invalidated_keys = ik)

    def rw_kwargs(*, read = None, write = None):
        return dict(pool__cache_transaction_id = "xa-{0:d}".format(randint(0, 999999)),
                    pool__cache_timeout = 1.0,
                    pool__cache_read_keys = read,
                    pool__cache_write_keys = write)

    # initial get

    dr = rw_kwargs(read = { "foo", "bar" })
    assert rwc.get("k", **dr) is None
    rwc.put("k", "v1", **dr)
    assert _hash("foo") in rwc._cached._keys
    assert _hash("bar") in rwc._cached._keys
    assert "k" in rwc._cache
    assert ik.pop(0.1) is None

    # cached get

    dr = rw_kwargs(read = { "foo", "BAR" })
    assert rwc.get("k", **dr) == "v1"
    rwc.put("k", None, **dr)
    assert _hash("BAR") in rwc._cached._keys
    assert "k" in rwc._cache
    assert ik.pop(0.1) is None

    # update

    dw = rw_kwargs(write = { "bar" })
    assert rwc.get("k", **dw) is None
    rwc.put("k", None, **dw)
    assert _hash("bar") not in rwc._cached._keys
    assert "k" not in rwc._cache
    assert ik.pop(0.1) == { _hash("bar") }
    assert ik.pop(0.1) is None

    # forced invalidation

    dr = rw_kwargs(read = { "a" })
    assert rwc.get("K1", **dr) is None
    rwc.put("K1", "V1", **dr)

    dr = rw_kwargs(read = { "b" })
    assert rwc.get("K2", **dr) is None
    rwc.put("K2", "V2", **dr)

    dr = rw_kwargs(read = { "c" })
    assert rwc.get("K3", **dr) is None
    rwc.put("K3", "V3", **dr)

    dr = rw_kwargs(read = { "b" })
    assert rwc.get("K?", **dr) is None
    assert len(rwc._read_reqs) == 1
    xa = list(rwc._read_reqs.keys())[0]
    assert rwc._read_reqs[xa] == { _hash("b") }

    assert "K1" in rwc
    assert "K2" in rwc
    assert "K3" in rwc

    rwc.invalidate({ "a", "b" })

    assert "K1" not in rwc
    assert "K2" not in rwc
    assert "K3" in rwc

    assert len(rwc._read_reqs) == 0
    assert ik.pop(0.1) is None

    # purging missing cache keys

    assert _hash("foo") in rwc._cached._keys
    assert _hash("BAR") in rwc._cached._keys
    rwc.purge()
    assert _hash("foo") not in rwc._cached._keys
    assert _hash("BAR") not in rwc._cached._keys

    # failing reads

    dr1 = rw_kwargs(read = { "foo", "bar" })

    t = time()
    assert rwc.get("k", **dr1) is None
    rwc.put("k", None, **dr1)
    assert "k" not in rwc
    assert time() - t < 0.1

    dr2 = rw_kwargs(read = { "foo", "bar" })

    t = time()
    assert rwc.get("k", **dr1) is None
    tt = time()
    assert rwc.get("k", **dr2) is Timeout
    assert time() - tt > 0.9
    rwc.put("k", None, **dr1)
    assert "k" not in rwc
    rwc.put("k", None, **dr1)
    assert "k" not in rwc
    assert time() - t < 1.1

    # transactions misbehaves

    dr1 = rw_kwargs(read = { "foo", "bar" })
    dr2 = rw_kwargs(read = { "foo", "bar" })

    with expected(AssertionError):
        rwc.put("k", "v", **dr1)

    assert rwc.get("k", **dr1) is None
    assert rwc.get("k", **dr2) is Timeout
    with expected(AssertionError):
        rwc.put("k", "v2", **dr2)
    assert "k" not in rwc
    rwc.put("k", "v1", **dr1)
    assert "k" in rwc
    assert rwc.pop("k") == "v1"
    assert "k" not in rwc

    dr = rw_kwargs(read = { "123" })

    assert rwc.get("k", **dr) is None
    rwc.put("k", "v", **dr)
    assert "k" in rwc
    assert rwc.get("k", **dr) == "v"
    with expected(AssertionError):
        rwc.put("k", "wut?", **dr)
    assert "k" in rwc
    del rwc["k"]
    assert "k" not in rwc

    # interleaved read/write access

    # case 1: rwRW

    dr = rw_kwargs(read = { "foo", "bar" })
    dw = rw_kwargs(write = { "bar", "biz" })

    assert rwc.get("k", **dr) is None
    assert rwc.get("k", **dw) is None         # this write invalidates
    rwc.put("k", "v", **dr)                   # prevents read result
    assert "k" not in rwc                     # from being cached
    rwc.put("k", None, **dw)

    # case 2: wrWR

    dr = rw_kwargs(read = { "1" })
    dw = rw_kwargs(write = { "1" })

    assert rwc.get("k", **dw) is None        # this write prevents read
    assert rwc.get("k", **dr) is None        # result from being cached
    rwc.put("k", None, **dw)
    rwc.put("k", "v", **dr)
    assert "k" not in rwc

    # case 3: wrRW

    dr = rw_kwargs(read = { "0", "00" })
    dw = rw_kwargs(write = { "0" })

    assert rwc.get("k", **dw) is None        # likewise
    assert rwc.get("k", **dr) is None
    rwc.put("k", "v", **dr)
    assert "k" not in rwc
    rwc.put("k", None, **dw)

    # case 4: rwWR

    dr = rw_kwargs(read = { "1" })
    dw = rw_kwargs(write = { "1", "11" })

    assert rwc.get("k", **dr) is None
    assert rwc.get("k", **dw) is None        # likewise
    rwc.put("k", None, **dw)
    rwc.put("k", "v", **dr)
    assert "k" not in rwc

    # case 5: rRwW (no conflict)

    dr = rw_kwargs(read = { "A" })
    dw = rw_kwargs(write = { "A" })

    assert rwc.get("k", **dr) is None
    rwc.put("k", "v", **dr)
    assert "k" in rwc
    assert rwc.get("k", **dw) is None
    rwc.put("k", None, **dw)
    assert "k" not in rwc

    # case 6: wWrR (no conflict)

    dr = rw_kwargs(read = { "B" })
    dw = rw_kwargs(write = { "B" })

    assert rwc.get("k", **dw) is None
    rwc.put("k", None, **dw)
    assert "k" not in rwc
    assert rwc.get("k", **dr) is None
    rwc.put("k", "v", **dr)
    assert "k" in rwc
    del rwc["k"]
    assert "k" not in rwc

    # case 7: rrwRRW

    dr1 = rw_kwargs(read = { "1", "2" })
    dr2 = rw_kwargs(read = { "1", "3" })
    dw = rw_kwargs(write = { "1", "4" })

    assert rwc.get("k", **dr1) is None
    assert rwc.get("k", **dr2) is Timeout
    assert rwc.get("k", **dw) is None
    rwc.put("k", "v1", **dr1)
    assert "k" not in rwc
    rwc.put("k", None, **dr2)
    assert "k" not in rwc
    rwc.put("k", None, **dw)
    assert "k" not in rwc

    # corner cases

    dr = rw_kwargs(read = { "foo" }, write = { "bar" })
    with expected(AssertionError):
        rwc.get("k", **dr)

    dr = rw_kwargs(read = {}, write = {})
    with expected(AssertionError):
        rwc.get("k", **dr)

    dr = rw_kwargs(read = {}) # this value cannot be invalidated by concurrent writes
    dr.update(pool__cache_ttl = 1.0)

    assert "k" not in rwc
    assert rwc.get("k", **dr) is None
    rwc.put("k", "v", **dr)
    assert "k" in rwc

    dw = rw_kwargs(write = {})
    assert rwc.get("k", **dw) is None
    rwc.put("k", None, **dw)
    assert "k" in rwc

    sleep(0.5)

    assert rwc.get("k", **dr) == "v"
    rwc.put("k", None, **dr)
    assert "k" in rwc

    sleep(1.0) # but it can expire

    assert rwc.get("k", **dr) is None
    rwc.put("k", None, **dr)
    assert "k" not in rwc

    # see what happens if the timeout expires immediately

    dr = rw_kwargs(read = { "foo" })
    dr.update(pool__cache_timeout = 0.0)

    assert rwc.get("k", **dr) is None
    sleep(0.1)
    rwc.put("k", "v", **dr) # this is too late from the request standpoint, but is still cached

    assert rwc.get("k", **dr) == "v"
    rwc.put("k", None, **dr)

    # burn-out test

    rwc = ResourcePoolReadWriteCache("name", size = 200, policy = "random",
                        default_ttl = 5.0, evict_period = 5.0, group_interval = 5.0)

    xac = InterlockedCounter()
    hits = 0
    miss = 0
    tout = 0

    def test_th(i):

        global hits, miss, tout

        running = Timeout(30.0)
        while not running.expired:

            xid = xac.next()

            k = randint(0, 999)
            t = random() * 0.1
            to = random() * 0.2
            g = randint(0, 9)
            ks = set(str(randint(0, 999)) for _ in range(randint(1, 10)))

            d = dict(pool__cache_transaction_id = xid,
                     pool__cache_group = g)
            if random() < 0.5:
                d.update(pool__cache_ttl = random() * 10)

            if random() < 0.9: # read
                d.update(pool__cache_timeout = to,
                         pool__cache_read_keys = ks)
                v = str(k)
            else: # write
                d.update(pool__cache_weight = t,
                         pool__cache_write_keys = ks)
                v = None

            r = rwc.get(k, **d)
            if r is None:
                miss += 1
                sleep(t)
                rwc.put(k, v, **d)
            elif r is Timeout:
                tout += 1
                rwc.put(k, None, **d)
            else:
                hits += 1
                assert r == str(k)
                rwc.put(k, None, **d)

            if xid % 1000 == 0:
                rwc.purge()

    print("multithreaded burn-out test: ", end = "")

    ths = []
    for i in range(50):
        th = Thread(target = test_th, args = (i, ))
        ths.append(th)
        th.start()

    for th in ths:
        th.join()

    print("hits: {0:d}, misses: {1:d}, timeouts: {2:d}".format(hits, miss, tout))

    print("all ok")

################################################################################
# EOF
