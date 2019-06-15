#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module implements the only existing facility for accessing resources
# from Pythomnic3k application - best effort distributed transaction where
# each participating resource is accessed through a separate resource pool
# managed by its own pool of threads.
#
# Examples:
#
# xa = pmnc.transaction.create()
# xa.db_resource.execute("SELECT ...")
# xa.http_resource.post("/", b"foo")
# db_result, http_result = xa.execute()
#
# xa = pmnc.transaction.create()
# xa.state.set(key, value)
# xa.pmnc("somecage", queue = "retry").module.method(...)
# retry_id = xa.execute()[1]
#
# For a transaction with just one participant the following shortcut syntax
# may be used:
#
# db_result = pmnc.transaction.db_resource.execute("SELECT ...")
#
# which is equivalent to
#
# xa = pmnc.transaction.create()
# xa.db_resource.execute("SELECT ...")
# db_result = xa.execute()[0]
#
# Not all of the resources may support "true" transactions, for instance
# in the above examples, sending HTTP request is irreversible and not really
# transaction-capable. Anyway, for uniformity access to all resources is
# wrapped in a transaction, possibly meaningless, and all resources are
# encouraged to implement some degree of atomicity and durability.
#
# Even if all the participating resources are transaction capable, there is
# still a chance of group commit failure, albeit a small one. At the moment
# of commit each participating resource is represented by separate thread
# waiting for decision signal, and they are signaled to commit simultaneously.
# Provided that each individual commit operation is fast and fail-safe,
# the chance of the group commit failure is small.
#
# To reiterate: this module has nothing to do with two-phase commit and it is
# intentional. All resources should attempt to make their commit operations
# fast and fail-safe, but if commit operation fails, bad luck, end of story.
#
# Error handling:
#
# A failing transaction may only throw ResourceError or TransactionError.
# By examining the exception, the caller may determine (1) which of the
# participating resources has failed, (2) whether or not the resource
# error occured before some irreversible changes have been made.
# ResourceError may also convey resource-specific error code.
#
# xa = pmnc.transaction.create()
# xa.foo.do_something(...)
# xa.bar.do_something_else(...)
# try:
#     xa.execute()
# except ResourceError as e:
#     if e.participant_index == 1: # resource bar failed
#         if e.recoverable: # everything has been undone at rollback
#             if e.code == -1: # oh, I know this one
#                 ...
#
# If more than one resource fail in the transaction you still get one
# exception from just one of them. To analyze more than one exception,
# you need to write a custom acceptance method (see below in advanced usage).
#
# If a resource throws some error other than ResourceError, it is converted
# into unrecoverable ResourceError just to be on the safe side. The resoures
# should therefore dictate by throwing appropriate ResourceError's.
#
# If a transaction fails as a whole, for example if it could not obtain
# the results from all of its participants, one of TransactionError's
# descendants is thrown: TransactionExecutionError or TransactionCommitError.
#
# try:
#     xa.execute()
# except TransactionExecutionError:
#     ... could not complete the transaction, rolling back ...
# except TransactionCommitError:
#     ... one of participating resources failed to commit ...
#
# Advanced usage:
#
# Results of each resource participating in a transaction are collected
# before any of the resources commits. The decision whether to commit or
# rollback a pending transaction is made by an acceptance method, which
# takes the results collected so far and returns the "true" transaction
# result or throws to initiate a rollback. The default acceptance method
# (see _default_accept) returns the results unmodified or throws if any
# of the resources threw.
#
# You can supply your own acceptance method for doing all sorts of magic.
# For example, if you have multiple different resources which may return
# the same result, you can execute them in one transaction and pick the
# fastest result, ignoring possible failures from the others, unless
# all of them fail.
#
# def accept_fastest(xa, results):
#     wait_for_more = False
#     any_exception = None
#     for result in results:
#         if result is xa.NoValue: # n'th resource has not returned yet
#             wait_for_more = True
#         elif isinstance(result, Exception): # n'th resource has failed
#             any_exception = value
#         else:
#             return result # we have the winner
#     if not wait_for_more:
#         raise any_exception
#
# xa = pmnc.transaction.create(accept = accept_fastest, sync_commit = False)
# xa.source_1.lookup(key)
# xa.source_2.lookup(key)
# xa.source_3.lookup(key)
# value = xa.execute()
#
# Note: sync_commit = False is required because with default of sync_commit = True
# all of the resources are waited to commit, and should any of them become slow
# it defeats the entire purpose of the "fastest" result.
#
# Caching (see resource_pool_cache.py for more information):
#
# You can control the cache behaviour by supplying the following optional
# kwargs to the resource call:
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_key = "key")
# The result of this particular call is cached under this literal key,
# thus changing the above "identical" semantics. As an alternative,
# a function could be passed for pool__cache_key, which then in turn
# returns the actual key value. The default caching key is essentially
# all arguments to the resource call i.e. (attrs, args, kwargs)
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_weight = N.N)
# Float weight of the resulting cache entry. By default a transaction assigns
# every entry it caches with float weight equal to the number of seconds
# the resource has been executing. This makes cache eviction policy
# "weight" particularly useful, as the cheapest entries get evicted first.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_wrap = lambda value: ...)
# Callable to be executed on the actual execution result before it's cached.
# The wrapped value will later be returned from the cache.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_get = lambda key: ...)
# Callable to override cache get behaviour altogether.
#
# >> xa.resource.foo(1, biz = "baz", pool__cache_put = lambda key, value: ...)
# Callable to override cache put behaviour altogether.
#
# Pythomnic3k project
# (c) 2005-2019, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "create", "Transaction", "__get_module_attr__" ]

################################################################################

import os; from os import urandom
import binascii; from binascii import b2a_hex
import threading; from threading import Event
import time; from time import time, strftime
import inspect; from inspect import isfunction

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import exc_string; from exc_string import exc_string
import typecheck; from typecheck import typecheck, callable, optional
import interlocked_queue; from interlocked_queue import InterlockedQueue
import interlocked_counter; from interlocked_counter import InterlockedCounter
import pmnc.thread_pool; from pmnc.thread_pool import WorkUnitTimedOut
import pmnc.samplers; from pmnc.samplers import RateSampler
import pmnc.timeout; from pmnc.timeout import Timeout
import pmnc.resource_pool; from pmnc.resource_pool import ResourceError, \
                                TransactionCommitError, TransactionExecutionError

###############################################################################

def create(*, __source_module_name, **options):

    # creating an instance of transaction by calling the class
    # through pmnc allows this module to be itself reloadable

    return pmnc.transaction.Transaction(__source_module_name, **options)

###############################################################################

class Transaction:

    _transaction_rate_sampler = RateSampler(10.0)
    _transaction_count = InterlockedCounter()

    @typecheck
    def __init__(self, source_module_name, *,
                 accept: optional(callable) = None,
                 sync_commit: optional(bool) = True,
                 **options):

        self._source_module_name = source_module_name
        self._accept = accept or self._default_accept
        self._sync_commit = sync_commit
        self._options = options

        transaction_time = strftime("%Y%m%d%H%M%S")
        random_id = b2a_hex(urandom(6)).decode("ascii").upper()
        self._xid = "XA-{0:s}-{1:s}".format(transaction_time, random_id)
        self._details = None

        self._resources, self._results = [], InterlockedQueue()
        self._decision, self._commit = Event(), Event()
        self._transaction_rate_sampler.tick()

    ###################################

    def __str__(self):
        return "XA-{0:s}{1:s}".format(self._xid[-4:],
               " ({0:s})".format(self._details) if self._details is not None else "")

    def __format__(self, spec):
        return format(str(self), spec)

    ###################################

    @staticmethod
    def _resource_ttl(resource_instance) -> str:
        if resource_instance.expired:
            return "expired"
        else:
            return "expires in {0:.01f} second(s)".format(resource_instance.ttl)

    # this method is used by interface_performance.py
    # to extract the current transaction rate

    @classmethod
    def get_transaction_rate(cls):
        return cls._transaction_rate_sampler.avg

    ###################################

    # this method is executed in context of a worker thread from the resource thread pool,
    # it initiates the transaction, executes the workload, delivers the result to the
    # original transaction thread, waits for a decision and performs commit/rollback

    def wu_participate(self, transaction_start, participant_index,
                       resource_name, attrs, args, kwargs, res_args, res_kwargs):

        # see whether the request by which this transaction was created
        # has expired in the meantime, and if it has, simply bail out
        # because the transaction should have long been perished

        # no attempt to execute the request is taken and no result
        # is delivered, simply because the transaction is assumed
        # to already be aborted, nowhere to report the result

        if pmnc.request.expired:
            pmnc.log.error("execution of resource {0:s} in transaction "
                           "{1:s} was late".format(resource_name, self))
            return

        try:

            if pmnc.log.noise:
                pmnc.log.noise("resource {0:s} joins transaction {1:s}".format(resource_name, self))

            resource_instance = None        # no instance has been allocated yet
            resource_in_transaction = False # no transaction has been started on the instance
            resource_failed = True          # (preventive) request execution has been a failure
            cache_key = None                # cache key to refer to this transaction's result
            cached_result = None            # value returned from the cache
            result = None                   # the actual execution result

            while True: # breaks when the result is obtained, either value or exception

                # any failure prior to actual resource allocation results
                # in a recoverable ResourceError, pointlessly terminal

                try:

                    # the pending interval is measured from the beginning of the
                    # transaction, not from the beginning of the request

                    pending_ms = int((time() - transaction_start) * 1000)
                    pmnc.performance.sample("resource.{0:s}.pending_time".\
                                            format(resource_name), pending_ms)

                    # allocate a resource instance from a specific resource pool

                    resource_pool = pmnc.shared_pools.get_resource_pool(resource_name)
                    resource_instance = resource_pool.allocate()

                except: # tested
                    result = ResourceError.snap_exception(
                                    participant_index = participant_index,
                                    recoverable = True, terminal = True) # but not really terminal,
                    break # while True                                   # no instance to terminate

                # some resource instance has been allocated

                try:

                    # see if request deadline should be restricted for the course of transaction

                    max_time = resource_instance.max_time
                    if max_time is not None:
                        request_start, request_remain = time(), pmnc.request.remain
                        if request_remain > max_time:
                            pmnc.request.remain = max_time
                            if pmnc.log.noise:
                                pmnc.log.noise("request deadline is restricted for the course of transaction")
                        else:
                            max_time = None # to not restore timeout to a bigger value than it already has now

                    try: # finally restore the timeout

                        # see if there is a cached result

                        cache_kwargs = { k: v for k, v in kwargs.items() if k.startswith("pool__cache_") }
                        for k in cache_kwargs:
                            del kwargs[k]

                        # cache key may be passed in pool__cache_key as a literal value
                        # or as a callable taking (attrs, args, kwargs) as parameters,
                        # and if it is not specified, the default cache key is simply
                        # a tuple of frozen (attrs, args, kwargs)

                        if not resource_pool.has_cache:
                            cache_key = None
                        elif "pool__cache_key" in cache_kwargs:
                            cache_key = cache_kwargs.pop("pool__cache_key") # but this still can be None
                        else:
                            cache_key = lambda attrs, args, kwargs: (tuple(attrs), args, frozenset(kwargs.items()))

                        if isfunction(cache_key):
                            cache_key = cache_key(attrs, args, kwargs)

                        # if cache key evaluated to None after all, the cache is bypassed at all

                        if cache_key is not None:
                            cache_get = cache_kwargs.pop("pool__cache_get", None) or resource_pool.cache_get
                            cache_put = cache_kwargs.pop("pool__cache_put", None) or resource_pool.cache_put

                        # weight can be overridden by the caller

                        cache_weight = cache_kwargs.pop("pool__cache_weight", None)

                        # executable to wrap the result before it's cached

                        cache_wrap = cache_kwargs.pop("pool__cache_wrap", None)

                        # this id allows the cache to match get/put calls from the same transaction

                        transaction_id = self._transaction_count.next()

                        # getting result from the cache may not be instant, therefore
                        # the timeout is passed, moreover it could block for a while
                        # and still return None

                        try:
                            if cache_key is not None:
                                cached_result = cache_get(cache_key,
                                                          pool__cache_timeout = pmnc.request.remain,
                                                          pool__cache_transaction_id = transaction_id,
                                                          **cache_kwargs)
                        except:
                            pmnc.log.error("cache get failed in {0:s}: {1:s}".format(self, exc_string())) # log and proceed without cache
                            cache_failed = True
                        else:
                            cache_failed = False

                        try:

                            if cached_result is None: # not found in the cache, but this transaction is allowed to proceed

                                # see if the transaction should be started in as little time as the request has left

                                if pmnc.request.remain < resource_instance.min_time:
                                    raise ResourceError(description = "transaction {0:s} is declined by resource instance "
                                                                      "{1:s}".format(self, resource_instance.name),
                                                        recoverable = True, terminal = False)

                                if pmnc.log.noise:
                                    pmnc.log.noise("resource instance {0:s} is used in transaction {1:s}, {2:s}".\
                                                   format(resource_instance.name, self, self._resource_ttl(resource_instance)))

                                # begin a new transaction, this is presumably a reversible operation

                                resource_instance.begin_transaction(self._xid,
                                                                    source_module_name = self._source_module_name,
                                                                    transaction_options = self._options,
                                                                    resource_args = res_args,
                                                                    resource_kwargs = res_kwargs)

                                resource_in_transaction = True

                                # replay attribute accesses to obtain the actual target method

                                target_method = resource_instance
                                for attr in attrs:
                                    target_method = getattr(target_method, attr)

                                # execute the request, registering the execution time

                                processing_start = time()
                                try:
                                    with pmnc.performance.timing("resource.{0:s}.processing_time".format(resource_name)):
                                        result = target_method(*args, **kwargs)
                                finally:
                                    if cache_weight is None:                               # by default cache weight
                                        cache_weight = max(time() - processing_start, 0.0) # is the execution time

                                # technically the resource call may return None, but then it wouldn't be cached

                                if result is not None and cache_wrap: # note that wrapping takes place even when
                                    try:                              # cache_key is None or cache_failed because
                                        result = cache_wrap(result)   # the caller expects a uniform result format
                                    except:
                                        result = None # this invalidates the execution result because
                                        raise         # now it can neither be cached nor returned

                            elif cached_result is Timeout: # not found in the cache and timeout has expired, technically this is
                                                           # the same as pmnc.request.expired but comparing time could be unreliable

                                raise ResourceError(description = "request deadline waiting for cached result from resource {0:s} "
                                                                  "in transaction {1:s}".format(resource_instance.name, self),
                                                    recoverable = True, terminal = False)

                        finally:
                            if cache_key is not None and not cache_failed:
                                try:
                                    cache_put(cache_key, result, # contains actual execution result or None upon exception
                                              pool__cache_weight = cache_weight, # contains actual execution time or None
                                              pool__cache_transaction_id = transaction_id,
                                              **cache_kwargs)
                                except:
                                    pmnc.log.error("cache put failed in {0:s}: {1:s}".format(self, exc_string())) # log and ignore cache error only

                        if cached_result is not None and cached_result is not Timeout: # now the cached result is put into place
                            result = cached_result

                    finally: # restore the request timeout if it has been restricted
                        if max_time is not None:
                            pmnc.request.remain = request_start + request_remain - time()
                            if pmnc.log.noise:
                                pmnc.log.noise("request deadline is restored")

                except ResourceError as e:
                    result = self._apply_error(participant_index, resource_instance, e)
                    break # while True
                except Exception: # tested
                    result = ResourceError.snap_exception(
                                    participant_index = participant_index,
                                    recoverable = not resource_in_transaction, terminal = True)
                    resource_instance.expire()
                    break # while True
                else:
                    resource_instance.reset_idle_timeout()
                    resource_failed = False
                    break # while True

            # we got an intermediate result, possibly an exception

            try:

                # deliver the result to the pending transaction

                self._results.push((participant_index, result))

                # register the actual result of this participant

                pmnc.performance.event("resource.{0:s}.transaction_rate.{1:s}".\
                                       format(resource_name, resource_failed and "failure" or "success"))

                # the result may have been taken from cache in which case there
                # has been no transaction and we simply acknowledge the commit

                if result is cached_result and cached_result is not None:
                    if pmnc.log.noise:
                        pmnc.log.noise("resource instance {0:s} is returning cached result in transaction "
                                       "{1:s}".format(resource_instance.name, self))
                    return "commit"

                if not resource_in_transaction: # as we couldn't begin a transaction,
                    return "failure"            # we are not interested in the decision

                if pmnc.log.noise:
                    pmnc.log.noise("resource instance {0:s} is waiting for decision in "
                                   "transaction {1:s}".format(resource_instance.name, self))

                # figure out whether the resource has to commit or rollback

                commit_transaction = False

                if pmnc.request.wait(self._decision): # wait for transaction's decision
                    if self._commit.is_set():
                        if not resource_failed:
                            commit_transaction = True
                            if pmnc.log.noise:
                                pmnc.log.noise("resource instance {0:s} decided to commit in transaction "
                                               "{1:s}".format(resource_instance.name, self))
                        else:
                            pmnc.log.warning("resource instance {0:s} had to rollback despite decision to commit "
                                             "in transaction {1:s}".format(resource_instance.name, self))
                    else:
                        if pmnc.log.noise:
                            pmnc.log.noise("resource instance {0:s} decided to rollback in transaction "
                                           "{1:s}".format(resource_instance.name, self))
                else:
                    pmnc.log.warning("resource instance {0:s} had to abandon waiting for decision and "
                                     "rollback in transaction {1:s}".format(resource_instance.name, self))

                # complete the transaction and return the final outcome

                if commit_transaction:
                    try:
                        resource_instance.commit()
                    except:
                        pmnc.log.error("resource instance {0:s} failed to commit in transaction {1:s}: "
                                       "{2:s}".format(resource_instance.name, self, exc_string())) # this is a severe problem
                        resource_instance.expire()
                        return "failure"
                    else:
                        if pmnc.log.noise:
                            pmnc.log.noise("resource instance {0:s} committed in transaction "
                                           "{1:s}".format(resource_instance.name, self))
                        return "commit"
                else:
                    try:
                        resource_instance.rollback()
                    except:
                        pmnc.log.warning("resource instance {0:s} failed to rollback in transaction {1:s}: "
                                         "{2:s}".format(resource_instance.name, self, exc_string())) # this is not a big deal
                        resource_instance.expire()
                        return "failure"
                    else:
                        if pmnc.log.noise:
                            pmnc.log.noise("resource instance {0:s} rolled back in transaction "
                                           "{1:s}".format(resource_instance.name, self))
                        return "rollback"

            finally:
                if resource_instance:
                    if pmnc.log.noise:
                        pmnc.log.noise("resource instance {0:s} is being released, {1:s}".\
                                       format(resource_instance.name, self._resource_ttl(resource_instance)))
                    resource_pool.release(resource_instance)

        except:
            pmnc.log.error(exc_string()) # this should not normally happen, but do
            raise                        # not allow such exception to be silenced

    ###################################

    # this utility methods applies a thrown ResourceError to a resource instance
    # that threw it, updates the participant index, presumably unknown to the instance

    def _apply_error(self, participant_index, resource_instance, resource_error):

        resource_error.participant_index = participant_index

        if resource_error.terminal:
            resource_instance.expire()
        else:
            resource_instance.reset_idle_timeout()

        return resource_error

    ###################################

    # this method initiates transaction execution for each of the individual
    # resources each through its own thread pool, collects the intermediate
    # results of the yet uncommitted transactions and makes the commit/rollback
    # decision

    class NoValue: pass # this class serves as an empty placeholder in transaction results

    def execute(self):

        if not self._resources: # shortcut to handle (useless) empty transactions
            return ()

        self._details = ", ".join("{0:s}.{1:s}".format(t[0], ".".join(t[1]))
                                  for t in self._resources)

        if pmnc.log.noise:
            pmnc.log.noise("transaction {0:s} begins".format(self))
        try:

            # initiate execution of all the individual resources, each through
            # its own thread pool but having an identical cloned request

            transaction_start = time()
            work_units = []

            for participant_index, (resource_name, attrs, args, kwargs, res_args, res_kwargs) in enumerate(self._resources):
                thread_pool = pmnc.shared_pools.get_thread_pool(resource_name)
                work_units.append(thread_pool.enqueue(pmnc.request.clone(), self.wu_participate,
                                                      (transaction_start, participant_index, resource_name,
                                                       attrs, args, kwargs, res_args, res_kwargs), {}))

            # wait for all the individual resources to deliver intermediate results,
            # which are pushed by each participant to a _results queue as it completes

            # upon each new result result interpretation is performed, accept method
            # returns None, the final transaction result or throws to initiate rollback

            results = [ self.NoValue ] * len(self._resources)
            result_count = 0

            while result_count < len(results):

                idx_result = pmnc.request.pop(self._results) # wait for another result
                if idx_result is None:
                    for i, result in enumerate(results): # find the first resource that did not return a result
                        if result is self.NoValue:
                            raise TransactionExecutionError(
                                    description = "request deadline waiting for intermediate result from resource " # tested
                                                  "{0:s} in transaction {1:s}".format(self._resources[i][0], self),
                                    participant_index = i)
                    else:
                        assert False # this should not happen

                results[idx_result[0]] = idx_result[1] # register the result
                result_count += 1

                result = self._accept(self, results) # this gets executed upon each incoming result,
                if result is not None:               # the final outcome may not require all the results
                    break

            else:
                raise TransactionExecutionError(
                        description = "intermediate results of transaction {0:s} " # tested
                                      "have not been accepted".format(self)) # participant index is None

        except:
            if pmnc.log.noise:
                pmnc.log.noise("transaction {0:s} is being rolled back".format(self))
            raise
        else:
            self._commit.set()
            if pmnc.log.noise:
                pmnc.log.noise("transaction {0:s} is being committed".format(self))
        finally:
            self._decision.set()

        if self._sync_commit: # wait for all the individual resources to commit

            for participant_index, work_unit in enumerate(work_units):
                resource_name = self._resources[participant_index][0]
                try:
                    resource_decision = work_unit.wait() # blocks until work_unit completes or request deadline
                except WorkUnitTimedOut:
                    pmnc.log.warning("transaction {0:s} had to abandon waiting for commit "
                                     "from resource {1:s}".format(self, resource_name))
                    raise TransactionCommitError(
                            description = "request deadline waiting for commit from resource " # tested
                                          "{0:s} in transaction {1:s}".format(resource_name, self),
                            participant_index = participant_index)
                if resource_decision != "commit":
                    raise TransactionCommitError(
                            description = "transaction {0:s} got unexpected commit outcome from resource " # tested
                                          "{1:s}: {2:s}".format(self, resource_name, resource_decision),
                            participant_index = participant_index)

            # transaction is a complete success

            if pmnc.log.noise:
                pmnc.log.noise("transaction {0:s} completes successfully in {1:.01f} "
                               "second(s)".format(self, time() - transaction_start))

        else: # leave without waiting for the individual resources to commit

            # transaction is likely a success, but no guarantee

            if pmnc.log.noise:
                pmnc.log.noise("transaction {0:s} is committed in {1:.01f} second(s)".\
                               format(self, time() - transaction_start))

        return result

    ###################################

    # this method analyzes the raw results of the not yet committed individual transactions and
    # returns None for waiting for more results, adjusted results for commit or throws for rollback

    @staticmethod
    def _default_accept(xa, results):
        for result in results:
            if result is xa.NoValue:
                return None
            elif isinstance(result, Exception):
                raise result
        else:
            return tuple(results)

    ###################################

    # the following methods and the supporting class collect arguments for participating resources

    def __getattr__(self, resource_name):
        return Transaction.ResourceArgumentCollector(resource_name, self._collect)

    class ResourceArgumentCollector:

        def __init__(self, resource_name, collect):
            self._resource_name, self._collect = resource_name, collect
            self._attrs, self._res_args, self._res_kwargs = [], (), {}

        def __getattr__(self, name):
            self._attrs.append(name)
            return self

        def __call__(self, *args, **kwargs):
            if not self._attrs:
                self._res_args, self._res_kwargs = args, kwargs # this allows for xa.res(...) syntax
                return self
            else:
                self._collect(self._resource_name, self._attrs, args, kwargs, # attach another participating
                              self._res_args, self._res_kwargs)               # resource to the transaction

    def _collect(self, resource_name, attrs, args, kwargs, res_args, res_kwargs):
        self._resources.append((resource_name, attrs, args, kwargs, res_args, res_kwargs))

###############################################################################

def __get_module_attr__(resource_name, *, __source_module_name):

    # this method executes a transaction with a single participant

    def execute_transaction(*args, __call_attributes, **kwargs):
        xa = pmnc.transaction.Transaction(__source_module_name)
        resource = getattr(xa, resource_name)
        for attr in __call_attributes:
            resource = getattr(resource, attr)
        resource(*args, **kwargs)
        return xa.execute()[0]

    return execute_transaction

###############################################################################

def self_test():

    from pmnc.request import fake_request
    from expected import expected
    from typecheck import by_regex
    from threading import Thread
    from random import randint
    from time import sleep
    from pmnc.timeout import Timeout
    from pmnc.resource_pool import SQLResourceError
    from pmnc.module_loader import ModuleNotFoundError
    from os import path as os_path

    ###################################

    def test_empty_transaction():
        fake_request(1.0)
        xa = pmnc.transaction.create()
        xa.execute()

    test_empty_transaction()

    ###################################

    def test_transaction_rate():
        fake_request(4.0)
        t = Timeout(2.0)
        while not t.expired:
            xa = pmnc.transaction.create()
            xa.execute()
        xa = pmnc.transaction.create()
        xa.execute()
        assert xa.get_transaction_rate() > 1.0

    test_transaction_rate()

    ###################################

    q = pmnc.config_resource_callable_1.get("trace_queue")

    # default implementations of callable hooks

    def begin_transaction(res, *args, **kwargs):
        res._count += 1
        res._q.push(("begin_transaction", res._count, args, kwargs))

    def execute(res, *args, **kwargs):
        res._count += 1
        res._q.push(("execute", res._count, args, kwargs))
        return "ok"

    def commit(res):
        res._count += 1
        res._q.push(("commit", res._count))

    def rollback(res):
        res._count += 1
        res._q.push(("rollback", res._count))

    hooks_ = dict(begin_transaction = begin_transaction,
                  execute = execute,
                  commit = commit,
                  rollback = rollback)

    ###################################

    def test_plain_success_failure():

        # success

        fake_request(1.0)

        hooks = hooks_.copy()

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        assert xa.execute() == ("ok", )

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(0.0)[:2] == ("execute", 2)
        assert q.pop(0.0) == ("commit", 3) # commit is waited upon, therefore "commit" is in the queue
        assert q.pop(1.0) is None

        # success

        fake_request(1.0)

        # it is impossible to pass arguments to the resource (**hooks)
        # using single participant syntax, therefore we resort to simple void

        assert pmnc.transaction.void.success("foo", biz = "baz") == (("foo", ), { "biz": "baz" })

        # success using cache

        cache = {}

        def from_cache(key, **kwargs):
            value = cache.get(key)
            if value is not None:
                return value + " from cache"

        def to_cache(key, value, **kwargs):
            if value is not None:
                cache[key] = value + " to cache"

        def execute(res, *args, **kwargs):
            return "success"
        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(1,
            pool__cache_get = from_cache, pool__cache_put = to_cache)
        assert xa.execute() == ("success", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(1,
            pool__cache_get = from_cache, pool__cache_put = to_cache)
        assert xa.execute() == ("success to cache from cache", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(2,
            pool__cache_get = from_cache, pool__cache_put = to_cache)
        assert xa.execute() == ("success", )

        # failure

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            1 / 0
        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert by_regex("^(?:int )?division (?:or modulo )?by zero$")(str(e))
            assert e.participant_index == 0 and e.terminal and not e.recoverable # unhandled exception
        else:
            assert False

        assert q.pop(0.0)[:2] == ("begin_transaction", 4)
        assert q.pop(1.0)[:3] == ("rollback", 5) # rollback is not waited upon, therefore "rollback" may not appear in the queue immediately
        assert q.pop(1.0)[:4] == ("disconnect", 6)
        assert q.pop(1.0) is None

        # failure

        fake_request(1.0)

        with expected(ResourceError, "failure"):
            pmnc.transaction.void.failure()

        with expected(ResourceError, "'function' object has no attribute 'unexpected'"):
            pmnc.transaction.void.failure.unexpected()

        with expected(ModuleNotFoundError, "file config_resource_nonexistent.py was not found"):
            pmnc.transaction.nonexistent.execute()

        # failure using cache

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(1,
            pool__cache_get = from_cache, pool__cache_put = to_cache)
        assert xa.execute() == ("success to cache from cache", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(1,
            pool__cache_key = "not there", pool__cache_get = from_cache, pool__cache_put = to_cache)
        with expected(ResourceError):
            xa.execute()

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(1,
            pool__cache_key = lambda attrs, args, kwargs: "still not there",
            pool__cache_get = from_cache, pool__cache_put = to_cache)
        with expected(ResourceError) as e:
            xa.execute()

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(2,
            pool__cache_get = from_cache, pool__cache_put = to_cache)
        assert xa.execute() == ("success to cache from cache", )

        xa = pmnc.transaction.create()
        xa.callable_4(execute = execute).execute(3,
            pool__cache_get = from_cache, pool__cache_put = to_cache)
        with expected(ResourceError):
            xa.execute()

    test_plain_success_failure()

    ###################################

    def test_connect_fails():

        # connect fails

        fake_request(1.0)

        xa = pmnc.transaction.create()
        xa.callable_2(**hooks_).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert by_regex("^(?:int )?division (?:or modulo )?by zero$")(str(e))
            assert e.participant_index == 0 and e.terminal and e.recoverable
        else:
            assert False

        # connect hangs

        fake_request(1.0)

        xa = pmnc.transaction.create()
        xa.callable_3(**hooks_).execute()
        try:
            xa.execute()
        except TransactionExecutionError as e:
            assert str(e) == "request deadline waiting for intermediate result from " \
                             "resource callable_3 in transaction {0:s}".format(xa)
            assert e.participant_index == 0
        else:
            assert False

    test_connect_fails()

    ###################################

    def test_begin_transaction_fails():

        # begin_transaction hangs

        fake_request(1.0)

        def begin_transaction(res, *args, **kwargs):
            sleep(2.0)
        hooks = hooks_.copy(); hooks["begin_transaction"] = begin_transaction

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except TransactionExecutionError as e:
            assert str(e) == "request deadline waiting for intermediate result from " \
                             "resource callable_1 in transaction {0:s}".format(xa)
            assert e.participant_index == 0
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.5) is None # this is where begin_transaction is called
        assert q.pop(1.0)[:2] == ("execute", 1)
        assert q.pop(1.0)[:2] == ("rollback", 2)
        assert q.pop(1.0) is None

        # note that the resource instance had not failed and is reused

        # begin_transaction fails

        fake_request(1.0)

        def begin_transaction(res, *args, **kwargs):
            1 / 0
        hooks = hooks_.copy(); hooks["begin_transaction"] = begin_transaction

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert by_regex("^(?:int )?division (?:or modulo )?by zero$")(str(e))
            assert e.participant_index == 0 and e.terminal and e.recoverable
        else:
            assert False

        assert q.pop(1.0)[:2] == ("disconnect", 3)
        assert q.pop(1.0) is None

    test_begin_transaction_fails()

    ###################################

    def test_execute_fails():

        # execute hangs

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            sleep(2.0)
        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except TransactionExecutionError as e:
            assert str(e) == "request deadline waiting for intermediate result from " \
                             "resource callable_1 in transaction {0:s}".format(xa)
            assert e.participant_index == 0
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(0.5) is None # this is where execute is called
        assert q.pop(1.0)[:2] == ("rollback", 2)
        assert q.pop(1.0) is None

        # note that the resource instance had not failed and is reused

        # execute throws

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            1 / 0
        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert by_regex("^(?:int )?division (?:or modulo )?by zero$")(str(e))
            assert e.participant_index == 0 and e.terminal and not e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("begin_transaction", 3)
        assert q.pop(1.0)[:2] == ("rollback", 4)
        assert q.pop(1.0)[:2] == ("disconnect", 5)
        assert q.pop(1.0) is None

    test_execute_fails()

    ###################################

    def test_commit_fails():

        # commit hangs

        fake_request(1.0)

        def commit(res, *args, **kwargs):
            sleep(2.0)
        hooks = hooks_.copy(); hooks["commit"] = commit

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except TransactionCommitError as e:
            assert str(e) == "request deadline waiting for commit from resource " \
                             "callable_1 in transaction {0:s}".format(xa)
            assert e.participant_index == 0
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(0.0)[:2] == ("execute", 2)
        assert q.pop(1.5) is None # this is where commit is called

        # note that the resource instance had not failed and is reused

        # commit throws

        fake_request(1.0)

        def commit(res, *args, **kwargs):
            1 / 0
        hooks = hooks_.copy(); hooks["commit"] = commit

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except TransactionCommitError as e:
            assert str(e) == "transaction {0:s} got unexpected commit outcome " \
                             "from resource callable_1: failure".format(xa)
            assert e.participant_index == 0
        else:
            assert False

        assert q.pop(0.0)[:2] == ("begin_transaction", 3)
        assert q.pop(0.0)[:2] == ("execute", 4)
        assert q.pop(1.0)[:2] == ("disconnect", 5)
        assert q.pop(1.0) is None

    test_commit_fails()

    ###################################

    def test_rollback_fails():

        # rollback hangs

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            1 / 0
        def rollback(res, *args, **kwargs):
            sleep(2.0)
        hooks = hooks_.copy(); hooks["execute"] = execute; hooks["rollback"] = rollback

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert by_regex("^(?:int )?division (?:or modulo )?by zero$")(str(e))
            assert e.participant_index == 0 and e.terminal and not e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(1.0) is None # this is where rollback is called
        assert q.pop(2.0)[:2] == ("disconnect", 2)

        # rollback throws

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            1 / 0
        def rollback(res, *args, **kwargs):
            {}["not there"]
        hooks = hooks_.copy(); hooks["execute"] = execute; hooks["rollback"] = rollback

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert by_regex("^(?:int )?division (?:or modulo )?by zero$")(str(e))
            assert e.participant_index == 0 and e.terminal and not e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(1.0)[:2] == ("disconnect", 2)

    test_rollback_fails()

    ###################################

    def test_errors():

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            raise ResourceError(code = 1, description = "foo", recoverable = True) # but terminal by default

        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert str(e) == "1: foo" and e.code == 1 and e.description == "foo"
            assert e.participant_index == 0 and e.terminal and e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(1.0)[:2] == ("rollback", 2)
        assert q.pop(1.0)[:2] == ("disconnect", 3)
        assert q.pop(1.0) is None

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            raise SQLResourceError(description = "bar", state = "P0001", recoverable = False) # and terminal by default

        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except SQLResourceError as e:
            assert str(e) == "[P0001] bar" and e.state == "P0001" and e.description == "bar"
            assert e.participant_index == 0 and e.terminal and not e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(1.0)[:2] == ("rollback", 2)
        assert q.pop(1.0)[:2] == ("disconnect", 3)
        assert q.pop(1.0) is None

    test_errors()

    ###################################

    def test_specific_error():

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            raise ResourceError(code = 123, description = "good error", terminal = False)

        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert str(e) == "123: good error"
            assert e.code == 123 and e.description == "good error" and e.terminal == False
            assert e.participant_index == 0 and not e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(1.0)[:2] == ("rollback", 2)
        assert q.pop(1.0) is None

        # note that the resource instance is reused despite the failure

        fake_request(1.0)

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks_).execute()
        assert xa.execute() == ("ok", )

        assert q.pop(0.0)[:2] == ("begin_transaction", 3)
        assert q.pop(0.0)[:2] == ("execute", 4)
        assert q.pop(0.0)[:2] == ("commit", 5)
        assert q.pop(1.0) is None

        # now for the terminal error

        fake_request(1.0)

        def execute(res, *args, **kwargs):
            raise ResourceError(code = 456, description = "bad error", terminal = True)

        hooks = hooks_.copy(); hooks["execute"] = execute

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks).execute()
        try:
            xa.execute()
        except ResourceError as e:
            assert str(e) == "456: bad error"
            assert e.code == 456 and e.description == "bad error" and e.terminal == True
            assert e.participant_index == 0 and not e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("begin_transaction", 6)
        assert q.pop(1.0)[:2] == ("rollback", 7)
        assert q.pop(1.0)[:2] == ("disconnect", 8)
        assert q.pop(1.0) is None

    test_specific_error()

    ###################################

    def test_invalid_use():

        fake_request(1.0)

        xa = pmnc.transaction.create()
        xa.callable_1(**hooks_).not_supported()
        try:
            xa.execute()
        except ResourceError as e:
            assert str(e) == "'Resource' object has no attribute 'not_supported'"
            assert e.code is None and e.description == "'Resource' object has no attribute 'not_supported'"
            assert e.participant_index == 0 and e.terminal and not e.recoverable
        else:
            assert False

        assert q.pop(0.0)[:2] == ("connect", 0)
        assert q.pop(0.0)[:2] == ("begin_transaction", 1)
        assert q.pop(1.0)[:2] == ("rollback", 2)
        assert q.pop(1.0)[:2] == ("disconnect", 3)
        assert q.pop(1.0) is None

    test_invalid_use()

    ###################################

    def test_pool_exhaust():

        fake_request(1.0)

        xa = pmnc.transaction.create()
        xa.void.success()
        xa.void.success()
        xa.void.success()
        xa.void.success() # one too many, pool__size = 3, results in deadlock
        try:
            xa.execute()
        except TransactionExecutionError as e:
            assert str(e) == "request deadline waiting for intermediate result " \
                             "from resource void in transaction {0:s}".format(xa)
            assert 0 <= e.participant_index <= 3
        else:
            assert False

    test_pool_exhaust()

    ###################################

    def test_resource_decline():

        fake_request(0.4) # less than pool__min_time

        xa = pmnc.transaction.create()
        xa.void.success()
        try:
            xa.execute()
        except ResourceError as e:
            assert str(e).startswith("transaction {0:s} is declined by resource instance void/".format(xa))
            assert e.participant_index == 0 and e.recoverable and not e.terminal
        else:
            assert False

    test_resource_decline()

    ###################################

    def test_resource_max_time():

        fake_request(10.0)

        def f():
            return pmnc.request.remain

        assert 4.0 < pmnc.transaction.void.execute(f) < 6.0

    test_resource_max_time()

    ###################################

    def test_no_accept():

        fake_request(1.0)

        def no_accept(xa, results):
            return None

        xa = pmnc.transaction.create(accept = no_accept)
        xa.void.success()
        try:
            xa.execute()
        except TransactionExecutionError as e:
            assert str(e) == "intermediate results of transaction {0:s} have not been accepted".format(xa)
            assert e.participant_index is None
        else:
            assert False

    test_no_accept()

    ###################################

    def test_accept_fastest():

        def accept_fastest(xa, results):
            wait_for_more = False
            any_exception = None
            for result in results:
                if result is xa.NoValue: # n'th resource has not returned yet
                    wait_for_more = True
                elif isinstance(result, Exception): # n'th resource has failed
                    any_exception = result
                else:
                    return result # we have the winner
            if not wait_for_more:
                raise any_exception

        fake_request(2.0)

        xa = pmnc.transaction.create(accept = accept_fastest)
        xa.callable_1(execute = lambda self: sleep(1.0) or 1).execute()
        xa.callable_1(execute = lambda self: sleep(0.5) or 2).execute()
        xa.callable_1(execute = lambda self: sleep(2.5) or 3).execute()
        try:
            xa.execute()
        except TransactionCommitError as e:
            assert str(e) == "request deadline waiting for commit from resource " \
                             "callable_1 in transaction {0:s}".format(xa)
            assert e.participant_index == 2
        else:
            assert False

        fake_request(2.0)

        xa = pmnc.transaction.create(accept = accept_fastest, sync_commit = False)
        xa.callable_1(execute = lambda self: sleep(1.0) or 1).execute()
        xa.callable_1(execute = lambda self: sleep(0.5) or 2).execute()
        xa.callable_1(execute = lambda self: sleep(2.5) or 3).execute()
        assert xa.execute() == 2

    test_accept_fastest()

    ###################################

    def test_partial_commit():

        def accept_anything(xa, results):
            for value in results:
                if value is not xa.NoValue:
                    return value

        fake_request(2.0)

        xa = pmnc.transaction.create(accept = accept_anything)
        xa.callable_1(execute = lambda self: 1).execute()
        xa.callable_1(execute = lambda self: 1 / 0).execute()
        try:
            xa.execute()
        except TransactionCommitError as e:
            assert str(e) == "transaction {0:s} got unexpected commit outcome " \
                             "from resource callable_1: rollback".format(xa)
            assert e.participant_index == 1
        else:
            assert False

        fake_request(2.0)

        xa = pmnc.transaction.create(accept = accept_anything, sync_commit = False)
        xa.callable_1(execute = lambda self: 1).execute()
        xa.callable_1(execute = lambda self: 1 / 0).execute()
        xa.execute()

    test_partial_commit()

    ###################################

    def test_performance():

        N = 256

        def threads(n, f):
            ths = [ Thread(target = f, args = (N // n, )) for i in range(n) ]
            start = time()
            for th in ths: th.start()
            for th in ths: th.join()
            return int(N  / (time() - start))

        def test_transaction(n):
            for i in range(n):
                fake_request(10.0)
                xa = pmnc.transaction.create()
                xa.void.success()
                xa.execute()

        def state_transaction(n):
            for i in range(n):
                fake_request(30.0)
                xa = pmnc.transaction.create()
                xa.state.set(str(randint(0, 1000000)), i)
                xa.execute()

        fake_request(60.0)

        pmnc.log("begin performance test (may take a few minutes)")
        pmnc._loader.set_log_level("LOG")
        try:

            test_1 = threads(1, test_transaction)
            test_4 = threads(4, test_transaction)
            test_16 = threads(16, test_transaction)
            test_64 = threads(64, test_transaction)

            pmnc.log("{0:d}/{1:d}/{2:d}/{3:d} empty transaction(s) per second".\
                     format(test_1, test_4, test_16, test_64))

            state_1 = threads(1, state_transaction)
            state_4 = threads(4, state_transaction)
            state_16 = threads(16, state_transaction)
            state_64 = threads(64, state_transaction)

            pmnc.log("{0:d}/{1:d}/{2:d}/{3:d} state transaction(s) per second".\
                     format(state_1, state_4, state_16, state_64))

        finally:
            pmnc._loader.set_log_level("DEBUG")

        pmnc.log("end performance test")

    test_performance()

    ###################################

    def test_syntax_equivalence():

        fake_request(10.0)

        with open(os_path.join(__cage_dir__, "test_smn.py"), "w") as f:
            f.write("""\
__all__ = ["test"]

def test():

    pmnc.state.set("key", "value")

    r1 = pmnc.state.get("key")

    xa = pmnc.transaction.create()
    xa.state.get("key")
    r2 = xa.execute()[0]

    r3 = pmnc.transaction.state.get("key")

    return r1, r2, r3

# EOF
""");

        sleep(1.0)

        r1, r2, r3 = pmnc.test_smn.test()
        assert r1 == r2 == r3 == "value"

    test_syntax_equivalence()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
