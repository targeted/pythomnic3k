#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module is a dispenser of thread pools and resource pools (which are
# grouped in pairs) and used by the transaction machinery and other modules
# that need them a private thread pool for something.
#
# Pythomnic3k project
# (c) 2005-2015, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
################################################################################

__all__ = [ "get_thread_pool", "get_resource_pool", "get_private_thread_pool" ]
__reloadable__ = False

################################################################################

import threading; from threading import Lock

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import typecheck; from typecheck import typecheck, optional, callable
import pmnc.thread_pool; from pmnc.thread_pool import ThreadPool
import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource, RegisteredResourcePool
import pmnc.resource_pool_cache; from pmnc.resource_pool_cache import ResourcePoolReadWriteCache

###############################################################################

# module-level state => not reloadable

_private_pools = {}
_combined_pools = {}
_pools_lock = Lock()

###############################################################################

class ResourceFactory: # callable factory object

    @typecheck
    def __init__(self, resource_name: str):

        self._resource_name = resource_name

        # load the configuration file

        self._config, self._config_version = self._get_config()

        # pool size for a resource is a static setting and is by default
        # equal to the number of the interfaces worker threads

        self._config_pool_size = self._config.pop("pool__size", None)
        self._pool_size = self._config_pool_size or pmnc.config_interfaces.get("thread_count")

        # number of resources to be kept warm is a static setting and is 0 by default

        self._pool_standby = self._config.pop("pool__standby", 0)

        # cache settings are optional

        self._pool_cache_size = self._config.pop("pool__cache_size", None)
        self._pool_cache_policy = self._config.pop("pool__cache_policy", None)
        self._pool_cache_default_ttl = self._config.pop("pool__cache_default_ttl", None)
        self._pool_cache_evict_period = self._config.pop("pool__cache_evict_period", None)
        self._pool_cache_group_interval = self._config.pop("pool__cache_group_interval", None)

        if self._pool_cache_size:
            self._pool_cache = ResourcePoolReadWriteCache(resource_name,
                                            size = self._pool_cache_size,
                                            policy = self._pool_cache_policy,
                                            default_ttl = self._pool_cache_default_ttl,
                                            evict_period = self._pool_cache_evict_period,
                                            group_interval = self._pool_cache_group_interval)
        else:
            self._pool_cache = None

        # instance adjustment settings are optional

        self._pool_idle_timeout = self._config.pop("pool__idle_timeout", None)
        self._pool_max_age = self._config.pop("pool__max_age", None)
        self._pool_min_time = self._config.pop("pool__min_time", None)
        self._pool_max_time = self._config.pop("pool__max_time", None)

    pool_size = property(lambda self: self._pool_size)
    pool_standby = property(lambda self: self._pool_standby)
    pool_cache = property(lambda self: self._pool_cache)

    @typecheck
    def __call__(self, resource_instance_name: str) -> TransactionalResource:

        # update current resource configuration if necessary

        config, config_version = self._get_config()
        if config_version != self._config_version:

            if pmnc.log.noise:
                pmnc.log.noise("configuration for resource {0:s} has been changed".\
                               format(self._resource_name))

            # pool settings cannot be changed at runtime

            if config.pop("pool__size", None) != self._config_pool_size:
                pmnc.log.warning("change in pool size for resource {0:s} at "
                                 "runtime has no effect".format(self._resource_name))

            if config.pop("pool__standby", 0) != self._pool_standby:
                pmnc.log.warning("change in pool standby for resource {0:s} at "
                                 "runtime has no effect".format(self._resource_name))

            # pool cache settings cannot be changed at runtime

            pool_cache_size = config.pop("pool__cache_size", None)
            pool_cache_policy = config.pop("pool__cache_policy", None)
            pool_cache_default_ttl = config.pop("pool__cache_default_ttl", None)
            pool_cache_evict_period = config.pop("pool__cache_evict_period", None)
            pool_cache_group_interval = config.pop("pool__cache_group_interval", None)

            if self._pool_cache:
                if pool_cache_size != self._pool_cache_size or \
                   pool_cache_policy != self._pool_cache_policy or \
                   pool_cache_default_ttl != self._pool_cache_default_ttl or \
                   pool_cache_evict_period != self._pool_cache_evict_period or \
                   pool_cache_group_interval != self._pool_cache_group_interval:
                    pmnc.log.warning("change in cache settings for resource {0:s} at "
                                     "runtime has no effect".format(self._resource_name))
            elif pool_cache_size:
                pmnc.log.warning("caching for resource {0:s} cannot be "
                                 "enabled at runtime".format(self._resource_name))

            # instance adjustment settings can be changed at runtime

            self._pool_idle_timeout = config.pop("pool__idle_timeout", None)
            self._pool_max_age = config.pop("pool__max_age", None)
            self._pool_min_time = config.pop("pool__min_time", None)
            self._pool_max_time = config.pop("pool__max_time", None)

            # what's left of the configuration file after stripping all meta
            # parameters is kept as the resource constructor parameters

            self._config = config
            self._config_version = config_version

        if pmnc.log.noise:
            pmnc.log.noise("creating resource instance {0:s}".format(resource_instance_name))

        resource_instance = pmnc.resource.create(resource_instance_name, **self._config)

        # if the meta settings have been present, adjust the created instance

        if self._pool_idle_timeout is not None:
            resource_instance.set_idle_timeout(self._pool_idle_timeout)

        if self._pool_max_age is not None:
            resource_instance.set_max_age(self._pool_max_age)

        if self._pool_min_time is not None:
            resource_instance.set_min_time(self._pool_min_time)

        if self._pool_max_time is not None:
            resource_instance.set_max_time(self._pool_max_time)

        resource_instance.set_pool_info(self._resource_name, self._pool_size)

        if pmnc.log.noise:
            pmnc.log.noise("resource instance {0:s} has been created".\
                           format(resource_instance_name))

        return resource_instance

    # returns a configuration dict for the resource plus configuration module version

    @typecheck
    def _get_config(self) -> (dict, int):

        module_properties = {}

        # resources with names like "rpc__cage" share the same base configuration
        # of "config_resource_rpc" and have pool__resource_name=cage parameter added
        #                     ^^^                               ^^^^

        if "__" in self._resource_name:
            config_name, resource_name = self._resource_name.split("__", 1)
            config_module_name = "config_resource_{0:s}".format(config_name)
            config = pmnc.__getattr__(config_module_name).copy(__module_properties = module_properties)
            config["pool__resource_name"] = resource_name
        else:
            config_module_name = "config_resource_{0:s}".format(self._resource_name)
            config = pmnc.__getattr__(config_module_name).copy(__module_properties = module_properties)

        return config, module_properties["version"]

###############################################################################
# this method returns (creating if necessary) a pair of a thread pool and
# resource pool for the specified resource

@typecheck
def _get_pools(resource_name: str) -> (ThreadPool, RegisteredResourcePool):

    pool_name = resource_name

    with _pools_lock:

        if pool_name not in _combined_pools:

            # create and cache new thread pool and resource pool for the resource,
            # they are of the same size, so that each thread can always pick a resource

            resource_factory = ResourceFactory(resource_name)

            # note that the hard limit on the resource pool size is greater than the number
            # of worker threads, this way worker threads still have each its guaranteed resource
            # instance whenever necessary, but the sweeper threads also can have their busy slots
            # whenever they intervene for removing the expired connections or warming the pool up

            thread_pool = ThreadPool(resource_name, resource_factory.pool_size)
            resource_pool = RegisteredResourcePool(resource_name, resource_factory,
                                                   resource_factory.pool_size + 2,
                                                   resource_factory.pool_standby,
                                                   resource_factory.pool_cache)

            _combined_pools[pool_name] = (thread_pool, resource_pool)

        return _combined_pools[pool_name]

###############################################################################

def get_thread_pool(resource_name: str) -> ThreadPool:
    return _get_pools(resource_name)[0]

###############################################################################

def get_resource_pool(resource_name: str) -> RegisteredResourcePool:
    return _get_pools(resource_name)[1]

###############################################################################

def get_private_thread_pool(pool_name: optional(str) = None,
                            pool_size: optional(int) = None,
                            *, __source_module_name) -> ThreadPool:

    pool_name = "{0:s}{1:s}".format(__source_module_name,
                                    pool_name is not None and "/{0:s}".format(pool_name) or "")
    with _pools_lock:

        if pool_name not in _private_pools:
            pool_size = pool_size or pmnc.config_interfaces.get("thread_count")
            _private_pools[pool_name] = ThreadPool(pool_name, pool_size)

        return _private_pools[pool_name]

###############################################################################

def self_test():

    from pmnc.request import fake_request
    from time import sleep
    from expected import expected
    from pmnc.timeout import Timeout

    ###################################

    def test_pools():

        tp1 = pmnc.shared_pools.get_thread_pool("void")
        tp2 = pmnc.shared_pools.get_thread_pool("void")
        assert tp1 is tp2

        assert tp1.free == 0

        assert list(_combined_pools.keys()) == [ "void" ]
        assert list(_private_pools.keys()) == []

        rp1 = pmnc.shared_pools.get_resource_pool("void")
        rp2 = pmnc.shared_pools.get_resource_pool("void")
        assert rp1 is rp2

        r = rp1.allocate()
        assert r.pool_name == "void" and r.pool_size == 3
        rp1.release(r)

    test_pools()

    ###################################

    def test_private_pool():

        tp1 = pmnc.shared_pools.get_private_thread_pool("foo")
        assert tp1.size == pmnc.config_interfaces.get("thread_count")

        tp2 = pmnc.shared_pools.get_private_thread_pool("foo", 10000)
        assert tp2 is tp1

        assert list(_combined_pools.keys()) == [ "void" ]
        assert list(_private_pools.keys()) == [ "shared_pools/foo" ]

        tp3 = pmnc.shared_pools.get_private_thread_pool(None, 3)
        assert tp3 is not tp2
        assert tp3.size == 3

        assert list(_combined_pools.keys()) == [ "void" ]
        assert list(sorted(_private_pools.keys())) == [ "shared_pools", "shared_pools/foo" ]

        def wu_test():
            return "123"

        wu = tp1.enqueue(fake_request(1.0), wu_test, (), {})
        assert wu.wait() == "123"

    test_private_pool()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
