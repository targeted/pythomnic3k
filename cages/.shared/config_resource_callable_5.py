# this configuration module exists for self-testing, but you can use it as
# a reference for configuring resources of "callable" protocol, in which case
# you copy this file and edit the copy

config = dict \
(
protocol = "callable",          # meta
)

# self-tests of protocol_callable.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
pool__cache_size = 3,
pool__cache_policy = "weight",  # will also be used for testing "weight" eviction policy
pool__cache_default_ttl = 2.0,  # enable caching for this pool, but for a very short time
pool__cache_evict_period = 3.0  # eviction will be possible once every 3 seconds
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF