# this configuration module exists for self-testing, but you can use it as
# a reference for configuring resources of "callable" protocol, in which case
# you copy this file and edit the copy

config = dict \
(
protocol = "callable",              # meta
)

# self-tests of transaction.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
pool__cache_size = 1, # enable caching for this pool
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF