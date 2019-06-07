# configuration file for resource "cmdexec_1"
#
# this file exists as a reference for configuring cmdexec resources
# and to support self-test run of module protocol_cmdexec.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "cmdexec",              # meta
executable = "c:\\app.exe",        # cmdexec
arguments = ("default_arg1", ),    # cmdexec, optional tuple of arguments to be specified at execution
environment = { "NAME": "VALUE" }, # cmdexec, optional dict of additional environment parameters
)

# self-tests of protocol_cmdexec.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
executable = "cat",
arguments = (),
environment = {},
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF