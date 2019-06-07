# configuration file for resource "file_1"
#
# this file exists as a reference for configuring file resources
# and to support self-test run of module protocol_file.py
#
# copy this file to your own cage, possibly renaming into
# config_resource_YOUR_RESOURCE_NAME.py, then modify the copy

config = dict \
(
protocol = "file",              # meta
target_directory = "/tmp",      # file, optional directory name
temp_directory = None,          # file, optional directory name for temporary files
temp_extension = "tmp",         # file
file_permissions = "rw-rw----", # file, permissions to set on saved files
)

# self-tests of protocol_file.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
target_directory = "/tmp",
file_permissions = "r-----rwx",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF