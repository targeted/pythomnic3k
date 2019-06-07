# configuration file for interface "file_1"
# this file exists as a reference for configuring file interfaces
#
# copy this file to your own cage, possibly renaming into
# config_interface_YOUR_INTERFACE_NAME.py, then modify the copy

config = dict \
(
protocol = "file",                       # meta
source_directory = "/tmp",               # file
filename_regex = "[A-Za-z0-9_]+\\.msg",  # file
interval = 10.0,                         # file
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF