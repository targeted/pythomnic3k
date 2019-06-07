# configuration file for interface "schedule_1"
# this file exists as a reference for configuring schedule interfaces
#
# copy this file to your own cage, possibly renaming into
# config_interface_YOUR_INTERFACE_NAME.py, then modify the copy

config = dict \
(
protocol = "schedule",     # meta
format = "%H:%M",          # schedule (argument to strftime)
match = "12:30",           # schedule (regular expression to match)
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

get = lambda key, default = None: pmnc.config.get_(config, {}, key, default)
copy = lambda: pmnc.config.copy_(config, {})

# EOF