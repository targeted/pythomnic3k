#-*- coding: windows-1251 -*-

# this configuration file contains meta configuration values
# which are automatically expanded in all other configuration
# files whenever a value is of type str and contains appropriate
# string.Template construct: $key or ${key}, optionally
# embraced in "eval()", ex. "eval(int($number))"

config = dict \
(
)

# self-tests of config.py depend on the following configuration,
# this dict may safely be removed in production copies of this module

self_test_config = dict \
(
key = "value",
russian = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя",
number = 123,
bytes = b"\x00",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF
