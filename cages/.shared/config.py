#!/usr/bin/env python3
#-*- coding: windows-1251 -*-
################################################################################
#
# This module redirects access to pmnc.config from some module to that module's
# private configuration module, so that any module can access its own private
# configuration by simply pmnc.config.
#
# For example,
#
# pmnc.config.get("foo") # in module bar.py
#
# actually fetches parameter "foo" from configuration file config_bar.py
#
# The returned values have certain meta-parameters expanded like macros,
# those meta-parameters are defined in config_config.py and can be used
# in actual parameters of type str using string.Template syntax, even if
# nested in simple containers.
#
# For example, if config_foo.py contains
#
# >> key = "here: ${bar}"
#
# and config_config.py contains
#
# >> bar = "biz"
#
# then in foo.py
#
# >> pmnc.config.get("key")
#
# returns
#
# >> "here: biz"
#
# Special variables $__node__, $__cage__ and $__cage_dir__ are expanded
# in this manner automatically.
#
# If you need values of type other than str to be expanded,
# you may use the following syntax:
#
# >> number = "eval(int(${number}))"
#              ^^^^^              ^ evaluated value is returned
#
# Note that the actual configuration files such as config_bar.py use this
# module's methods get_ and copy_ for extracting values. This comes in
# useful when you want to introduce your own custom source of configuration,
# in which case you modify just one this module to intercept the calls.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "get", "copy", "get_", "copy_" ]

###############################################################################

import string; from string import Template

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

###############################################################################
# the following methods redirect access to particular configuration files

def get(key: str, default = None, *, __source_module_name):

    config_module_name = "config_{0:s}".format(__source_module_name)
    return pmnc.__getattr__(config_module_name).get(key, default)

def copy(*, __source_module_name):

    config_module_name = "config_{0:s}".format(__source_module_name)
    return pmnc.__getattr__(config_module_name).copy()

###############################################################################
# the following methods are called back from redirected calls
# with their private configuration dicts as parameters

def get_(config, self_test_config, key, default, *, __source_module_name):

    if pmnc.request.self_test and key in self_test_config:
        result = self_test_config[key]
    else:
        result = config.get(key, default)

    return _expand(result, _get_config(__source_module_name))

def copy_(config, self_test_config, *, __source_module_name):

    result = config.copy()
    if pmnc.request.self_test:
        result.update(self_test_config)

    return _expand(result, _get_config(__source_module_name))

###############################################################################

def _get_config(source_module_name):

    if source_module_name == "config_{0:s}".format(__name__):
        return None

    result = pmnc.config.copy()
    result.update(__cage__ = __cage__, __node__ = __node__, __cage_dir__ = __cage_dir__)
    return result

###############################################################################

def _expand(value, config):

    if config is None:
        return value

    expand = _expand_type.get(type(value))
    return expand(value, config) if expand else value

###############################################################################

_expand_type = {}

def _expand_str(s, c):
    if s.startswith("eval(") and s.endswith(")"):
        return eval(Template(s[5:-1]).safe_substitute(c))
    else:
        return Template(s).safe_substitute(c)
_expand_type[str] = _expand_str

def _expand_dict(d, c):
    return { k: _expand(v, c) for k, v in d.items() }
_expand_type[dict] = _expand_dict

def _expand_list(el, c):
    return list(_expand(v, c) for v in el)
_expand_type[list] = _expand_list

def _expand_tuple(t, c):
    return tuple(_expand(v, c) for v in t)
_expand_type[tuple] = _expand_tuple

def _expand_set(s, c):
    return set(_expand(v, c) for v in s)
_expand_type[set] = _expand_set

###############################################################################

def self_test():

    from os import path as os_path, urandom
    from binascii import b2a_hex

    russian = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЬЫЪЭЮЯабвгдеёжзийклмнопрстуфхцчшщьыъэюя"

    ###################################

    # create a fake module with passthrough methods for get/copy

    module_name = "test{0:s}".format(b2a_hex(urandom(4)).decode("ascii"))

    with open(os_path.join(__cage_dir__, module_name) + ".py", "wb") as f:
        f.write("""\
__all__ = [ "get", "copy" ]

def get(k):
    return pmnc.config.get(k)

def copy():
    return pmnc.config.copy()

# EOF""".encode("ascii"))

    ###################################

    # create a regular config module for the fake module

    config_name = "config_{0:s}".format(module_name)

    with open(os_path.join(__cage_dir__, config_name) + ".py", "wb") as f:
        f.write("""\
#-*- coding: windows-1251 -*-

config = dict \\
(
a = "key",
b = "$key",
cage = "$__cage__",
node = "${__node__}",
cage_dir = "${__cage_dir__}",
mixed = { ("$key", ): [ ("$key", ), { "$key" } ], },
)

self_test_config = dict \\
(
c = "${key}${{key}}$$key{$}key$keykey{key$}{{$key{{${key$key}",
d = { "${russian}": "${russian}" },
number = "eval(int($number))",
bytes = "eval($bytes)",
bytes_str = "$bytes",
opaque = lambda: "$key",
)

# DO NOT TOUCH BELOW THIS LINE

__all__ = [ "get", "copy" ]

try: self_test_config
except NameError: self_test_config = {}

get = lambda key, default = None: pmnc.config.get_(config, self_test_config, key, default)
copy = lambda: pmnc.config.copy_(config, self_test_config)

# EOF""".encode("windows-1251"))

    ###################################

    def test_get():

        def get1(k):
            return pmnc.__getattr__(config_name).get(k)

        def get2(k):
            return pmnc.__getattr__(module_name).get(k)

        def test():

            assert get("a") == "key"
            assert get("b") == "value"
            assert get("node") == __node__
            assert get("cage") == __cage__
            assert get("cage_dir") == __cage_dir__
            assert get("c") == "value${{key}}$key{$}key$keykey{key$}{{value{{${keyvalue}"

            assert get("d") == { "${russian}": russian }
            assert get("mixed") == { ("$key", ): [ ("value", ), { "value" } ] }

            assert get("number") == 123
            assert get("bytes") == b"\x00"
            assert get("bytes_str") == "b'\\x00'"
            assert get("opaque")() == "$key"

        get = get1
        test()

        get = get2
        test()

    test_get()

    ###################################

    def test_copy():

        def copy1():
            return pmnc.__getattr__(config_name).copy()

        def copy2():
            return pmnc.__getattr__(module_name).copy()

        def test():

            d = copy()

            assert d.pop("opaque")() == "$key"

            assert d == dict \
            (
                a = "key",
                b = "value",
                bytes = b"\x00",
                bytes_str = "b'\\x00'",
                c = "value${{key}}$key{$}key$keykey{key$}{{value{{${keyvalue}",
                cage = __cage__,
                cage_dir = __cage_dir__,
                d = { "${russian}": russian },
                node = __node__,
                number = 123,
                mixed = { ("$key", ): [ ("value", ), { "value" } ] },
            )

        copy = copy1
        test()

        copy = copy2
        test()

    test_copy()

    ###################################

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
