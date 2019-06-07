#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# Function guards for Python 3.
#
# (c) 2016, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license.
#
# Samples:
#
# from funcguard import guard
#
# @guard
# def abs(a, _when = "a >= 0"):
#     return a
#
# @guard
# def abs(a, _when = "a < 0"):
#     return -a
#
# assert abs(1) == abs(-1) == 1
#
# @guard
# def factorial(n): # no _when expression => default
#    return 1
#
# @guard
# def factorial(n, _when = "n > 1"):
#    return n * factorial(n - 1)
#
# assert factorial(10) == 3628800
#
# class TypeTeller:
#     @staticmethod
#     @guard
#     def typeof(value, _when = "isinstance(value, int)"):
#         return int
#     @staticmethod
#     @guard
#     def typeof(value, _when = "isinstance(value, str)"):
#         return str
#
# assert TypeTeller.typeof(0) is int
# TypeTeller.typeof(0.0) # throws
#
# class AllowedProcessor:
#     def __init__(self, allowed):
#         self._allowed = allowed
#     @guard
#     def process(self, value, _when = "value in self._allowed"):
#         return "ok"
#     @guard
#     def process(self, value): # no _when expression => default
#         return "fail"
#
# ap = AllowedProcessor({1, 2, 3})
# assert ap.process(1) == "ok"
# assert ap.process(0) == "fail"
#
# guard.default_eval_args( # insert values to all guards scopes
#     office_hours = lambda: 9 <= datetime.now().hour < 18)
#
# @guard
# def at_work(*args, _when = "office_hours()", **kwargs):
#     print("welcome")
#
# @guard
# def at_work(*args, **kwargs):
#     print("come back tomorrow")
#
# at_work() # either "welcome" or "come back tomorrow"
#
# The (5 times longer) source code with self-tests is available from:
# http://www.targeted.org/python/recipes/funcguard.py
#
################################################################################

__all__ = [ "guard", "guard_module",
            "GuardException", "IncompatibleFunctionsException",
            "FunctionArgumentsMatchException", "GuardExpressionException",
            "DuplicateDefaultGuardException", "GuardEvalException",
            "NoMatchingFunctionException" ]

################################################################################

import inspect; from inspect import getfullargspec
import functools; from functools import wraps
import sys; from sys import modules
try:
    (lambda: None).__qualname__
except AttributeError:
    if __name__ != "__main__":
        import qualname; from qualname import qualname # Python 3.1 workaround
else:
    qualname = lambda f: f.__qualname__

################################################################################

class GuardException(Exception): pass
class IncompatibleFunctionsException(GuardException): pass
class FunctionArgumentsMatchException(GuardException): pass
class GuardExpressionException(GuardException): pass
class DuplicateDefaultGuardException(GuardException): pass
class GuardEvalException(GuardException): pass
class NoMatchingFunctionException(GuardException): pass

################################################################################
# takes an argument specification for a function and a set of actual call
# positional and keyword arguments, returns a flat namespace-like dict
# mapping parameter names to their actual values

def _eval_args(argspec, args, kwargs):

    # match positional arguments

    matched_args = {}
    expected_args = argspec.args
    default_args = argspec.defaults or ()

    _many = lambda t: "argument" + ("s" if len(t) != 1 else "")

    # copy provided args to expected, append defaults if necessary

    for i, name in enumerate(expected_args):
        if i < len(args):
            value = args[i]
        elif i >= len(expected_args) - len(default_args):
            value = argspec.defaults[i - len(expected_args) + len(default_args)]
        else:
            missing_args = expected_args[len(args):len(expected_args) - len(default_args)]
            raise FunctionArgumentsMatchException("missing required positional {0:s}: {1:s}".\
                      format(_many(missing_args), ", ".join(missing_args)))
        matched_args[name] = value

    # put extra provided args to *args if the function allows

    if argspec.varargs:
        matched_args[argspec.varargs] = args[len(expected_args):] if len(args) > len(expected_args) else ()
    elif len(args) > len(expected_args):
        raise FunctionArgumentsMatchException(
                  "takes {0:d} positional {1:s} but {2:d} {3:s} given".
                  format(len(expected_args), _many(expected_args),
                         len(args), len(args) == 1 and "was" or "were"))

    # match keyword arguments

    matched_kwargs = {}
    expected_kwargs = argspec.kwonlyargs
    default_kwargs = argspec.kwonlydefaults or {}

    # extract expected kwargs from provided, using defaults if necessary

    missing_kwargs = []
    for name in expected_kwargs:
        if name in kwargs:
            matched_kwargs[name] = kwargs[name]
        elif name in default_kwargs:
            matched_kwargs[name] = default_kwargs[name]
        else:
            missing_kwargs.append(name)
    if missing_kwargs:
        raise FunctionArgumentsMatchException("missing required keyword {0:s}: {1:s}".\
                  format(_many(missing_kwargs), ", ".join(missing_kwargs)))

    extra_kwarg_names = [ name for name in kwargs if name not in matched_kwargs ]
    if argspec.varkw:
        if extra_kwarg_names:
            extra_kwargs = { name: kwargs[name] for name in extra_kwarg_names }
        else:
            extra_kwargs = {}
        matched_args[argspec.varkw] = extra_kwargs
    elif extra_kwarg_names:
        raise FunctionArgumentsMatchException("got unexpected keyword {0:s}: {1:s}".\
                  format(_many(extra_kwarg_names), ", ".join(extra_kwarg_names)))

    # both positional and keyword argument are returned in the same scope-like dict

    for name, value in matched_kwargs.items():
        matched_args[name] = value

    return matched_args

################################################################################
# takes an argument specification for a function, from it extracts and returns
# a compiled expression which is to be matched against call arguments

def _get_guard_expr(func_name, argspec):

    guard_expr_text = None

    if "_when" in argspec.args:
        defaults = argspec.defaults or ()
        i = argspec.args.index("_when")
        if i >= len(argspec.args) - len(defaults):
            guard_expr_text = defaults[i - len(argspec.args) + len(defaults)]
    elif "_when" in argspec.kwonlyargs:
        guard_expr_text = (argspec.kwonlydefaults or {}).get("_when")
    else:
        return None # indicates default guard

    if guard_expr_text is None:
        raise GuardExpressionException("guarded function {0:s}() requires a \"_when\" "
                                       "argument with guard expression text as its "
                                       "default value".format(func_name))
    try:
        guard_expr = compile(guard_expr_text, func_name, "eval")
    except Exception as e:
        error = str(e)
    else:
        error = None
    if error is not None:
        raise GuardExpressionException("invalid guard expression for {0:s}(): "
                                       "{1:s}".format(func_name, error))

    return guard_expr

################################################################################
# checks whether two functions' argspecs are compatible to be guarded as one,
# compatible argspecs have identical positional and keyword parameters except
# for "_when" and annotations

def _compatible_argspecs(argspec1, argspec2):
    return _stripped_argspec(argspec1) == _stripped_argspec(argspec2)

def _stripped_argspec(argspec):

    args = argspec.args[:]
    defaults = list(argspec.defaults or ())
    kwonlyargs = argspec.kwonlyargs[:]
    kwonlydefaults = (argspec.kwonlydefaults or {}).copy()

    if "_when" in args:
        i = args.index("_when")
        if i >= len(args) - len(defaults):
            del defaults[i - len(args) + len(defaults)]
            del args[i]
    elif "_when" in kwonlyargs and "_when" in kwonlydefaults:
        i = kwonlyargs.index("_when")
        del kwonlyargs[i]
        del kwonlydefaults["_when"]

    return (args, defaults, kwonlyargs, kwonlydefaults, argspec.varargs, argspec.varkw)

################################################################################

def guard(func, module = None): # the main decorator function

    # see if it is a function of a lambda

    try:
        eval(func.__name__)
    except SyntaxError:
        return func # <lambda> => not guarded
    except NameError:
        pass # valid name

    # get to the bottom of a possible decorator chain
    # to get the original function's specification

    original_func = func
    while hasattr(original_func, "__wrapped__"):
        original_func = original_func.__wrapped__

    func_name = qualname(original_func)
    func_module = module or modules[func.__module__]
    argspec = getfullargspec(original_func)

    # the registry of known guarded function is attached to the module containg them

    guarded_functions = getattr(func_module, "__guarded_functions__", None)
    if guarded_functions is None:
        guarded_functions = func_module.__guarded_functions__ = {}

    original_argspec, first_guard, last_guard = guard_info = \
        guarded_functions.setdefault(func_name, [argspec, None, None])

    # all the guarded functions with the same name must have identical signature

    if argspec is not original_argspec and not _compatible_argspecs(argspec, original_argspec):
        raise IncompatibleFunctionsException("function signature is incompatible "
                    "with the previosly registered {0:s}()".format(func_name))

    @wraps(func)
    def func_guard(*args, **kwargs): # the call proxy function

        # since all versions of the function have essentially identical signatures,
        # their mapping to the actually provided arguments can be calculated once
        # for each call and not against every version of the function

        try:
            eval_args = _eval_args(argspec, args, kwargs)
        except FunctionArgumentsMatchException as e:
            error = str(e)
        else:
            error = None
        if error is not None:
            raise FunctionArgumentsMatchException("{0:s}() {1:s}".format(func_name, error))

        for name, value in guard.__default_eval_args__.items():
            eval_args.setdefault(name, value)

        # walk the chain of function versions starting with the first, looking
        # for the one for which the guard expression evaluates to truth

        current_guard = func_guard.__first_guard__
        while current_guard:
            try:
                if not current_guard.__guard_expr__ or \
                   eval(current_guard.__guard_expr__, globals(), eval_args):
                    break
            except Exception as e:
                error = str(e)
            else:
                error = None
            if error is not None:
                raise GuardEvalException("guard expression evaluation failed for "
                                         "{0:s}(): {1:s}".format(func_name, error))
            current_guard = current_guard.__next_guard__
        else:
            raise NoMatchingFunctionException("none of the guard expressions for {0:s}() "
                                              "matched the call arguments".format(func_name))

        return current_guard.__wrapped__(*args, **kwargs) # call the winning function version

    # instrument the new guard with expression and reference to
    # the wrapped function (unless @wraps already did the latter)

    if not hasattr(func_guard, "__wrapped__"):
        func_guard.__wrapped__ = func
    func_guard.__guard_expr__ = _get_guard_expr(func_name, argspec)

    # maintain a linked list for all versions of the function

    if last_guard and not last_guard.__guard_expr__: # the list is not empty and the
                                                     # last guard is already a default
        if not func_guard.__guard_expr__:
            raise DuplicateDefaultGuardException("the default version of {0:s}() has already "
                                                 "been specified".format(func_name))

        # the new guard has to be inserted one before the last

        if first_guard is last_guard: # the list contains just one guard

            # new becomes first, last is not changed

            first_guard.__first_guard__ = func_guard.__first_guard__ = func_guard
            func_guard.__next_guard__ = first_guard
            first_guard = guard_info[1] = func_guard

        else: # the list contains more than one guard

            # neither first nor last are changed

            prev_guard = first_guard
            while prev_guard.__next_guard__ is not last_guard:
                prev_guard = prev_guard.__next_guard__

            func_guard.__first_guard__ = first_guard
            func_guard.__next_guard__ = last_guard
            prev_guard.__next_guard__ = func_guard

    else: # the new guard is inserted last

        if not first_guard:
            first_guard = guard_info[1] = func_guard
        func_guard.__first_guard__ = first_guard
        func_guard.__next_guard__ = None
        if last_guard:
            last_guard.__next_guard__ = func_guard
        last_guard = guard_info[2] = func_guard

    return func_guard

guard.__default_eval_args__ = {}
guard.default_eval_args = lambda *args, **kwargs: guard.__default_eval_args__.update(*args, **kwargs)

guard_module = lambda module: lambda func: guard(func, module)

################################################################################

if __name__ == "__main__":

    print("self-testing module func_guard.py:")

    ############################################################################

    from sys import path
    from math import sqrt

    try:
        from expected import expected
        from typecheck import typecheck, by_regex, InputParameterError, ReturnValueError
    except ImportError:
        print("Utility modules expected.py and typecheck.py are required "
              "for these self-tests to run,\nyou may dowlnload them as a part "
              "of Pythomnic3k framework at http://www.pythomnic3k.org")
        raise SystemExit()

    ############################################################################

    print("guard expression: ", end = "")

    ########

    def _get_guard(func):
        guard_expr = _get_guard_expr(func.__name__, getfullargspec(func))
        return eval(guard_expr, {}) if guard_expr else None

    def _get_guard_fails(func):
        with expected(GuardExpressionException("guarded function foo() requires a \"_when\" "
                                       "argument with guard expression text as its default value")):
            _get_guard(func)

    def foo(): pass
    assert _get_guard(foo) is None

    def foo(_when): pass
    _get_guard_fails(foo)

    def foo(_when = "123"): pass
    assert _get_guard(foo) == 123

    def foo(a, _when): pass
    _get_guard_fails(foo)

    def foo(a = 1, _when = "123"): pass
    assert _get_guard(foo) == 123

    def foo(a, _when = "123"): pass
    assert _get_guard(foo) == 123

    def foo(a, b = 1, _when = "123"): pass
    assert _get_guard(foo) == 123

    def foo(a, b = 1, _when = "123", c = 2): pass
    assert _get_guard(foo) == 123

    def foo(*, _when): pass
    _get_guard_fails(foo)

    def foo(*, _when = "123"): pass
    assert _get_guard(foo) == 123

    def foo(*, a, _when): pass
    _get_guard_fails(foo)

    def foo(*, a, _when = "123"): pass
    assert _get_guard(foo) == 123

    def foo(a, *, _when): pass
    _get_guard_fails(foo)

    def foo(a, *, _when = "123"): pass
    assert _get_guard(foo) == 123

    def foo(_when, *, a): pass
    _get_guard_fails(foo)

    def foo(_when = "123", *, a): pass
    assert _get_guard(foo) == 123

    def foo(_when = ""): pass
    with expected(GuardExpressionException("invalid guard expression for foo(): "
                                           "unexpected EOF while parsing (foo, line 0)")):
        _get_guard(foo)

    def foo(_when = "@\n"): pass
    with expected(GuardExpressionException("invalid guard expression for foo(): "
                                           "invalid syntax (foo, line 1)")):
        _get_guard(foo)

    ########

    print("ok")

    ############################################################################

    print("simple match args: ", end = "")

    ########

    def _apply_args(f, *args, **kwargs):
        return _eval_args(getfullargspec(f), args, kwargs)

    ########

    def f01(): pass
    assert _apply_args(f01) == dict()
    with expected(FunctionArgumentsMatchException("takes 0 positional arguments but 1 was given")):
        _apply_args(f01, 1)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: a")):
        _apply_args(f01, a = 1)

    ########

    def f02(a): pass
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f02)
    assert _apply_args(f02, 1) == dict(a = 1)
    with expected(FunctionArgumentsMatchException("takes 1 positional argument but 2 were given")):
        _apply_args(f02, 1, 2)
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f02, a = 1)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: a")):
        _apply_args(f02, 1, a = 2)

    ########

    def f03(*, a): pass
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f03)
    with expected(FunctionArgumentsMatchException("takes 0 positional arguments but 1 was given")):
        _apply_args(f03, 1)
    with expected(FunctionArgumentsMatchException("takes 0 positional arguments but 1 was given")):
        _apply_args(f03, 1, a = 2)
    assert _apply_args(f03, a = 1) == dict(a = 1)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: b")):
        _apply_args(f03, a = 1, b = 2)

    ########

    def f04(a, *, b): pass
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f04)
    with expected(FunctionArgumentsMatchException("missing required keyword argument: b")):
        _apply_args(f04, 1)
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f04, b = 1)
    assert _apply_args(f04, 1, b = 2) == dict(a = 1, b = 2)
    with expected(FunctionArgumentsMatchException("takes 1 positional argument but 2 were given")):
        _apply_args(f04, 1, 2, b = 3)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: c")):
        _apply_args(f04, 1, b = 2, c = 3)

    ########

    print("ok")

    ############################################################################

    print("match default args: ", end = "")

    ########

    def f11(a = 1): pass
    assert _apply_args(f11) == dict(a = 1)
    assert _apply_args(f11, 2) == dict(a = 2)
    with expected(FunctionArgumentsMatchException("takes 1 positional argument but 2 were given")):
        _apply_args(f11, 1, 2)

    ########

    def f12(a, b = 1): pass
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f12)
    assert _apply_args(f12, 2) == dict(a = 2, b = 1)
    assert _apply_args(f12, 1, 2) == dict(a = 1, b = 2)
    with expected(FunctionArgumentsMatchException("takes 2 positional arguments but 3 were given")):
        _apply_args(f12, 1, 2, 3)

    ########

    def f13(*, a = 1): pass
    assert _apply_args(f13) == dict(a = 1)
    assert _apply_args(f13, a = 2) == dict(a = 2)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: b")):
        _apply_args(f13, a = 1, b = 2)
    with expected(FunctionArgumentsMatchException("got unexpected keyword arguments: c, b")):
        _apply_args(f13, b = 1, c = 2)

    ########

    def f14(*, a, b = 2): pass
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f14)
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f14, b = 1)
    assert _apply_args(f14, a = 1) == dict(a = 1, b = 2)
    assert _apply_args(f14, a = 2, b = 1) == dict(a = 2, b = 1)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: c")):
        _apply_args(f14, a = 1, c = 3)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: c")):
        _apply_args(f14, a = 1, b = 3, c = 2)
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f14, b = 1, c = 2)

    ########

    print("ok")

    ############################################################################

    print("match varargs: ", end = "")

    ########

    def f21(*args): pass
    assert _apply_args(f21) == dict(args = ())
    assert _apply_args(f21, 1) == dict(args = (1, ))

    ########

    def f22(a, *args): pass
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f22)
    assert _apply_args(f22, 1) == dict(a = 1, args = ())
    assert _apply_args(f22, 1, 2) == dict(a = 1, args = (2, ))

    ########

    def f23(a = 1, *args): pass
    assert _apply_args(f23) == dict(a = 1, args = ())
    assert _apply_args(f23, 2) == dict(a = 2, args = ())
    assert _apply_args(f23, 2, 3) == dict(a = 2, args = (3, ))

    ########

    def f24(a, b = 1, *args): pass
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f24)
    assert _apply_args(f24, 2) == dict(a = 2, b = 1, args = ())
    assert _apply_args(f24, 1, 2) == dict(a = 1, b = 2, args = ())
    assert _apply_args(f24, 1, 2, 3) == dict(a = 1, b = 2, args = (3, ))

    ########

    def f25(**kwargs): pass
    assert _apply_args(f25) == dict(kwargs = dict())
    assert _apply_args(f25, a = 1) == dict(kwargs = dict(a = 1))

    ########

    def f26(*, a, **kwargs): pass
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f26)
    assert _apply_args(f26, a = 1) == dict(a = 1, kwargs = dict())
    assert _apply_args(f26, a = 1, b = 2) == dict(a = 1, kwargs = dict(b = 2))
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f26, b = 2)

    ########

    def f27(*, a = 1, **kwargs): pass
    assert _apply_args(f27) == dict(a = 1, kwargs = dict())
    assert _apply_args(f27, a = 2) == dict(a = 2, kwargs = dict())
    assert _apply_args(f27, a = 2, b = 1) == dict(a = 2, kwargs = dict(b = 1))

    ########

    def f28(*, a, b = 1, **kwargs): pass
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f28)
    assert _apply_args(f28, a = 2) == dict(a = 2, b = 1, kwargs = dict())
    assert _apply_args(f28, a = 1, b = 2) == dict(a = 1, b = 2, kwargs = dict())
    assert _apply_args(f28, a = 2, c = 3) == dict(a = 2, b = 1, kwargs = dict(c = 3))
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f28, b = 1)
    assert _apply_args(f28, a = 1, b = 2, c = 3) == dict(a = 1, b = 2, kwargs = dict(c = 3))

    ########

    def f29(a, b = 2, *args, c, d = 4, **kwargs): pass
    assert _apply_args(f29, 1, c = 3) == dict(a = 1, b = 2, c = 3, d = 4, args = (), kwargs = dict())
    assert _apply_args(f29, 2, 1, c = 4, d = 3) == dict(a = 2, b = 1, c = 4, d = 3, args = (), kwargs = dict())
    assert _apply_args(f29, 5, 6, 7, c = 8, d = 9, e = 0) == dict(a = 5, b = 6, c = 8, d = 9, args = (7, ), kwargs = dict(e = 0))

    ########

    print("ok")

    ############################################################################

    print("multiple args messages: ", end = "")

    ########

    def f31(): pass
    with expected(FunctionArgumentsMatchException("takes 0 positional arguments but 1 was given")):
        _apply_args(f31, 1)
    with expected(FunctionArgumentsMatchException("takes 0 positional arguments but 2 were given")):
        _apply_args(f31, 1, 2)

    def f32(a): pass
    with expected(FunctionArgumentsMatchException("missing required positional argument: a")):
        _apply_args(f32)
    with expected(FunctionArgumentsMatchException("takes 1 positional argument but 2 were given")):
        _apply_args(f32, 1, 2)

    def f33(a, b): pass
    with expected(FunctionArgumentsMatchException("missing required positional arguments: a, b")):
        _apply_args(f33)
    with expected(FunctionArgumentsMatchException("takes 2 positional arguments but 3 were given")):
        _apply_args(f33, 1, 2, 3)

    def f34(): pass
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: a")):
        _apply_args(f34, a = 1)
    with expected(FunctionArgumentsMatchException("got unexpected keyword arguments: a, b")):
        _apply_args(f34, a = 1, b = 2)

    def f35(*, a): pass
    with expected(FunctionArgumentsMatchException("missing required keyword argument: a")):
        _apply_args(f35)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: b")):
        _apply_args(f35, a = 1, b = 2)
    with expected(FunctionArgumentsMatchException, "got unexpected keyword arguments: (b, c|c, b)"):
        _apply_args(f35, a = 1, b = 2, c = 3)

    def f36(*, a, b): pass
    with expected(FunctionArgumentsMatchException("missing required keyword arguments: a, b")):
        _apply_args(f36)
    with expected(FunctionArgumentsMatchException("missing required keyword argument: b")):
        _apply_args(f36, a = 1)
    with expected(FunctionArgumentsMatchException("got unexpected keyword argument: c")):
        _apply_args(f36, a = 1, b = 2, c = 3)
    with expected(FunctionArgumentsMatchException, "got unexpected keyword arguments: (c, d|d, c)"):
        _apply_args(f36, a = 1, b = 2, c = 3, d = 4)

    ########

    print("ok")

    ############################################################################

    print("compatible argspecs: ", end = "")

    ########

    def _unguard(func):
        return _stripped_argspec(getfullargspec(func))

    def foo(): pass
    assert _unguard(foo) == ([], [], [], {}, None, None)

    def foo(_when): pass
    assert _unguard(foo) == (["_when"], [], [], {}, None, None)

    def foo(_when = "expr"): pass
    assert _unguard(foo) == ([], [], [], {}, None, None)

    def foo(a, _when = "expr"): pass
    assert _unguard(foo) == (["a"], [], [], {}, None, None)

    def foo(a = 1, _when = "expr"): pass
    assert _unguard(foo) == (["a"], [1], [], {}, None, None)

    def foo(*, _when): pass
    assert _unguard(foo) == ([], [], ["_when"], {}, None, None)

    def foo(*, _when = "expr"): pass
    assert _unguard(foo) == ([], [], [], {}, None, None)

    def foo(*, a, _when = "expr"): pass
    assert _unguard(foo) == ([], [], ["a"], {}, None, None)

    def foo(*, a = 1, _when = "expr"): pass
    assert _unguard(foo) == ([], [], ["a"], {"a": 1}, None, None)

    def foo(a, b: "ignored" = 1, _when = "expr", *args, c: "ignored", d = 2, **kwargs) -> "ignored": pass
    assert _unguard(foo) == (["a", "b"], [1], ["c", "d"], {"d": 2}, "args", "kwargs")

    def foo(a, b: "ignored" = 1, *args, c: "ignored", d = 2, _when = "expr", **kwargs) -> "ignored": pass
    assert _unguard(foo) == (["a", "b"], [1], ["c", "d"], {"d": 2}, "args", "kwargs")

    ########

    print("ok")

    ############################################################################

    print("simple function behavior: ", end = "")

    ########

    @guard
    def g01():
        return "g01"

    assert g01() == "g01"

    ########

    @guard
    def g02(_when = "False"):
        return "g02.False"

    with expected(NoMatchingFunctionException):
        g02()

    @guard
    def g02(_when = "True"):
        return "g02.True"

    assert g02() == "g02.True"

    ########

    @guard
    def g03(a, _when = "a == 1"):
        return "g03.1"

    with expected(NoMatchingFunctionException):
        g03(2)

    @guard
    def g03(a, _when = "a == 2"):
        return "g03.2"

    assert g03(2) == "g03.2"

    @guard
    def g03(a, _when = "a == 3"):
        return g03(1)

    assert g03(3) == "g03.1"

    ########

    @guard
    def g04(a = 1, _when = "a == 1"):
        return "g04.1"

    assert g04(1) == g04() == "g04.1"

    with expected(NoMatchingFunctionException):
        g04(2)

    ########

    @guard
    def g11(*, _when = "True"):
        return "g11"

    assert g11() == "g11"

    ########

    @guard
    def g12(*, _when = "False"):
        return "g12.False"

    with expected(NoMatchingFunctionException):
        g12()

    @guard
    def g12(*, _when = "True"):
        return "g12.True"

    assert g12() == "g12.True"

    ########

    @guard
    def g13(*, a, _when = "a == 1"):
        return "g13.1"

    with expected(NoMatchingFunctionException):
        g13(a = 2)

    @guard
    def g13(*, a, _when = "a == 2"):
        return "g13.2"

    assert g13(a = 2) == "g13.2"

    @guard
    def g13(*, a, _when = "a == 3"):
        return g13(a = 1)

    assert g13(a = 3) == "g13.1"

    ########

    @guard
    def g14(*, a = 1, _when = "a == 1"):
        return "g14.1"

    assert g14(a = 1) == g14() == "g14.1"

    with expected(NoMatchingFunctionException):
        g14(a = 2)

    ########

    print("ok")

    ############################################################################

    print("argvars: ", end = "")

    ########

    @guard
    def argskw(_when = "kwargs['el'] == len(args)", *args, **kwargs):
        return "equal"

    @guard
    def argskw(*args, _when = "kwargs['el'] != len(args)", **kwargs):
        return "not equal"

    assert argskw(1, 2, 3, el = 4) == "not equal"
    assert argskw(el = 0) == "equal"
    with expected(GuardEvalException("guard expression evaluation failed for argskw(): 'el'")):
        argskw(1)

    ########

    print("ok")

    ############################################################################

    print("guarded vs. unguarded: ", end = "")

    ########

    def m01(a = 0):
        return "UNGUARDED.1"
    assert m01() == "UNGUARDED.1"

    @guard
    def m01(a = 0, _when = "a == 0"):
        return "GUARDED.0"
    assert m01() == m01(0) == "GUARDED.0"

    def m01(a = 0):
        return "UNGUARDED.2"
    assert m01() == m01(0) == "UNGUARDED.2"

    @guard
    def m01(a = 0, _when = "a == 1"):
        return "GUARDED.1"
    assert m01() == m01(0) == "GUARDED.0"
    assert m01(1) == "GUARDED.1"

    def m01(a = 0):
        return "UNGUARDED.3"
    assert m01() == m01(0) == m01(1) == "UNGUARDED.3"

    ########

    print("ok")

    ############################################################################

    print("guard ordering: ", end = "")

    ########

    @guard
    def h01():
        return "A"

    assert h01() == "A"

    with expected(DuplicateDefaultGuardException("the default version of h01() has already been specified")):
        @guard
        def h01():
            pass

    ########

    @guard
    def h02(*, a):
        return "A"

    assert h02(a = 1) == h02(a = 0) == h02(a = -1) == "A"

    with expected(DuplicateDefaultGuardException("the default version of h02() has already been specified")):
        @guard
        def h02(*, a):
            pass

    @guard
    def h02(*, a, _when = "a > 0"):
        return "B"

    assert h02(a = 1) == "B"
    assert h02(a = 0) == h02(a = -1) == "A"

    @guard
    def h02(*, a, _when = "a < 0"):
        return "C"

    assert h02(a = 1) == "B"
    assert h02(a = 0) == "A"
    assert h02(a = -1) == "C"

    @guard
    def h02(*, a, _when = "a < 0"):
        return "D"

    assert h02(a = -1) == "C"

    ########

    @guard
    def h03(a, _when = "a > 0"):
        return "A"

    assert h03(1) == "A"
    with expected(NoMatchingFunctionException("none of the guard expressions for h03() matched the call arguments")):
        h03(0)
    with expected(NoMatchingFunctionException("none of the guard expressions for h03() matched the call arguments")):
        h03(-1)

    @guard
    def h03(a):
        return "B"

    assert h03(1) == "A"
    assert h03(0) == h03(-1) ==  "B"

    with expected(DuplicateDefaultGuardException("the default version of h03() has already been specified")):
        @guard
        def h03(a):
            pass

    @guard
    def h03(a, _when = "a < 0"):
        return "C"

    assert h03(1) == "A"
    assert h03(0) == "B"
    assert h03(-1) == "C"

    with expected(DuplicateDefaultGuardException("the default version of h03() has already been specified")):
        @guard
        def h03(a):
            pass

    @guard
    def h03(a, _when = "a < 0"):
        return "D"

    assert h03(-1) == "C"

    ########

    @guard
    def h04(a, _when = "a > 0"):
        return "A"

    assert h04(1) == "A"
    with expected(NoMatchingFunctionException("none of the guard expressions for h04() matched the call arguments")):
        h04(-1)
    with expected(NoMatchingFunctionException("none of the guard expressions for h04() matched the call arguments")):
        h04(0)

    @guard
    def h04(a, _when = "a < 0"):
        return "B"

    assert h04(1) == "A"
    assert h04(-1) == "B"
    with expected(NoMatchingFunctionException("none of the guard expressions for h04() matched the call arguments")):
        h04(0)

    @guard
    def h04(a):
        return "C"

    assert h04(1) == "A"
    assert h04(-1) == "B"
    assert h04(0) == "C"

    with expected(DuplicateDefaultGuardException("the default version of h04() has already been specified")):
        @guard
        def h04(a):
            pass

    @guard
    def h04(a, _when = "a == 0"):
        return "D"

    assert h04(1) == "A"
    assert h04(-1) == "B"
    assert h04(0) == "D"

    with expected(DuplicateDefaultGuardException("the default version of h04() has already been specified")):
        @guard
        def h04(a):
            pass

    @guard
    def h04(a, _when = "a == 0"):
        return "E"

    assert h04(0) == "D"

    ########

    print("ok")

    ############################################################################

    print("simple recursive function: ", end = "")

    ########

    @guard
    def factorial(a, _when = "a == 1"):
        return 1

    @guard
    def factorial(a, _when = "a > 1"):
        return a * factorial(a - 1)

    assert factorial(10) == 3628800

    ########

    print("ok")

    ############################################################################

    print("deep recursive function: ", end = "")

    ########

    @guard
    def ackermann(m, n, _when = "m == 0"):
        return n + 1

    @guard
    def ackermann(m, n, _when = "m > 0 and n == 0"):
        return ackermann(m - 1, 1)

    @guard
    def ackermann(m, n, _when = "m > 0 and n > 0"):
        return ackermann(m - 1, ackermann(m, n - 1))

    assert ackermann(3, 5) == 253

    ########

    print("ok")

    ############################################################################

    print("creating guards: ", end = "")

    ########

    N = 1000

    @guard
    def prime(n):
        return False

    for i in range(2, N):
        is_prime = True
        for j in range(2, round(sqrt(i) + 1)):
            if prime(j) and i % j == 0:
                is_prime = False
                break
        if is_prime:
            @guard
            def prime(n, _when = "n == {0:d}".format(i)):
                return True
        else:
            @guard
            def prime(n, _when = "n == {0:d}".format(i)):
                return False

    assert ",".join(str(i) for i in range(2, N) if prime(i)) == \
           "2,3,5,7,11,13,17,19,23,29,31,37,41," \
           "43,47,53,59,61,67,71,73,79,83,89,97,101," \
           "103,107,109,113,127,131,137,139,149,151,157,163,167," \
           "173,179,181,191,193,197,199,211,223,227,229,233,239," \
           "241,251,257,263,269,271,277,281,283,293,307,311,313," \
           "317,331,337,347,349,353,359,367,373,379,383,389,397," \
           "401,409,419,421,431,433,439,443,449,457,461,463,467," \
           "479,487,491,499,503,509,521,523,541,547,557,563,569," \
           "571,577,587,593,599,601,607,613,617,619,631,641,643," \
           "647,653,659,661,673,677,683,691,701,709,719,727,733," \
           "739,743,751,757,761,769,773,787,797,809,811,821,823," \
           "827,829,839,853,857,859,863,877,881,883,887,907,911," \
           "919,929,937,941,947,953,967,971,977,983,991,997"

    ########

    print("ok")

    ############################################################################

    print("function name scoping: ", end = "")

    ########

    @guard
    def k01():
        return "k01"
    assert k01() == "k01"

    def x():
        @guard
        def k01():
            return "x.k01"
        assert k01() == "x.k01"
        return k01
    assert x()() == "x.k01"

    class C:
        @guard
        def k01(self):
            return "C.k01"
    assert k01() == "k01"
    assert C().k01() == "C.k01"

    ########

    print("ok")

    ############################################################################

    print("decorator stacking: ", end = "")

    ########

    def trace(name):
        def trace_deco(f):
            @wraps(f)
            def trace_proxy(*args, trail, **kwargs):
                trail.append((name, args, kwargs))
                result = f(*args, trail = trail, **kwargs)
                return result
            if not hasattr(trace_proxy, "__wrapped__"):
                trace_proxy.__wrapped__ = f
            return trace_proxy
        return trace_deco

    ########

    @trace("A")
    def q01(*args, trail, **kwargs):
        trail.append(("q01", args, kwargs))
        return trail

    assert q01(trail = []) == \
    [
        ("A", (), {}),
        ("q01", (), {})
    ]

    ########

    @trace("B")
    @trace("C")
    def q02(*args, trail, **kwargs):
        trail.append(("q02", args, kwargs))
        return trail

    assert q02(1, trail = [], n1 = "v1") == \
    [
        ("B", (1, ), { "n1": "v1" }),
        ("C", (1, ), { "n1": "v1" }),
        ("q02", (1, ), { "n1": "v1" })
    ]

    ########

    @guard
    @trace("D")
    def q03(*args, trail, **kwargs):
        trail.append(("q03.1", args, kwargs))
        return trail

    assert q03(2, trail = [], n2 = "v2") == \
    [
        ("D", (2, ), { "n2": "v2" }),
        ("q03.1", (2, ), { "n2": "v2" })
    ]

    @guard
    @trace("E")
    def q03(*args, trail, _when = "len(args) > 0", **kwargs):
        trail.append(("q03.2", args, kwargs))
        return trail

    assert q03(3, trail = [], n3 = "v3") == \
    [
        ("E", (3, ), { "n3": "v3" }),
        ("q03.2", (3, ), { "n3": "v3" })
    ]

    assert q03(trail = [], n4 = "v4") == \
    [
        ("D", (), { "n4": "v4" }),
        ("q03.1", (), { "n4": "v4" })
    ]

    ########

    @trace("F")
    @guard
    def q04(*args, trail, **kwargs):
        trail.append(("q04.1", args, kwargs))
        return trail

    assert q04(4, trail = [], n5 = "v5") == \
    [
        ("F", (4, ), { "n5": "v5" }),
        ("q04.1", (4, ), { "n5": "v5" })
    ]

    @trace("G")
    @guard
    def q04(*args, trail, _when = "len(args) > 0", **kwargs):
        trail.append(("q04.2", args, kwargs))
        return trail

    assert q04(5, trail = [], n6 = "v6") == \
    [
        ("G", (5, ), { "n6": "v6" }),
        ("q04.2", (5, ), { "n6": "v6" })
    ]

    # note the outer decorator is taken from the point of invocation,
    # but the matched wrapped function is invoked

    assert q04(trail = [], n7 = "v7") == \
    [
        ("G", (), { "n7": "v7" }),
        ("q04.1", (), { "n7": "v7" })
    ]

    # it is exactly this glitch that allows for class an static
    # methods to be guarded despite they actually are objects

    class D:
        @classmethod # but only when classmethod is on the outside
        @guard
        def foo(cls, *args, _when = "kwargs['n'] == 1", **kwargs):
            return "foo1", cls, args, kwargs
        @classmethod
        @guard
        def foo(cls, *args, _when = "kwargs['n'] == 2", **kwargs):
            return "foo2", cls, args, kwargs

    assert D().foo(n = 1) == D.foo(n = 1) == ("foo1", D, (), { "n": 1 })
    assert D().foo(n = 2) == D.foo(n = 2) == ("foo2", D, (), { "n": 2 })

    with expected(NoMatchingFunctionException("none of the guard expressions for "
                                              "D.foo() matched the call arguments")):
        D().foo(n = 3)

    class D:
        with expected(AttributeError):
            @guard
            @classmethod # and not on the inside
            def foo(cls):
                pass

    ########

    print("ok")

    ############################################################################

    print("interaction with typecheck decorator: ", end = "")

    ########

    @guard
    @typecheck
    def r01(a: by_regex('^[0-9]+$'), _when = "isinstance(a, str)") -> lambda i: i == 99:
        return int(a)

    @guard
    @typecheck
    def r01(a: int) -> lambda s: s == "99":
        return str(a)

    assert r01("99") == 99
    with expected(ReturnValueError("r01() has returned an incompatible value: 0")):
        r01("0")
    with expected(InputParameterError("r01() has got an incompatible value for a: foo")):
        r01("foo")

    assert r01(99) == "99"
    with expected(ReturnValueError("r01() has returned an incompatible value: 0")):
        r01(0)
    with expected(InputParameterError("r01() has got an incompatible value for a: 0.99")):
        r01(0.99)

    # but guard must be outside of typecheck because otherwise
    # typecheck will only check against the actual call site

    @typecheck
    @guard
    def r02(a: int, _when = "isinstance(a, str)"):
        assert isinstance(a, int)

    @typecheck # this would be the only actual check
    @guard
    def r02(a: str):
        pass

    with expected(InputParameterError("r02() has got an incompatible value for a: 1")):
        r02(1)

    with expected(AssertionError):
        r02("foo")

    ########

    print("ok")

    ############################################################################

    print("default eval args: ", end = "")

    ########

    guard.default_eval_args(aaa = 11)
    guard.default_eval_args({ "bbb": 22 })

    @guard
    def s01(a = 33):
        return "s01.1"

    @guard
    def s01(a = 33, _when = "a == aaa + bbb"):
        return "s01.2"

    assert s01() == "s01.2"
    assert s01(11) == "s01.1"

    ########

    print("ok")

    ############################################################################

    print("eval expression scoping and side-effects: ", end = "")

    ########

    d = []
    guard.default_eval_args(d = d)

    c = []

    @guard
    def t01(a, b, _when = "a.append(b) or c.append(b * 2) or d.append(b * 3) or b == 1"):
        return "t01.1"

    @guard
    def t01(a, b, _when = "a.append(b * 4) or c.append(b * 5) or d.append(b * 6) or b == -1"):
        return "t01.2"

    a = []

    assert t01(a, 1) == "t01.1"
    assert (a, c, d) == ([1], [2], [3])

    assert t01(a, -1) == "t01.2"
    assert (a, c, d) == ([1, -1, -4], [2, -2, -5], [3, -3, -6])

    with expected(NoMatchingFunctionException):
        t01(a, 2)
    assert (a, c, d) == ([1, -1, -4, 2, 8], [2, -2, -5, 4, 10], [3, -3, -6, 6, 12])

    ########

    print("ok")

    ############################################################################

    print("classmethod guarding: ", end = "")

    ########

    class E:
        @guard
        def foo(cls, a, _when = "a > 0"): # note that this is a regular instance method
            return cls
        @classmethod
        @guard
        def foo(cls, a, _when = "a < 0"):
            return cls

    assert E.foo(1) is E.foo(-1) is E # which is wrong

    ########

    print("ok")

    ############################################################################

    print("unnamed functions: ", end = "")

    ########

    el = lambda: None
    assert guard(el) is el

    def el():
        pass
    assert guard(el) is not el

    ########

    print("ok")

    ############################################################################

    print("overriding module: ", end = "")

    ########

    def u01():
        return "sys.u01"
    u01 = guard_module(modules["sys"])(u01)

    u01_sys = u01

    def u01():
        return "functools.u01"
    u01 = guard_module(modules["functools"])(u01)

    assert u01() == "functools.u01"
    assert u01_sys() == "sys.u01"

    ########

    print("ok")

    ############################################################################

    print("all ok")

################################################################################
# EOF