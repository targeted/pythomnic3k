#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################
#
# Module smpp_pdu_param. Contains PDU parameter classes, mandatory and optional.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
#
###############################################################################

__all__ = []

###############################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..")))

import typecheck; from typecheck import *
import smpp34.smpp_tools; from smpp34.smpp_tools import *
import smpp34.smpp_types; from smpp34.smpp_types import *
import smpp34.smpp_errors; from smpp34.smpp_errors import *

###############################################################################

class PDUParameter:

    @typecheck
    def __init__(self, *, name: str,
                 type: lambda x: type(x) is type and issubclass(x, SMPPType),
                 check: optional(callable) = None):
        self._name = name
        self._type = type
        self._check = check

    name = property(lambda self: self._name)
    type = property(lambda self: self._type)

    # note that comparison ignores check

    def __eq__(self, other):
        return type(self) is type(other) and \
               self._name == other._name and \
               self._type is other._type

    def __neq__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._name) ^ hash(id(self._type))

    @typecheck
    def create(self, value) -> SMPPType:
        v = self._type(value)
        if self._check and not self._check(v):
            raise Exception("{0:s} does not pass the check".format(v)) # caught and rethrown in smpp_pdu.py
        return v

    @typecheck
    def read(self, r: with_attr("read"), length: optional(int) = None) -> SMPPType:
        v = self._type.read(r, length)
        if self._check and not self._check(v):
            raise Exception("{0:s} does not pass the check".format(v)) # caught and rethrown in smpp_pdu.py
        return v

###############################################################################

class MandatoryPDUParameter(PDUParameter):

    pass

###############################################################################

class OptionalPDUParameter(PDUParameter):

    @typecheck
    def __init__(self, *, code: word, name: str,
                 type: lambda x: type(x) is type and issubclass(x, SMPPType),
                 check: optional(callable) = None):
        PDUParameter.__init__(self, name = name, type = type, check = check)
        self._code = code

    code = property(lambda self: self._code)

    def __eq__(self, other):
        return PDUParameter.__eq__(self, other) and self._code == other._code

    def __hash__(self):
        return PDUParameter.__hash__(self) ^ hash(self._code)

##############################################################################

if __name__ == "__main__":

    print("self-testing module smpp_pdu_param.py:")

    from io import BytesIO
    from expected import expected

    ###################################

    def test_compare_mandatory():

        assert MandatoryPDUParameter(name = "foo", type = SMPPInteger1) == \
               MandatoryPDUParameter(name = "foo", type = SMPPInteger1, check = lambda v: None)

        assert MandatoryPDUParameter(name = "foo", type = SMPPInteger1, check = lambda v: 1) == \
               MandatoryPDUParameter(name = "foo", type = SMPPInteger1, check = lambda v: 2)

        assert MandatoryPDUParameter(name = "foo", type = SMPPInteger2) != \
               MandatoryPDUParameter(name = "foo", type = SMPPInteger1)

        assert MandatoryPDUParameter(name = "foo", type = SMPPInteger2) != \
               MandatoryPDUParameter(name = "bar", type = SMPPInteger2)

        mp = MandatoryPDUParameter(name = "foo", type = SMPPInteger1)
        assert mp in { mp }

    test_compare_mandatory()

    ###################################

    def test_compare_optional():

        assert OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1) == \
               OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1, check = lambda v: None)

        assert OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1, check = lambda v: 1) == \
               OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1, check = lambda v: 2)

        assert OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger2) != \
               OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1)

        assert OptionalPDUParameter(code = 0x0001, name = "bar", type = SMPPInteger2) != \
               OptionalPDUParameter(code = 0x0002, name = "bar", type = SMPPInteger2)

        op = OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1)
        assert op in { op }

    test_compare_optional()

    ###################################

    def test_compare_cross():

        assert MandatoryPDUParameter(name = "foo", type = SMPPInteger1) != \
               OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1)

        assert OptionalPDUParameter(code = 0x0001, name = "foo", type = SMPPInteger1) != \
               MandatoryPDUParameter(name = "foo", type = SMPPInteger1)

    test_compare_cross()

    ###################################

    def test_create():

        p = PDUParameter(name = "foo", type = SMPPInteger1, check = lambda v: v.value % 2 == 0)

        with expected(InputParameterError("__init__() has got an incompatible value for value: b'bar'")):
            p.create(b"bar")

        assert p.create(0x00) == SMPPInteger1(0)

        with expected(Exception("SMPPInteger1(0x01) does not pass the check")):
            p.create(0x01)

    test_create()

    ###################################

    def test_read():

        p = PDUParameter(name = "foo", type = SMPPInteger1, check = lambda v: v.value % 2 == 0)

        assert p.read(BytesIO(SMPPInteger1(0x00).serialize())) == SMPPInteger1(0x00)
        assert p.read(BytesIO(SMPPInteger1(0x00).serialize()), 1) == SMPPInteger1(0x00)
        with expected(InputParameterError("read() has got an incompatible value for length: 2")):
            p.read(BytesIO(SMPPInteger1(0x00).serialize()), 2)
        with expected(Exception("SMPPInteger1(0x01) does not pass the check")):
            p.read(BytesIO(SMPPInteger1(0x01).serialize()))

        p = PDUParameter(name = "foo", type = SMPPInteger1)
        assert p.read(BytesIO(SMPPInteger1(0x01).serialize())) == SMPPInteger1(0x01)

    test_read()

    ###################################

    print("ok")

###############################################################################
# EOF