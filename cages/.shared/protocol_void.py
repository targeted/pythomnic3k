#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
################################################################################
#
# This module "implements" a void resource used only for self-testing.
#
# Pythomnic3k project
# (c) 2005-2014, Dmitry Dvoinikov <dmitry@targeted.org>
# Distributed under BSD license
#
###############################################################################

__all__ = [ "Resource" ]

###############################################################################

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

import pmnc.resource_pool; from pmnc.resource_pool import TransactionalResource

###############################################################################

class Resource(TransactionalResource):

    def success(self, *args, **kwargs):
        return args, kwargs

    def failure(self):
        raise Exception("failure")

    def execute(self, f, *args, **kwargs):
        return f(*args, **kwargs)

###############################################################################

def self_test():

    from expected import expected
    from pmnc.request import fake_request
    from pmnc.resource_pool import ResourceError

    fake_request(10.0)

    assert pmnc.transaction.void.success("foo", biz = "baz") == \
           (("foo", ), { "biz": "baz" })

    with expected(ResourceError, "failure"):
        pmnc.transaction.void.failure()

    assert pmnc.transaction.void.execute(lambda *args, **kwargs: (args, kwargs), "foo", biz = "baz") == \
           (("foo", ), { "biz": "baz" })

if __name__ == "__main__": import pmnc.self_test; pmnc.self_test.run()

###############################################################################
# EOF
