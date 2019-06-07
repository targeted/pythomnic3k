#!/usr/bin/env python3
#-*- coding: iso-8859-1 -*-
###############################################################################

__all__ = [ "process_request" ]

###############################################################################
# imports section

if __name__ == "__main__": # add pythomnic/lib to sys.path
    import os; import sys
    main_module_dir = os.path.dirname(sys.modules["__main__"].__file__) or os.getcwd()
    sys.path.insert(0, os.path.normpath(os.path.join(main_module_dir, "..", "..", "lib")))

###############################################################################
# request processing method, this is an entry point to your application

def process_request(request: dict, response: dict):

    # request contains "packet" of type bytes, response is and should remain empty

    packet = request["packet"]

###############################################################################
# EOF