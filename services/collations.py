#!/usr/bin/env python3

import re
from os import path, environ, pardir
import sys
from pymongo import MongoClient

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from service_response_generation import *

################################################################################
################################################################################
################################################################################

def main():

    try:
        collations()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def collations():

    initialize_bycon_service(byc, "collations")
    run_beacon_init_stack(byc)
    prdbug(byc, byc["filters"])

    r = ByconautServiceResponse(byc)

    byc.update({
        "service_response": r.collationsResponse(),
        "error_response": r.errorResponse()
    })

    cgi_print_response( byc, 200 )

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
