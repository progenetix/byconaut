#!/usr/bin/env python3

import re, sys
from os import path, environ, pardir
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
    r = ByconautServiceResponse(byc)
    print_json_response(r.collationsResponse(), byc["env"])


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
