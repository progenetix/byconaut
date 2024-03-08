#!/usr/bin/env python3
import re
from os import path, environ
from importlib import import_module

from bycon import *

pkg_path = path.dirname( path.abspath(__file__) )

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from service_helpers import read_service_prefs

"""
The `services` application deparses a request URI and calls the respective
script. The functionality is combined with the correct configuration of a 
rewrite in the server configuration for creation of canonical URLs.
"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        services()
    except Exception:
        print_text_response(traceback.format_exc(), 302)
    
################################################################################

def services():
    set_debug_state(debug=0)

    p_e_m = BYC.get("path_entry_type_mappings", {})
    e_p_m = BYC.get("entry_type_path_mappings", {})

    byc.update({"request_path_root": "services"})
    rest_path_elements(byc)
    e_p_id = BYC_PARS.get("request_entity_path_id", "___none___")
    if e_p_id in p_e_m:
        byc.update({"request_entity_path_id": e_p_id})
    r_p_id = byc.get("request_entity_path_id", "ids")
    prdbug(f'services.py - request_entity_path_id: {r_p_id}')

    e = p_e_m.get(r_p_id)   # entry type
    f = e_p_m.get(e)        # canonical entry type path

    if not f:
        pass
    elif f:
        # dynamic package/function loading; e.g. `intervalFrequencies` loads
        # `intervalFrequencies` from `interval_frequencies.py` which is an alias to
        # the `interval_frequencies` function there...
        try:
            mod = import_module(f)
            serv = getattr(mod, f)
            serv()
            exit()
        except Exception as e:
            print('Content-Type: text')
            print('status:422')
            print()
            print(f'Service {f} WTF error: {e}')

            exit()

    BYC["ERRORS"].append("No correct service path provided. Please refer to the documentation at http://docs.progenetix.org")
    BeaconErrorResponse(byc).response(422)


################################################################################
################################################################################

if __name__ == '__main__':
    main()
