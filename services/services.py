#!/usr/bin/env python3
import re
from os import path, environ
from importlib import import_module

from bycon import *

pkg_path = path.dirname( path.abspath(__file__) )

services_conf_path = path.join( path.dirname( path.abspath(__file__) ), "config" )
services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
services_loc_path = path.join( path.dirname( path.abspath(__file__) ), "local" )
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

    frm = inspect.stack()[1]
    service = frm.function
    mod = inspect.getmodule(frm[0])

    # updates `beacon_defaults`, `dataset_definitions` and `local_paths`
    # update_rootpars_from_local(services_loc_path, byc)
    # defaults = byc["beacon_defaults"].get("defaults", {})
    # for d_k, d_v in defaults.items():
    #     byc.update( { d_k: d_v } )
    read_service_prefs(service, services_conf_path, byc)
    defs = byc.get("beacon_defaults", {})
    s_a_s = defs.get("service_path_aliases", {})
    r_w = defs.get("rewrites", {})

    byc.update({"request_path_root": "services"})
    rest_path_elements(byc)
    # args_update_form(byc)

    r_p_id = byc.get("request_entity_path_id", "info")

    # check for rewrites
    if r_p_id in r_w:
        uri = environ.get('REQUEST_URI')
        pat = re.compile( rf"^.+\/{r_p_id}\/?(.*?)$" )
        if pat.match(uri):
            stuff = pat.match(uri).group(1)
            print_uri_rewrite_response(r_w[r_p_id], stuff)

    f = s_a_s.get(r_p_id)
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
