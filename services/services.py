#!/usr/bin/env python3

from os import path, pardir, environ
import sys, re
from importlib import import_module

from bycon import *
pkg_path = path.dirname( path.abspath(__file__) )

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
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def services():

    set_debug_state(debug=0)

    frm = inspect.stack()[1]
    service = frm.function

    loc_dir = path.join( pkg_path, "local" )
    conf_dir = path.join( pkg_path, "config" )

    mod = inspect.getmodule(frm[0])

    # updates `beacon_defaults`, `dataset_definitions` and `local_paths`
    update_rootpars_from_local(loc_dir, byc)
    read_service_prefs(service, conf_dir, byc)

    defaults = byc["beacon_defaults"].get("defaults", {})
    for d_k, d_v in defaults.items():
        byc.update( { d_k: d_v } )

    s_a_s = byc["beacon_defaults"].get("service_path_aliases", {})
    r_w = byc["beacon_defaults"].get("rewrites", {})
    # d_p_s = byc["beacon_defaults"].get("data_pipeline_path_ids", [])

    byc.update({"request_path_root": "services"})
    rest_path_elements(byc)
    get_bycon_args(byc)
    args_update_form(byc)

    r_p_id = byc.get("request_entity_path_id", "info")

    prdbug(byc, r_p_id)
    prdbug(byc, s_a_s.keys())

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
    # elif f in d_p_s:
    #     beacon_data_pipeline(byc, f)
    elif f:
        # dynamic package/function loading; e.g. `filteringTerms` loads
        # `filteringTerms` from `filteringTerm.py` which is an alias to
        # the `filtering_terms` function there...
        try:
            mod = import_module(f)
            serv = getattr(mod, f)
            serv()
            exit()
        except Exception as e:
            print('Content-Type: text')
            print('status:422')
            print()
            print('Service {} WTF error: {}'.format(f, e))

            exit()

    byc.update({
        "service_response": {},
        "error_response": {
            "error": {
                "error_code": 422,
                "error_message": "No correct service path provided. Please refer to the documentation at http://docs.progenetix.org"
            }
        }
    })

    cgi_print_response(byc, 422)

################################################################################
################################################################################

if __name__ == '__main__':
    main()
