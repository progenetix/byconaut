#!/usr/bin/env python3
import sys
from os import path
from copy import deepcopy
from liftover import get_lifter

from bycon import *

services_conf_path = path.join( path.dirname( path.abspath(__file__) ), "config" )
services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from service_response_generation import *

"""podmd
# `retriever`

**UNDER DEVELOPMENT OR NOT**

The bycon retriever is intended as a support function for beacon aggregators with
heterogeneous members. It should (at some point) re-map Beacon v2 query parameters
to the format of the indicated beacon, according to the remapping information
(parameter names, genome assembly liftover) from a confiuration file.

#### Tests

* http://progenetix.org/cgi/bycon/beaconServer/retriever.py?debug=&selectedBeacons=progenetixTest&url=http%3A//progenetix.test/beacon/g_variants/%3FdatasetIds%3Dprogenetix%26assemblyId%3DGRCh38%26referenceName%3D17%26variantType%3DDEL%26start%3D7500000%252C7676592%26end%3D7669607%252C7800000
"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        retriever()
    except Exception:
        print_text_response(traceback.format_exc(), 302)
   
################################################################################

def retriever():
    initialize_bycon_service(byc, "retriever")
    read_service_prefs("aggregator", services_conf_path, byc)
    run_beacon_init_stack(byc)

    r = ByconautServiceResponse(byc)

    b = BYC_PARS.get("selected_beacons", [])
    url = BYC_PARS.get("url", "")
    # print(url)
    if len(b) != 1:
        print_text_response('not a single "selectedBeacons" value')
    byc["service_config"].update({"selected_beacons": b})
    b = b[0]
    if not "http" in url:
        print_text_response('url seems missing / incomplete')    
    b_p = byc["service_config"]["beacon_params"]["instances"]
    if not b in b_p.keys():
        print_text_response(f'"{b}"is not in available beacon definitions')

    byc["service_response"]["meta"]["received_request_summary"]["requested_granularity"] ="boolean"
    check_switch_to_boolean_response(byc)
    byc["service_response"].update({"response": { "response_sets": [] }})

    ext_defs = b_p[b]

    # TODO: extract dataset id from URL using the ext_defs parameter mapping
    ds_id = BYC_PARS.get("dataset_ids", [])
    if len(ds_id) != 1:
        ds_id = ext_defs["dataset_ids"]
    ds_id = ds_id[0]

    resp_start = time.time()
    # print_text_response(url)
    r = retrieve_beacon_response(url, byc)
    resp_end = time.time()
    # prjsoncam(r)
    # print(url)
    r_f = format_response(r, url, ext_defs, ds_id, byc)
    r_f["info"].update({"response_time": "{}ms".format(round((resp_end - resp_start) * 1000, 0)) })
    byc["service_response"]["response"]["response_sets"].append(r_f)

    for r in byc["service_response"]["response"]["response_sets"]:
        if r["exists"] is True:
            byc["service_response"]["response_summary"].update({"exists": True})
            continue

    print_json_response(byc["service_response"])


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()