#!/usr/bin/env python3
import re, json, sys
from os import path, environ
from pymongo import MongoClient

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from geomap_utils import *
from service_helpers import *
from service_response_generation import *

"""podmd
* <https://progenetix.org/services/geolocations?city=zurich>
* <https://progenetix.org/services/geolocations?geoLongitude=8.55&geoLatitude=47.37&geoDistance=100000>
* <https://progenetix.org/services/geolocations?geoLongitude=8.55&geoLatitude=47.37&geoDistance=100000&output=map>
* <http://progenetix.org/services/geolocations?bubble_stroke_weight=0&marker_scale=5&canvas_w_px=1000&file=https://raw.githubusercontent.com/progenetix/pgxMaps/main/rsrc/locationtest.tsv&debug=&output=map&help=true>
* <http://progenetix.org/cgi/bycon/services/geolocations.py?city=New&ISO3166alpha2=UK&output=map&markerType=marker>
podmd"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        geolocations()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def geolocations():

    initialize_bycon_service(byc, "geolocations")
    byc["geoloc_definitions"].update({"geo_root": "geo_location"})

    r = ByconautServiceResponse(byc)
    byc.update({
        "service_response": r.emptyResponse(),
        "error_response": r.errorResponse()
    })

    services_db = byc.get("services_db")
    geo_coll = byc.get("geolocs_coll")
    
    if "file" in byc["form_data"]:
        results = read_geomarker_table_web(byc)
    else:
        query, geo_pars = geo_query(byc)

        if len(query.keys()) < 1:
            response_add_error(byc, 422, "No query generated - missing or malformed parameters" )    
        cgi_break_on_errors(byc)

        results, e = mongo_result_list( services_db, geo_coll, query, { '_id': False } )
        response_add_error(byc, 422, e)

    print_map_from_geolocations(byc, results)

    if len(results) == 1:
        if "geo_distance" in byc["form_data"]:
            l_l = results[0]["geo_location"]["geometry"]["coordinates"]
            geo_pars = {
                "geo_longitude": l_l[0],
                "geo_latitude": l_l[1],
                "geo_distance": int(byc["form_data"]["geo_distance"])
            }
            query = return_geo_longlat_query(geo_root, geo_pars)
            results, e = mongo_result_list( services_db, geo_coll, query, { '_id': False } )
            response_add_error(byc, 422, e)
    
    cgi_break_on_errors(byc)

    if "text" in byc["output"]:
        open_text_streaming(byc["env"], "browser")
        for g in results:
            s_comps = []
            for k in ["city", "country", "continent"]:
                s_comps.append(str(g["geo_location"]["properties"].get(k, "")))
            s_comps.append(str(g.get("id", "")))
            for l in g["geo_location"]["geometry"].get("coordinates", [0,0]):
                s_comps.append(str(l))
            print("\t".join(s_comps))
        exit()

    byc.update({"service_response": r.populatedResponse(results)})
    cgi_print_response( byc, 200 )

################################################################################
################################################################################

if __name__ == '__main__':
    main()
