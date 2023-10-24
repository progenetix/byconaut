#!/usr/bin/env python3

import cgi
import re, json, yaml
from os import path, environ, pardir
import sys, datetime, argparse
from pymongo import MongoClient

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from service_helpers import *
from service_response_generation import *

"""podmd

* https://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167,pgx:icdom-85003
* https://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167&output=histoplot
* https://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&id=pgxcohort-TCGAcancers
* https://progenetix.org/cgi/bycon/services/intervalFrequencies.py/?output=pgxseg&datasetIds=progenetix&filters=NCIT:C7376
* http://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT&filterPrecision=start&withSamples=20&collationTypes=NCIT&output=histoplot&plot_area_height=20&plot_labelcol_font_size=6&plot_axislab_y_width=2&plot_label_y_values=0&plot_axis_y_max=80&plot_region_gap_width=1&debug=
* http://progenetix.test/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167&output=histoheatplot
podmd"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        interval_frequencies()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)

################################################################################

def intervalFrequencies():
    
    try:
        interval_frequencies()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)
   
################################################################################

def interval_frequencies():

    initialize_bycon_service(byc, sys._getframe().f_code.co_name)
    run_beacon_init_stack(byc)

    r = ByconautServiceResponse(byc)
    byc.update({
        "service_response": r.emptyResponse(),
        "error_response": r.errorResponse()
    })

    id_rest = rest_path_value("intervalFrequencies")
    ff = byc.get("filter_flags", {})

    if id_rest:
        byc[ "filters" ] = [ {"id": id_rest } ]
    elif "id" in byc["form_data"]:
        byc[ "filters" ] = [ {"id": byc["form_data"]["id"]} ]

    if not "filters" in byc:
        response_add_error(byc, 422, "No value was provided for collation `id` or `filters`.")  
    cgi_break_on_errors(byc)

    # data retrieval & response population
    fmap_name = "frequencymap"
    if "codematches" in byc["method"]:
        fmap_name = "frequencymap_codematches"

    prdbug(byc, f'===> method: {byc["method"]}')
    prdbug(byc, f'===> method: {fmap_name}')

    results = [ ]
    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))

    prdbug(byc, byc.get("filters", []))
    prdbug(byc, byc.get("form_data", {}))

    include_f = [x["id"] for x in byc.get("filters", [])]

    for ds_id in byc["dataset_ids"]:
        coll_db = mongo_client[ds_id]
        for f in include_f:

        coll_ids = coll_db[ "frequencymaps" ].distinct("id", {"$in": include_f})
        for c_id in coll_ids:

            collation_f = coll_db[ "frequencymaps" ].find_one( { "id": c_id } )
            collation_c = coll_db[ "collations" ].find_one( { "id": c_id } )

            if not collation_f:
                continue
            if not collation_c:
                continue

            s_cm = collation_c.get("code_matches", 0)
            with_s = int(byc["form_data"].get("with_samples", 0))
            if with_s > 0:
                if s_cm < with_s:
                    continue

            if not fmap_name in collation_f:
                continue

            s_c = collation_c.get("count", 0)
            min_no = int(byc["form_data"].get("min_number", 0))
            if min_no > 0 and s_c < min_no:
                continue

            s_t = collation_c.get("collation_type", "___none___")
            c_t_s = byc["form_data"].get("collation_types", [])
            if len(c_t_s) > 0:
                if s_t not in c_t_s:
                    continue

            if "analysis_count" in collation_f[ fmap_name ]:
               s_c = collation_f[ fmap_name ]["analysis_count"]

            r_o = {
                "dataset_id": ds_id,
                "group_id": c_id,
                "label": re.sub(r';', ',', collation_c["label"]),
                "sample_count": s_c,
                "interval_frequencies": collation_f[ fmap_name ]["intervals"] }
                
            results.append(r_o)

    mongo_client.close( )

    plot_data_bundle = { "interval_frequencies_bundles": results }
    ByconPlot(byc, plot_data_bundle).svgResponse()

    check_pgxseg_frequencies_export(byc, results)
    check_pgxmatrix_frequencies_export(byc, results)
    byc.update({"service_response": r.populatedResponse(results)})
    cgi_print_response( byc, 200 )

################################################################################

def check_pgxseg_frequencies_export(byc, results):

    if not "pgxseg" in byc["output"] and not "pgxfreq" in byc["output"]:
        return

    export_pgxseg_frequencies(byc, results)

################################################################################

def check_pgxmatrix_frequencies_export(byc, results):

    if not "pgxmatrix" in byc["output"]:
        return

    export_pgxmatrix_frequencies(byc, results)

################################################################################
################################################################################

if __name__ == '__main__':
    main()
