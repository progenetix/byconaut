#!/usr/bin/env python3

import cgi
import re, json, yaml
from os import path, environ, pardir
import sys, datetime, argparse
from pymongo import MongoClient

from bycon import *

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

    initialize_bycon_service(byc)

    select_dataset_ids(byc)
    parse_filters(byc)
    parse_variants(byc)
    generate_genomic_mappings(byc)

    create_empty_service_response(byc)
    cgi_break_on_errors(byc)

    id_rest = rest_path_value("intervalFrequencies")
    ff = byc.get("filter_flags", {})

    if id_rest is not None:
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

    results = [ ]
    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))

    include_f = [x for x in byc.get("filters", []) if not "!" in x["id"]]
    # exclude filters are for exact matches
    exclude_f = [re.sub("!", "", x["id"]) for x in byc.get("filters", []) if "!" in x["id"]]

    for ds_id in byc[ "dataset_ids" ]:
        coll_db = mongo_client[ ds_id ]
        for f in include_f:
            f_val = f["id"]

            if "start" in ff.get("precision", "exact"):
                f_q = { "id":{'$regex': f'^{f_val}' } }
            else:
                f_q = { "id": f_val }

            coll_ids = coll_db[ "frequencymaps" ].distinct("id", f_q)

            for c_id in coll_ids:

                if c_id in exclude_f:
                    continue
 
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

                # if not collation_f:
                #     response_add_error(byc, 422, "No collation {} was found in {}.frquencymaps".format(c_id, ds_id))
                # if not collation_c:
                #     response_add_error(byc, 422, "No collation {} was found in {}.collations".format(c_id, ds_id))
                # cgi_break_on_errors(byc)

                s_c = collation_c.get("count", 0)
                min_no = int(byc["form_data"].get("min_number", 0))
                if min_no > 0:
                    if s_c < min_no:
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
    ByconPlot(byc, plot_data_bundle).svg_response()

    check_pgxseg_frequencies_export(byc, results)
    check_pgxmatrix_frequencies_export(byc, results)
    populate_service_response( byc, results)
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
