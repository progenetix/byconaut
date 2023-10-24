#!/usr/bin/env python3

import cgi
import re, json, yaml
from os import path, environ, pardir
import sys, datetime, argparse
from pymongo import MongoClient

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from bycon_plot import *
from service_helpers import *
from service_response_generation import *

"""podmd

* https://progenetix.org/services/collationPlots/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167,pgx:icdom-85003
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
        collationplots()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)

################################################################################

def collationplots():

    initialize_bycon_service(byc, "collationplots")
    run_beacon_init_stack(byc)

    id_from_path = rest_path_value("collationplots")
    if id_from_path:
        byc[ "filters" ] = [ {"id": id_from_path } ]
    elif "id" in byc["form_data"]:
        byc[ "filters" ] = [ {"id": byc["form_data"]["id"]} ]

    if not "filters" in byc:
        response_add_error(byc, 422, "No value was provided for collation `id` or `filters`.")  
        cgi_break_on_errors(byc)

    fmap_name = "frequencymap"
    plot_type = byc["form_data"].get("plot_type", "histoplot")
    if plot_type not in ["histoplot", "samplesplot", "histoheatplot"]:
        plot_type = "histoplot"
    byc.update({"output": plot_type})

    prdbug(byc, f'===> method: {fmap_name}')
    prdbug(byc, byc["filters"])

    results = [ ]
    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))

    for ds_id in byc["dataset_ids"]:
        coll_db = mongo_client[ds_id]
        for f in byc[ "filters" ]:
            f_val = f["id"]
            f_q = { "id": f_val }
            collation_f = coll_db[ "frequencymaps" ].find_one( { "id": f_val } )
            collation_c = coll_db[ "collations" ].find_one( { "id": f_val } )

            if not collation_f:
                continue
            if not collation_c:
                continue
            if not fmap_name in collation_f:
                continue

            r_o = {
                "dataset_id": ds_id,
                "group_id": f_val,
                "label": re.sub(r';', ',', collation_c["label"]),
                "sample_count": collation_f[ fmap_name ].get("analysis_count", 0),
                "interval_frequencies": collation_f[ fmap_name ]["intervals"] }
                
            results.append(r_o)


    mongo_client.close( )
    # prdbug(byc, results)

    plot_data_bundle = { "interval_frequencies_bundles": results }
    ByconPlot(byc, plot_data_bundle).svgResponse()


################################################################################
################################################################################

if __name__ == '__main__':
    main()
