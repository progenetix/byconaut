#!/usr/bin/env python3

import cgi
import re, json, yaml
from os import path, environ, pardir
import sys, datetime, argparse
from pymongo import MongoClient

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import *
from bycon_plot import *
from interval_utils import generate_genome_bins
from service_helpers import *
from service_response_generation import *

"""podmd

* https://progenetix.org/services/collationplots/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167,pgx:icdom-85003
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
    generate_genome_bins(byc)

    plot_type = byc["form_data"].get("plot_type", "___none___")
    if plot_type not in ["histoplot", "histoheatplot", "histosparkplot"]:
        plot_type = "histoplot"
    
    byc["form_data"].update({"plot_type": plot_type})
    id_from_path = rest_path_value("collationplots")
    if id_from_path:
        byc[ "filters" ] = [ {"id": id_from_path } ]
    elif "id" in byc["form_data"]:
        byc[ "filters" ] = [ {"id": byc["form_data"]["id"]} ]

    if not "filters" in byc:
        e_m = "No value was provided for collation `id` or `filters`."
        e_r = BeaconErrorResponse(byc).error(e_m, 422)
        print_json_response(e_r, byc["env"])

    pdb = ByconBundler(byc).collationsPlotbundles()
    ByconPlot(byc, pdb).svgResponse()


################################################################################
################################################################################

if __name__ == '__main__':
    main()
