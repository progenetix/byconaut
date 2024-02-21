#!/usr/bin/env python3
import re, sys
from os import path, environ, pardir
from pymongo import MongoClient

from bycon import *

services_conf_path = path.join( path.dirname( path.abspath(__file__) ), "config" )
services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import *
from bycon_plot import *
from interval_utils import generate_genome_bins
from service_helpers import *
from service_response_generation import *

"""podmd
* https://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167,pgx:icdom-85003
* https://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&id=pgxcohort-TCGAcancers
* https://progenetix.org/cgi/bycon/services/intervalFrequencies.py/?datasetIds=progenetix,cellz&filters=NCIT:C7376
* http://progenetix.test/services/intervalFrequencies/?datasetIds=progenetix&output=pgxmatrix&filters=NCIT:C7376,PMID:22824167
podmd"""

################################################################################
################################################################################
################################################################################

def main():
    try:
        interval_frequencies()
    except Exception:
        print_text_response(traceback.format_exc(), 302)

################################################################################

def intervalFrequencies():
    try:
        interval_frequencies()
    except Exception:
        print_text_response(traceback.format_exc(), 302)
   
################################################################################

def interval_frequencies():
    initialize_bycon_service(byc, "interval_frequencies")
    read_service_prefs("interval_frequencies", services_conf_path, byc)
    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    id_from_path = rest_path_value("intervalFrequencies")
    if id_from_path:
        byc[ "filters" ] = [ {"id": id_from_path } ]
    elif "id" in BYC_PARS:
        byc[ "filters" ] = [ {"id": BYC_PARS["id"]} ]

    if not "filters" in byc:
        BYC["ERRORS"].append("No value was provided for collation `id` or `filters`.")
        BeaconErrorResponse(byc).response(422)

    file_type = BYC_PARS.get("output", "___none___")
    if file_type not in ["pgxfreq", "pgxmatrix", "pgxseg"]:
        file_type = "pgxfreq"
    output = file_type
    pdb = ByconBundler(byc).collationsPlotbundles()

    if "pgxseg" in output or "pgxfreq" in output:
        export_pgxseg_frequencies(byc, pdb["interval_frequencies_bundles"])
    elif "matrix" in output:
        export_pgxmatrix_frequencies(byc, pdb["interval_frequencies_bundles"])


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
