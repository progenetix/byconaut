#!/usr/bin/env python3
import re, sys
from os import path, environ, pardir
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
    generate_genome_bins(byc)

    id_from_path = rest_path_value("intervalFrequencies")
    if id_from_path:
        byc[ "filters" ] = [ {"id": id_from_path } ]
    elif "id" in byc["form_data"]:
        byc[ "filters" ] = [ {"id": byc["form_data"]["id"]} ]

    if not "filters" in byc:
        response_add_error(byc, 422, "No value was provided for collation `id` or `filters`.")  
        cgi_break_on_errors(byc)

    file_type = byc["form_data"].get("output", "pgxfreq")
    if file_type not in ["pgxfreq", "pgxmatrix"]:
        file_type = "pgxfreq"
    byc.update({"output": file_type})

    pdb = ByconBundler(byc).collationsPlotbundles()
    prdbug(byc, pdb)
    check_pgxseg_frequencies_export(byc, pdb.get("interval_frequencies_bundles", []))
    check_pgxmatrix_frequencies_export(byc, pdb.get("interval_frequencies_bundles", []))
    # ByconPlot(byc, pdb).svgResponse()

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
################################################################################

if __name__ == '__main__':
    main()
