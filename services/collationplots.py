#!/usr/bin/env python3

from os import path, environ, pardir
import sys, datetime, argparse
from pymongo import MongoClient

from bycon import (
    BeaconErrorResponse,
    byc,
    initialize_bycon_service,
    print_text_response,
    rest_path_value,
    run_beacon_init_stack,
    BYC,
    BYC_PARS
)

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import *
from bycon_plot import *
from file_utils import ExportFile
from interval_utils import generate_genome_bins
from service_helpers import *
from service_response_generation import *

"""podmd

* https://progenetix.org/services/collationplots/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167,pgx:icdom-85003
* https://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167&output=histoplot
* https://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&id=pgxcohort-TCGAcancers
* http://progenetix.org/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT&filterPrecision=start&withSamples=20&collationTypes=NCIT&output=histoplot&plot_area_height=20&plot_labelcol_font_size=6&plot_axislab_y_width=2&plot_label_y_values=0&plot_axis_y_max=80&plot_region_gap_width=1&debug=
* http://progenetix.test/services/intervalFrequencies/?datasetIds=progenetix&filters=NCIT:C7376,PMID:22824167&output=histoheatplot
* http://progenetix.test/services/collationplots/?datasetIds=progenetix&collationTypes=NCIT&minNumber=500&plotType=histoheatplot&method=codematches
podmd"""

################################################################################
################################################################################
################################################################################

def main():
    try:
        collationplots()
    except Exception:
        print_text_response(traceback.format_exc(), 302)

################################################################################

def collationplots():
    initialize_bycon_service(byc, "collationplots")
#    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    if (id_from_path := rest_path_value("collationplots")):
        byc["filters"] = [ {"id": id_from_path } ]
    elif "id" in BYC_PARS:
        byc["filters"] = [ {"id": BYC_PARS["id"]} ]
    if BYC_PARS.get("plot_type", "___none___") not in ["histoplot", "histoheatplot", "histosparkplot"]:
        BYC_PARS.update({"plot_type": "histoplot"})

    svg_f = ExportFile("svg").checkOutputFile()
    pdb = ByconBundler(byc).collationsPlotbundles()
    if len(BYC["ERRORS"]) >1:
        BeaconErrorResponse(byc).response(422)

    BP = ByconPlot(byc, pdb)
    if svg_f:
        BP.svg2file(svg_f)
    else:
        BP.svgResponse()


################################################################################
################################################################################

if __name__ == '__main__':
    main()
