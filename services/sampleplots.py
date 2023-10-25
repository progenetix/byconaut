#!/usr/bin/env python3

import sys
from os import path

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from bycon_plot import *

"""
The plot service uses the standard bycon data retrieval pipeline with `biosample`
as entity type. Therefore, all standard Beacon query parameters work and also
the path is interpreted for an biosample `id` value if there is an entry at
`.../sampleplots/{id}`

The plot type can be set with `plotType=samplesplot` (or `histoplot` but that is
the fallback). Plot options are available as usual.

* http://progenetix.org/services/sampleplots/pgxbs-kftvjv8w
* http://progenetix.org/services/sampleplots/pgxbs-kftvjv8w?plotType=samplesplot&datasetIds=cellz
* http://progenetix.org/services/sampleplots?plotType=samplesplot&datasetIds=cellz&filters=cellosaurus:CVCL_0030
* http://progenetix.org/services/sampleplots?filters=pgx:icdom-81703

"""

################################################################################
################################################################################
################################################################################

def main():

    sampleplots()

################################################################################

def sampleplots():

    initialize_bycon_service(byc, "biosamples")
    run_beacon_init_stack(byc)
    generate_genomic_mappings(byc)

    plot_type = byc["form_data"].get("plot_type", "histoplot")
    if plot_type not in ["histoplot", "samplesplot", "histoheatplot"]:
        plot_type = "histoplot"
    byc.update({"output": plot_type})
    RSS = ByconResultSets(byc).datasetsResults()
    pdb = ByconBundler(byc).resultsets_frequencies_bundles(RSS)

    # getting the variants for the ssamples is time consuming so only optional
    if "samples" in plot_type:
        pdb.update( ByconBundler(byc).resultsets_callset_bundles(RSS) )

    ByconPlot(byc, pdb).svgResponse()

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
