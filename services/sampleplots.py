#!/usr/bin/env python3
import sys
from os import path
from pathlib import Path

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from bycon_plot import *
from interval_utils import generate_genome_bins

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
* http://progenetix.org/services/sampleplots/?testMode=true&plotType=samplesplot
* http://progenetix.org/services/sampleplots?filters=pgx:icdom-81703&plotType=histoplot&plotPars=plot_chro_height=0::plot_title_font_size=0::plot_area_height=18::plot_margins=0::plot_axislab_y_width=0::plot_grid_stroke=0::plot_footer_font_size=0::plot_width=400
"""

################################################################################
################################################################################
################################################################################

def main():

    sampleplots()

################################################################################

def sampleplots():
    initialize_bycon_service(byc, "biosamples")
#    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    plot_type = BYC_PARS.get("plot_type", "histoplot")
    file_id = BYC_PARS.get("file_id", "___no-input-file___")
    inputfile = Path( path.join( *byc["local_paths"][ "server_tmp_dir_loc" ], file_id ) )

    pb = ByconBundler(byc)
    if inputfile.is_file():
        pdb = pb.pgxseg_to_plotbundle(inputfile)
    else:
        RSS = ByconResultSets(byc).datasetsResults()
        pdb = pb.resultsets_frequencies_bundles(RSS)

        # getting the variants for the samples is time consuming so only optional
        if "samples" in plot_type:
            pdb.update( ByconBundler(byc).resultsets_callset_bundles(RSS) )

    ByconPlot(byc, pdb).svgResponse()

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
