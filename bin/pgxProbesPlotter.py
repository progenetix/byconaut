#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_plot import *
from bycon_bundler import ByconBundler
from interval_utils import generate_genome_bins
"""
"""

################################################################################
################################################################################
################################################################################

def main():

    pgx_probes_plotter()

################################################################################

def pgx_probes_plotter():
    initialize_bycon_service(byc)
    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    input_file = byc["form_data"].get("inputfile")
    output_file = byc["form_data"].get("outputfile")

    if not input_file:
        print("No input file specified (-i, --inputfile) => read_probedata_file(filepath, byc):quitting ...")
        exit()

    if not "probe" in input_file:
        print('Only probe files are accepted (should have "...probes..." in name).')
        exit()

    pb = ByconBundler(byc)

    # TODO: method for multiple?
    cs_probes = pb.read_probedata_file(input_file)
    
    plot_data_bundle = {
        "callsets_probes_bundles": [ {"id": "TBD", "probes": cs_probes }]
    }

    byc["form_data"].update({"plot_type":"probesplot"})
    if not output_file:
        output_file = re.sub(".tsv", "_sampleplots.svg", input_file)

    ByconPlot(byc, plot_data_bundle).svg2file(output_file)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
