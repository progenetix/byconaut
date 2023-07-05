#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir

from bycon import *

"""
"""

################################################################################
################################################################################
################################################################################

def main():

    pgxfreq_plotter()

################################################################################

def pgxfreq_plotter():

    initialize_bycon_service(byc)
    parse_variants(byc)
    generate_genomic_mappings(byc)

    if not byc["args"].inputfile:
        print("No input file specified (-i, --inputfile) => quitting ...")
        exit()

    inputfile = byc["args"].inputfile
    if not inputfile.endswith(".pgxfreq"):
        print('Only ".pgxseg" input files are accepted.')
        exit()

    print('BREAK: Script is just a stub...')
    exit()

    pb = ByconBundler(byc)
    pb.pgxseg_to_bundle(inputfile)    

    interval_frequency_object = bycon_bundle_create_intervalfrequencies_object(bycon_bundle, byc)

    plot_data_bundle = {
        "interval_frequencies_bundles": [ interval_frequency_object ],
        "callsets_variants_bundles": cs_plot_data
    }

    ByconPlot(byc, plot_data_bundle).svg2file(outfile)

    byc.update({"output":"histoplot"})
    outfile = re.sub(".pgxseg", "_histoplot.svg", inputfile)
    ByconPlot(byc, plot_data_bundle).svg2file(outfile)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
