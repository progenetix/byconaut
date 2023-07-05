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

    pgxseg_plotter()

################################################################################

def pgxseg_plotter():

    initialize_bycon_service(byc)
    parse_variants(byc)
    generate_genomic_mappings(byc)

    if not byc["args"].inputfile:
        print("No input file specified (-i, --inputfile) => read_pgxseg_2_objects(filepath, byc):quitting ...")
        exit()

    inputfile = byc["args"].inputfile
    if not inputfile.endswith(".pgxseg"):
        print('Only ".pgxseg" input files are accepted.')
        exit()

    pb = ByconBundler(byc)
    pb.pgxseg_to_bundle(inputfile)
    plot_data_bundle = {
        "interval_frequencies_bundles": pb.callsets_frequencies_bundles(),
        "callsets_variants_bundles": pb.callsets_variants_bundles()
    }

    byc.update({"output":"samplesplot"})
    if byc["args"].outputfile:
        outfile = byc["args"].outputfile
    else:
        outfile = re.sub(".pgxseg", "_sampleplots.svg", inputfile)

    ByconPlot(byc, plot_data_bundle).svg2file(outfile)

    byc.update({"output":"histoplot"})
    outfile = re.sub(".pgxseg", "_histoplot.svg", inputfile)
    ByconPlot(byc, plot_data_bundle).svg2file(outfile)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
