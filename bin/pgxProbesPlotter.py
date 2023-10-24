#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_plot import *

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
    parse_variants(byc)
    generate_genomic_mappings(byc)

    if not byc["args"].inputfile:
        print("No input file specified (-i, --inputfile) => read_probedata_file(filepath, byc):quitting ...")
        exit()

    inputfile = byc["args"].inputfile
    if not "probe" in inputfile:
        print('Only probe files are accepted.')
        exit()

    pb = ByconBundler(byc)

    # TODO: method for multiple?
    cs_probes = pb.read_probedata_file(inputfile)
    
    plot_data_bundle = {
        "callsets_probes_bundles": [ {"id": "TBD", "probes": cs_probes }]
    }

    byc.update({"output":"arrayplot"})
    if byc["args"].outputfile:
        outfile = byc["args"].outputfile
    else:
        outfile = re.sub(".tsv", "_sampleplots.svg", inputfile)

    ByconPlot(byc, plot_data_bundle).svg2file(outfile)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
