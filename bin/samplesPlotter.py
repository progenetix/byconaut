#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir

from bycon import *

loc_path = path.dirname( path.abspath(__file__) )
services_lib_path = path.join( loc_path, pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from bycon_plot import *
from interval_utils import generate_genome_bins

"""
./bin/samplesPlotter.py -d examplez --filters "pgx:icdom-85003" -o ./exports/samplestest.svg
"""

################################################################################
################################################################################
################################################################################

def main():

    samples_plotter()

################################################################################

def samples_plotter():

    initialize_bycon_service(byc, "biosamples")
    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    if not byc["args"].datasetIds:
        print("No dataset id(s) were specified (-d, --datasetIds) => quitting ...")
        exit()
    if not byc["args"].outputfile:
        print("No output file specified (-o, --outputfile) => quitting ...")
        exit()
    outfile = byc["args"].outputfile
    if not outfile.endswith(".svg"):
        print("The output file has to end with `.svg` => quitting ...")
        exit()
    if len(byc.get("filters", [])) < 1:
        print("No filter(s) were specified (--filters) => quitting ...")
        exit()

    todos = {
        "samplesplot": input("Create samples plot?\n(y|N): "),
        "histoplot": input(f'Create histogram plot?\n(Y|n): ')
    }

    RSS = ByconResultSets(byc).datasetsResults()
    pdb = ByconBundler(byc).resultsets_frequencies_bundles(RSS)

    if "y" in todos.get("samplesplot", "n").lower():
        byc.update({"output": "samplesplot"})
        pdb.update( ByconBundler(byc).resultsets_callset_bundles(RSS) )        
        s_file = re.sub(".svg", "_samplesplot.svg", outfile)
        print(f'==> Writing to {s_file}')
        ByconPlot(byc, pdb).svg2file(s_file)

    if "y" in todos.get("histoplot", "y").lower():
        byc.update({"output": "histoplot"})
        h_file = re.sub(".svg", "_histoplot.svg", outfile)
        print(f'==> Writing to {h_file}')
        ByconPlot(byc, pdb).svg2file(h_file)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
