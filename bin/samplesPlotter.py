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
    initialize_bycon_service()
    generate_genome_bins()

    # parameter test
    output_file = BYC_PARS.get("outputfile")
    # in this case checking for the command line argument; avoiding defaults
    dataset_ids = BYC_PARS.get("dataset_ids", [])

    if len(dataset_ids) < 1:
        print("No dataset id(s) were specified (-d, --datasetIds) => quitting ...")
        exit()
    if not output_file:
        print("No output file specified (-o, --outputfile) => quitting ...")
        exit()
    if not output_file.endswith(".svg"):
        print("The output file has to end with `.svg` => quitting ...")
        exit()

    q_pars = ("filters", "biosample_ids", "analysis_ids", "individual_ids")
    par_test = []
    for q in q_pars:
        par_test += BYC_PARS.get(q, [])
    if len(par_test) < 1 :
        print("No `--filters` or `--biosampleIds` etc. were specified => quitting ...")
        exit()

    # / parameter test

    # output types selection

    todos = {
        "samplesplot": input("Create samples plot?\n(y|N): "),
        "histoplot": input(f'Create histogram plot?\n(Y|n): ')
    }

    # processing ...

    RSS = ByconResultSets().datasetsResults()
    pdb = ByconBundler().resultsets_frequencies_bundles(RSS)

    if "y" in todos.get("samplesplot", "n").lower():
        BYC_PARS.update({"plot_type": "samplesplot"})
        pdb.update( ByconBundler().resultsets_callset_bundles(RSS) )        
        s_file = re.sub(".svg", "_samplesplot.svg", output_file)
        print(f'==> Writing to {s_file}')
        ByconPlot(pdb).svg2file(s_file)

    if "y" in todos.get("histoplot", "y").lower():
        BYC_PARS.update({"plot_type": "histoplot"})
        h_file = re.sub(".svg", "_histoplot.svg", output_file)
        print(f'==> Writing to {h_file}')
        ByconPlot(pdb).svg2file(h_file)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
