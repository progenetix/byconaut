#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from bycon_plot import *
from interval_utils import generate_genome_bins

"""
./bin/pgxsegPlotter.py -i ./imports/test.pgxseg -o ./exports/test.svg
"""

################################################################################
################################################################################
################################################################################

def main():

    pgxseg_plotter()

################################################################################

def pgxseg_plotter():
    initialize_bycon_service(byc, "pgxseg_plotter")
#    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    input_file = BYC_PARS.get("inputfile")
    output_file = BYC_PARS.get("outputfile")

    if not input_file:
        print("No input file specified (-i, --inputfile) => read_pgxseg_2_objects(filepath, byc):quitting ...")
        exit()

    if not input_file.endswith(".pgxseg"):
        print('Only ".pgxseg" input files are accepted.')
        exit()

    if not output_file:
        print("No output file specified (-o, --outputfile) => quitting ...")
        exit()
    if not output_file.endswith(".svg"):
        print("The output file has to end with `.svg` => quitting ...")
        exit()

    todos = {
        "samplesplot": input("Create samples plot?\n(y|N): "),
        "histoplot": input(f'Create histogram plot?\n(Y|n): ')
    }

    pb = ByconBundler(byc)
    pdb = pb.pgxseg_to_plotbundle(input_file)

    if not "n" in todos.get("samplesplot", "n").lower():
        print(BYC_PARS.get("plot_pars"))
        BYC_PARS.update({"plot_type": "samplesplot"})
        s_file = re.sub(".svg", "_samplesplot.svg", output_file)
        print(f'==> writing samplesplot to \n    {s_file}')
        ByconPlot(byc, pdb).svg2file(s_file)

    if not "n" in todos.get("histoplot", "y").lower():
        print(BYC_PARS.get("plot_pars"))
        BYC_PARS.update({"plot_type": "histoplot"})
        h_file = re.sub(".svg", "_histoplot.svg", output_file)
        print(f'==> writing histoplot to \n    {h_file}')
        ByconPlot(byc, pdb).svg2file(h_file)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
