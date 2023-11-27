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

    initialize_bycon_service(byc)
    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    if not byc["args"].inputfile:
        print("No input file specified (-i, --inputfile) => read_pgxseg_2_objects(filepath, byc):quitting ...")
        exit()

    inputfile = byc["args"].inputfile
    if not inputfile.endswith(".pgxseg"):
        print('Only ".pgxseg" input files are accepted.')
        exit()

    if not byc["args"].outputfile:
        print("No output file specified (-o, --outputfile) => quitting ...")
        exit()
    outfile = byc["args"].outputfile
    if not outfile.endswith(".svg"):
        print("The output file has to end with `.svg` => quitting ...")
        exit()

    todos = {
        "samplesplot": input("Create samples plot?\n(y|N): "),
        "histoplot": input(f'Create histogram plot?\n(Y|n): ')
    }

    pb = ByconBundler(byc)
    pdb = pb.pgxseg_to_plotbundle(inputfile)

    if not "n" in todos.get("samplesplot", "n").lower():
        byc.update({"output": "samplesplot"})
        s_file = re.sub(".svg", "_samplesplot.svg", outfile)
        print(f'==> writing samplesplot to \n    {s_file}')
        ByconPlot(byc, pdb).svg2file(s_file)

    if not "n" in todos.get("histoplot", "y").lower():
        byc.update({"output": "histoplot"})
        h_file = re.sub(".svg", "_histoplot.svg", outfile)
        print(f'==> writing histoplot to \n    {h_file}')
        ByconPlot(byc, pdb).svg2file(h_file)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
