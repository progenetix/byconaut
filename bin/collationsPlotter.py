#!/usr/bin/env python3
import argparse, datetime, re, sys
from pymongo import MongoClient
from humps import camelize
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import *
from bycon_plot import *
from interval_utils import generate_genome_bins

"""
./bin/collationsPlotter.py -d "progenetix,cellz" --filters "pgx:icdom-85003,pgx:icdom-81703,pgx:icdom-87003,pgx:icdom-87203,pgx:icdom-94003,pgx:icdom-95003,pgx:icdom-81403" -o ./exports/multicollationtest.svg -p "plot_area_height=50&plot_axis_y_max=80&plot_histogram_frequency_labels=30,60"

"""

################################################################################
################################################################################
################################################################################

def main():
    collations_plotter()

################################################################################

def collations_plotter():

    initialize_bycon_service(byc)
    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    byc["form_data"].update({"plot_type": "histoplot"})
    out_putfile = byc["form_data"].get("outputfile")

    if len(byc["dataset_ids"]) < 1:
        print("Please indicate one or more dataset ids using `-d`")
        exit()
    if len(byc["form_data"].get("filters", [])) < 1:
        print("Please indicate one or more collation ids using `--filters`")
    if not out_putfile:
        print("No output file specified (-o, --outputfile) => quitting ...")
        exit()
    svg_file = out_putfile
    if not ".svg" in svg_file.lower():
        print("The output file should be an `.svg` => quitting ...")
        exit()    

    pdb = ByconBundler(byc).collationsPlotbundles()
    ByconPlot(byc, pdb).svg2file(svg_file)

################################################################################
################################################################################
################################################################################


if __name__ == '__main__':
    main()
