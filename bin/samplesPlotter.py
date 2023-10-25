#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from bycon_plot import *

"""
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

    if not byc["args"].outputfile:
        print("No output file specified (-o, --outputfile) => quitting ...")
        exit()
    outfile = byc["args"].outputfile
    if not outfile.endswith(".svg"):
        print("The output file has to end with `.svg` => quitting ...")
        exit()
       
    ds_id = byc["dataset_ids"][0]
    print(f'=> Using data values from {ds_id}')

    todos = {
        "samplesplot": input("Create samples plot?\n(y|N): "),
        "histoplot": input(f'Create histogram plot?\n(Y|n): ')
    }

    # re-doing the interval generation for non-standard CNV binning
    # genome_binning_from_args(byc)
    generate_genomic_mappings(byc)
    RSS = ByconResultSets(byc).datasetsResults()
    pdb = ByconBundler(byc).resultsets_frequencies_bundles(RSS)

    if "y" in todos.get("samplesplot", "n").lower():
        byc.update({"output": "samplesplot"})
        pdb.update( ByconBundler(byc).resultsets_callset_bundles(RSS) )        
        s_file = re.sub(".svg", "_samplesplot.svg", outfile)
        ByconPlot(byc, pdb).svg2file(s_file)

    if "y" in todos.get("histoplot", "y").lower():
        byc.update({"output": "histoplot"})
        h_file = re.sub(".svg", "_histoplot.svg", outfile)
        ByconPlot(byc, pdb).svg2file(h_file)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
