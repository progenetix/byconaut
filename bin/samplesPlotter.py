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
    RSS = ByconResultSets(byc)

    if "y" in todos.get("samplesplot", "n").lower():

        byc.update({"output": "samplesplot"})
        s_file = re.sub(".svg", "_samplesplot.svg", outfile)
        s_svg = RSS.samplesPlot()
        svg_fh = open(s_file, "w")
        svg_fh.write(s_svg)
        svg_fh.close()

    if "y" in todos.get("histoplot", "y").lower():

        byc.update({"output": "histoplot"})
        h_file = re.sub(".svg", "_histoplot.svg", outfile)
        h_svg = RSS.histoPlot()
        svg_fh = open(h_file, "w")
        svg_fh.write(h_svg)
        svg_fh.close()

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
