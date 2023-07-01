#!/usr/bin/env python3

import argparse, datetime, re, sys
from pymongo import MongoClient
from humps import camelize
from os import environ
from bycon import *

"""
./bin/collationsPlotter.py --filters "pgx:icdom-85003,pgx:icdom-81703,pgx:icdom-87003,pgx:icdom-87203,pgx:icdom-94003,pgx:icdom-95003,pgx:icdom-81403" -o ./exports/test.svg -p "plot_area_height=50&plot_axis_y_max=80&plot_histogram_frequency_labels=30,60"

"""

################################################################################
################################################################################
################################################################################

def main():
    collations_plotter()

################################################################################

def collations_plotter():

    initialize_bycon_service(byc)
    select_dataset_ids(byc)
    parse_variant_parameters(byc)
    generate_genomic_mappings(byc)

    p_d_p = byc["plot_defaults"]["parameters"]
    p_d_l = byc["plot_defaults"]["legacy_parameters"]

    # plot parameters can be modified by providing them in a `-p` string...

    if byc["args"].parse:
        for c_arg in re.split(r',|&',byc["args"].parse):
            p_v = re.split('=', c_arg)
            if len(p_v) != 2:
                continue
            if p_v[0] in p_d_p.keys():
                byc["form_data"].update({p_v[0]:p_v[1]})

    byc.update({"output":"histoplot"})

    if len(byc["dataset_ids"]) < 1:
        print("Please indicate one or more dataset ids using `-d`")
        exit()
    if not byc["args"].outputfile:
        print("No output file specified (-o, --outputfile) => quitting ...")
        exit()
    svg_file = byc["args"].outputfile
    if not ".svg" in svg_file.lower():
        print("The output file should be an `.svg` => quitting ...")
        exit()

    coll_ids = byc["form_data"].get("filters", [])
    
    if len(coll_ids) < 1:
        print("Please indicate one or more collation ids using `--filters`")

    # data retrieval & response population
    f_coll_name = byc["config"]["frequencymaps_coll"]
    c_coll_name = byc["config"]["collations_coll"]

    fmap_name = "frequencymap"
    if "codematches" in byc["method"]:
        fmap_name = "frequencymap_codematches"

    results = [ ]
    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    for ds_id in byc[ "dataset_ids" ]:

        for f_val in coll_ids:
 
            collation_f = mongo_client[ ds_id ][ f_coll_name ].find_one( { "id": f_val } )
            collation_c = mongo_client[ ds_id ][ c_coll_name ].find_one( { "id": f_val } )

            if collation_f is None:
                continue

            if "with_samples" in byc["form_data"]: 
                if int(byc["form_data"]["with_samples"]) > 0:
                    if int(collation_c[ "code_matches" ]) < 1:
                        continue

            if not fmap_name in collation_f:
                continue

            if not collation_f:
                print("No collation {} was found in {}.{}".format(f_val, ds_id, f_coll_name))
            if not collation_c:
                print("No collation {} was found in {}.{}".format(f_val, ds_id, c_coll_name))

            s_c = collation_c["count"]
            if "analysis_count" in collation_f[ fmap_name ]:
               s_c = collation_f[ fmap_name ]["analysis_count"]

            i_d = collation_c["id"]
            r_o = {
                "dataset_id": ds_id,
                "group_id": i_d,
                "label": re.sub(r';', ',', collation_c["label"]),
                "sample_count": s_c,
                "interval_frequencies": collation_f[ fmap_name ]["intervals"] }
                
            results.append(r_o)

    mongo_client.close( )

    svg = bycon_plot_generator(byc, results)

    svg_fh = open(svg_file, "w")
    svg_fh.write( svg )
    svg_fh.close()

################################################################################
################################################################################
################################################################################


if __name__ == '__main__':
    main()
