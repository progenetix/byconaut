#!/usr/bin/env python3

import re, json, yaml
from copy import deepcopy
from progress.bar import Bar
import sys, datetime

from bycon import *

loc_path = path.dirname( path.abspath(__file__) )
services_lib_path = path.join( loc_path, pardir, "services", "lib" )
sys.path.append( services_lib_path )
# from datatable_utils import import_datatable_dict_line
from file_utils import read_tsv_to_dictlist
"""

"""

################################################################################
################################################################################
################################################################################

def main():
    this()

################################################################################

def this():
    initialize_bycon_service(byc, "biosamples")

    # if len(byc["dataset_ids"]) != 1:
    #     print("No single existing dataset was provided with -d ...")
    #     exit()

    # HACK
    byc["dataset_ids"] = ["progenetix"]
    match_item = 'tcgaproject_id'
    match_field = 'references.tcgaproject.id'

    ds_id = byc["dataset_ids"][0]
    input_file = BYC_PARS.get("inputfile")

    if not input_file:
        print("No input file file specified (-i, --inputfile) => quitting ...")
        exit()

    if BYC["TEST_MODE"] is False:
        tmi = input("Do you want to run in TEST MODE (i.e. no database insertions/updates)?\n(Y|n): ")
        if not "n" in tmi.lower():
            BYC.update({"TEST_MODE": True})

    if BYC["TEST_MODE"] is True:
            print("... running in TEST MODE")

    bs_updates = []

    mongo_client = MongoClient(host=DB_MONGOHOST)    
    bios_coll = mongo_client[ ds_id ][ "biosamples" ]
    pub_coll = mongo_client[ ds_id ]["publications"]

    # now parse over the input file, find all biosamples for each match parameter
    # create a bios update object with each PMID: [ biosample ids ]

    id_coll = {}
    data, fieldnames = read_tsv_to_dictlist(input_file, int(BYC_PARS.get("limit", 0)))
    data_no = len(data)
    print(f'=> The input file contains {data_no} items')
    for c, d in enumerate(data):
        if not (pmid := d.get("pubmed_id")):
            continue
        if not (match_value := d.get(match_item)):
            continue
        id_coll.update({match_value: {
            "pubmedid": pmid,
            "tcgaproject_label": d.get("tcgaproject_label"),
            "biosample_ids": []
        }})
        query = { match_field: re.compile(match_value, re.IGNORECASE) }
        # print(f'=> {pmid} query: {query}')
        bios_ids = list(bios_coll.distinct("id", query))
        if len(bios_ids) < 1:
            print(f'!!! none\t{match_value}')
        id_coll[match_value]["biosample_ids"] += bios_ids

    for p, v in id_coll.items():
        bios_ids = v.get("biosample_ids")
        pmid = v.get("pubmedid")
        if len(bios_ids) > 0:
            print(f'==>> {p}: {len(bios_ids)}')
        pub = pub_coll.find_one({"pubmedid":pmid})
        if not pub:
            print(f'!!! {pmid} not in publications')
        t_lab = v.get("tcgaproject_label")
        p_lab = pub.get("label", "")
        p_id = f'PMID:{pmid}'
        p_geo = pub["provenance"].get("geo_location")
        for bid in bios_ids:
            if BYC["TEST_MODE"] is False:
                bios_coll.update_one({"id":bid},
                    {"$set":{
                        "provenance.geo_location": p_geo,
                        "references.tcgaproject.label": t_lab,
                        "references.pubmed.label": p_lab,
                        "references.pubmed.id": pmid
                    }
                })
            else:
                print(f'... updating {bid}')




    # error object for PMIDs w/o match
    # for each PMID
    # retrieve publication data
    # for each biosample_id => update provenance. & references.pubmed




################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
