#!/usr/bin/env python3

# import re, json, yaml
# from os import path, environ, pardir
from os import environ
import sys, datetime
from isodate import date_isoformat
from pymongo import MongoClient
from progress.bar import Bar
#import time

from bycon import *

"""

## `callsetsStatusmapsRefresher`

"""

################################################################################
"""
* `bin/callsetsStatusmapsRefresher.py -d 1000genomesDRAGEN -s variants`
* `bin/callsetsStatusmapsRefresher.py -d progenetix -s biosamples -f "icdom-81703"`
* `bin/callsetsStatusmapsRefresher.py`
  - default; new statusmaps for all `progenetix` callsets
"""
################################################################################

def main():

    callsets_refresher()

################################################################################

def callsets_refresher():

    initialize_bycon_service(byc)
    run_beacon_init_stack(byc)

    if len(byc["dataset_ids"]) > 1:
        print("Please give only one dataset using -d")
        exit()

    ds_id = byc["dataset_ids"][0]

    # re-doing the interval generation for non-standard CNV binning
    # genome_binning_from_args(byc)
    generate_genomic_mappings(byc)
        
    print("=> Using data values from {}".format(ds_id))

    data_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    data_db = data_client[ ds_id ]
    cs_coll = data_db[ "callsets" ]
    v_coll = data_db[ "variants" ]

    execute_bycon_queries( ds_id, byc )

    ds_results = byc["dataset_results"][ds_id]

    no_cnv_type = 0

    if not "callsets._id" in ds_results.keys():
        cs_ids = []
        for cs in cs_coll.find( {} ):
            cs_ids.append(cs["_id"])
        cs_no = len(cs_ids)
        print(f'¡¡¡ Using all {cs_no} callsets from {ds_id} !!!')
    else:
        cs_ids = ds_results["callsets._id"]["target_values"]
        cs_no = len(cs_ids)

    print(f'Re-generating statusmaps with {byc["genomic_interval_count"]} intervals for {cs_no} callsets...')
    bar = Bar("{} callsets".format(ds_id), max = cs_no, suffix='%(percent)d%%'+" of "+str(cs_no) )
    counter = 0
    updated = 0

    proceed = input(f'Do you want to continue to update database **{ds_id}**?\n(Y|n): ')
    if "n" in proceed.lower():
        exit()

    for _id in cs_ids:

        cs = cs_coll.find_one( { "_id": _id } )
        csid = cs["id"]
        counter += 1

        bar.next()

        if not "CNV" in cs.get("variant_class", "CNV"):
            no_cnv_type += 1
            continue

        # only the defined parameters will be overwritten
        cs_update_obj = { "info": cs.get("info", {}) }
        cs_update_obj["info"].pop("statusmaps", None)
        cs_update_obj["info"].pop("cnvstatistics", None)

        cs_vars = v_coll.find({ "callset_id": csid })
        maps, cs_cnv_stats, cs_chro_stats = interval_cnv_arrays(cs_vars, byc)

        cs_update_obj.update({"cnv_statusmaps": maps})
        cs_update_obj.update({"cnv_stats": cs_cnv_stats})
        cs_update_obj.update({"cnv_chro_stats": cs_chro_stats})
        cs_update_obj.update({ "updated": datetime.datetime.now().isoformat() })

        if not byc["test_mode"]:
            cs_coll.update_one( { "_id": _id }, { '$set': cs_update_obj }  )
            updated += 1
        else:
            prjsonnice(cs_chro_stats)

        ####################################################################
        ####################################################################
        ####################################################################

    bar.finish()

    print(f"{counter} callsets were processed")
    print(f"{no_cnv_type} callsets were not from CNV calling")
    print(f'{updated} callsets were updated for\n    `cnv_statusmaps`\n    `cnv_stats`\n    `cnv_chro_stats`\nusing {byc["genomic_interval_count"]} bins ({byc["interval_definitions"].get("genome_binning", "")})')

################################################################################
################################################################################
################################################################################


if __name__ == '__main__':
    main()
