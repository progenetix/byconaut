#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir
import sys, datetime
from isodate import date_isoformat
from pymongo import MongoClient
import argparse
from progress.bar import Bar
import time

from bycon import *

dir_path = path.dirname( path.abspath(__file__) )
lib_path = path.join( dir_path, "lib" )
sys.path.append( lib_path )
from collation_utils import set_collation_types

"""
## `frequencymapsCreator`
"""

################################################################################
################################################################################
################################################################################

def main():
    frequencymaps_creator()

################################################################################

def frequencymaps_creator():

    initialize_bycon_service(byc)
    run_beacon_init_stack(byc)

    if len(byc["dataset_ids"]) > 1:
        print("Please give only one dataset using -d")
        exit()

    ds_id = byc["dataset_ids"][0]

    set_collation_types(byc)

    # re-doing the interval generation for non-standard CNV binning
    # genome_binning_from_args(byc)
    generate_genomic_mappings(byc)
    
    print("=> Using data values from {}".format(ds_id))

    data_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    data_db = data_client[ ds_id ]
    coll_coll = data_db[ "collations" ]
    fm_coll = data_db[ "frequencymaps_coll" ]
    ind_coll = data_db["individuals"]
    bios_coll = data_db[ "biosamples" ]
    cs_coll = data_db["callsets"]

    coll_ids = _filter_coll_ids(coll_coll, byc)    
    coll_no = len(coll_ids)
   
    bar = Bar(f'{coll_no} {ds_id} fMaps for {byc["genomic_interval_count"]} intervals', max = coll_no, suffix='%(percent)d%%'+f' of {coll_no}' )

    coll_i = 0

    for c_id in coll_ids:

        bar.next()

        coll = coll_coll.find_one({"_id":c_id})

        if not coll:
            print("¡¡¡ some error - collation {} not found !!!".format(c_id))
            continue

        pre, code = re.split("[:-]", coll["id"], 1)
        coll_type = coll.get("collation_type", "undefined")

        exclude_normals = True

        for normal in ("EFO:0009654", "oneKgenomes"):
            if normal in coll["id"]:
                print(f'---> keeping normals for {coll["id"]}')
                exclude_normals = False

        db_key = coll["db_key"]

        coll_i += 1

        query = { db_key: { '$in': coll["child_terms"] } }
        bios_no, cs_cursor = _cs_cursor_from_bios_query(bios_coll, ind_coll, cs_coll, coll["id"], coll["scope"], query, exclude_normals)
        cs_no = len(list(cs_cursor))

        if cs_no < 1:
            coll_coll.update_one({"_id": c_id}, {"$set": {"cnv_analyses": 0}})
            continue

        i_t = coll_i % 100
        start_time = time.time()
        # if i_t == 0 or cs_no > 1000:
        #     print("{}: {} bios, {} cs\t{}/{}\t{:.1f}%".format(coll["id"], bios_no, cs_no, coll_i, coll_no, 100*coll_i/coll_no))

        update_obj = {
            "id": coll["id"],
            "label": coll["label"],
            "dataset_id": coll["dataset_id"],
            "scope": coll["scope"],
            "db_key": coll["db_key"],
            "collation_type": coll["collation_type"],
            "child_terms": coll["child_terms"],
            "updated": date_isoformat(datetime.datetime.now()),
            "counts": {"biosamples": bios_no, "callsets": cs_no },
            "frequencymap": {
                "interval_count": byc["genomic_interval_count"],
                "binning": byc["interval_definitions"].get("genome_binning", ""),
                "biosample_count": bios_no
            }
        }

        intervals, cnv_cs_count = interval_counts_from_callsets(cs_cursor, byc)
        update_obj["frequencymap"].update({
            "intervals": intervals,
            "analysis_count": cnv_cs_count
        })

        coll_coll.update_one({"_id": c_id}, {"$set": {"cnv_analyses": cnv_cs_count}})

        proc_time = time.time() - start_time
        # if cs_no > 1000:
        #     print(" => Processed in {:.2f}s: {:.4f}s per callset".format(proc_time, (proc_time/cs_no)))

        if not byc["test_mode"]:
            # print("Updating {}...".format(coll["id"]))
            fm_coll.delete_one( { "id": coll["id"] } )
            fm_coll.insert_one( update_obj )

        if coll["code_matches"] > 0:
            if cs_no != coll["code_matches"]:
                query_cm = { db_key: coll["id"] }
                bios_no_cm, cs_cursor_cm = _cs_cursor_from_bios_query(bios_coll, ind_coll, cs_coll, coll["id"], coll["scope"], query_cm)
                cs_no_cm = len(list(cs_cursor_cm))
                if cs_no_cm > 0:
                    cm_obj = { "frequencymap_codematches": {
                            "interval_count": len(byc["genomic_intervals"]),
                            "binning": byc["interval_definitions"].get("genome_binning", ""),
                            "biosample_count": bios_no_cm
                        }
                    }

                    intervals, cnv_cs_count = interval_counts_from_callsets(cs_cursor_cm, byc)
                    cm_obj["frequencymap_codematches"].update({
                        "intervals": intervals,
                        "analysis_count": cs_no_cm
                    })

                    print(f'\n{coll["id"]}: {cs_no_cm} exact of {cs_no} total code matches')

                    if not byc["test_mode"]:
                        fm_coll.update_one( { "id": coll["id"] }, { '$set': cm_obj }, upsert=False )

    bar.finish()


################################################################################

def _filter_coll_ids(coll_coll, byc):

    c_t_s = list(byc["filter_definitions"].keys())

    id_query = {}
    id_ql = [{ "collation_type":{"$in": c_t_s }}]

    if len(byc["filters"]) > 0:
        f_l = []
        for c_t in byc["filters"]:
            f_l.append( { "id": { "$regex": "^"+c_t["id"] } })
        if len(f_l) > 1:
            id_ql.append( { "$or": f_l } )
        else:
            id_ql.append(f_l[0])

    if len(id_ql) == 1:
        id_query = id_ql[0]
    elif len(id_ql) > 1:
        id_query = { "$and":id_ql }

    coll_ids = coll_coll.distinct("_id", id_query)

    return coll_ids

################################################################################

def _cs_cursor_from_bios_query(bios_coll, ind_coll, cs_coll, coll_id, scope, query, exclude_normals=True):

    if scope == "individuals":
        ind_ids = ind_coll.distinct( "id" , query )
        bios_ids = bios_coll.distinct( "id" , {"individual_id":{"$in": ind_ids } } )
    elif scope == "callsets":
        bios_ids = cs_coll.distinct( "biosample_id" , query )
    else:
        bios_ids = bios_coll.distinct( "id" , query )

    pre_b = len(bios_ids)

    # for most entities samples labeled as "normal" will be excluded for frequency calculations
    if exclude_normals:
        bios_ids = bios_coll.distinct( "id" , { "id": { "$in": bios_ids } , "biosample_status.id": {"$ne": "EFO:0009654" }} )
    bios_no = len(bios_ids)
    
    if pre_b > bios_no:
        print(f'\nWARNING: {pre_b} samples for {coll_id}, while {bios_no} after excluding normals by EFO:0009654')
       
    cs_query = { "biosample_id": { "$in": bios_ids } , "variant_class": { "$ne": "SNV" } }
    cs_cursor = cs_coll.find(cs_query)

    return bios_no, cs_cursor

################################################################################
################################################################################
################################################################################


if __name__ == '__main__':
    main()
