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

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from interval_utils import generate_genome_bins, interval_cnv_arrays, interval_counts_from_callsets
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
    initialize_bycon_service(byc, "frequencymaps_creator")
    generate_genome_bins(byc)

    # avoiding pagination default ...
    limit = BYC_PARS.get("limit")

    if limit > 0 and limit < 10000: 
        proceed = input(f'Do you want to really want to process max. `--limit {limit}` samples per subset?\n(Y|n): ')
        if "n" in proceed.lower():
            exit()

    if len(byc["dataset_ids"]) > 1:
        print("Please give only one dataset using -d")
        exit()

    ds_id = byc["dataset_ids"][0]
    set_collation_types(byc)
    print(f'=> Using data values from {ds_id} for {byc.get("genomic_interval_count", 0)} intervals...')

    data_client = MongoClient(host=DB_MONGOHOST)
    data_db = data_client[ ds_id ]
    coll_coll = data_db[ "collations" ]
    fm_coll = data_db[ "frequencymaps" ]
    ind_coll = data_db["individuals"]
    bios_coll = data_db[ "biosamples" ]
    cs_coll = data_db["analyses"]

    coll_ids = _filter_coll_ids(coll_coll, byc)    
    coll_no = len(coll_ids)
   
    if not BYC["TEST_MODE"]:
        bar = Bar(f'{coll_no} {ds_id} fMaps', max = coll_no, suffix='%(percent)d%%'+f' of {coll_no}' )

    coll_i = 0

    for c_id in coll_ids:
        if not BYC["TEST_MODE"]:
            bar.next()
        coll = coll_coll.find_one({"id": c_id})
        c_o_id = coll.get("_id")
        if not coll:
            print(f"¡¡¡ some error - collation {c_id} not found !!!")
            continue
        coll_i += 1

        byc.update({"filters":[{"id":c_id}, {"id": "EDAM:operation_3961"}]})
        RSS = ByconResultSets(byc).datasetsResults()
        pdb = ByconBundler(byc).resultsets_frequencies_bundles(RSS)
        if_bundles = pdb.get("interval_frequencies_bundles")
        if len(if_bundles) < 1:
            prdbug(f'No interval_frequencies for {c_id}')
            continue

        cnv_cs_count = if_bundles[0].get("sample_count", 0)
        coll_coll.update_one({"_id": c_o_id}, {"$set": {"cnv_analyses": cnv_cs_count}})
        if cnv_cs_count < 1:
            continue

        start_time = time.time()

        update_obj = {
            "id": c_id,
            "label": coll["label"],
            "dataset_id": coll["dataset_id"],
            "scope": coll["scope"],
            "db_key": coll["db_key"],
            "collation_type": coll["collation_type"],
            "child_terms": coll["child_terms"],
            "updated": datetime.datetime.now().isoformat(),
            "counts": {"analyses": cnv_cs_count },
            "frequencymap": {
                "interval_count": byc["genomic_interval_count"],
                "binning": BYC_PARS.get("genome_binning", ""),
                "intervals": if_bundles[0].get("interval_frequencies", []),
                "analysis_count": cnv_cs_count
            }
        }

        proc_time = time.time() - start_time
        # if cs_no > 1000:
        #     print(" => Processed in {:.2f}s: {:.4f}s per callset".format(proc_time, (proc_time/cs_no)))

        if not BYC["TEST_MODE"]:
            fm_coll.delete_many( { "id": c_id } )
            fm_coll.insert_one( update_obj )

        if cnv_cs_count > coll.get("code_matches", cnv_cs_count):
            byc.update({"filters":[{"id":c_id, "includeDescendantTerms": False}, {"id": "EDAM:operation_3961"}]})
            CMRSS = ByconResultSets(byc).datasetsResults()
            cmpdb = ByconBundler(byc).resultsets_frequencies_bundles(CMRSS)

            cmif_bundles = cmpdb.get("interval_frequencies_bundles")
            if len(cmif_bundles) < 1:
                # print(f'No code match interval_frequencies for {c_id}')
                continue

            cnv_cmcs_count = cmif_bundles[0].get("sample_count", 0)
            if cnv_cmcs_count > 0:
                cm_obj = {"frequencymap_codematches":
                    {
                        "interval_count": len(byc["genomic_intervals"]),
                        "binning": BYC_PARS.get("genome_binning", ""),
                        "intervals": cmif_bundles[0].get("interval_frequencies", []),
                        "analysis_count": cnv_cmcs_count
                    }
                }
                prdbug(f'\n{c_id}: {cnv_cmcs_count} exact of {cnv_cs_count} total code matches ({coll["code_matches"]} indicated)')
                if not BYC["TEST_MODE"]:
                    fm_coll.update_one( { "id": c_id }, { '$set': cm_obj }, upsert=False )

    if not BYC["TEST_MODE"]:
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

    coll_ids = coll_coll.distinct("id", id_query)

    return coll_ids


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
