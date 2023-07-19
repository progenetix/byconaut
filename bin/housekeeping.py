#!/usr/bin/env python3

import re, json, yaml, sys, datetime
from copy import deepcopy
from isodate import date_isoformat
from os import path, environ, pardir, system
from pymongo import MongoClient
from progress.bar import Bar

from bycon import *

dir_path = path.dirname( path.abspath(__file__) )
pkg_path = path.join( dir_path, pardir )
sys.path.append( path.join( pkg_path, pardir ) )

from byconaut import *

"""
The housekeeping script contains **non-destructive** maintenance scripts which
e.g. may insert derived helper values (e.g. `age_days`).
"""

################################################################################
################################################################################
################################################################################

def main():
    housekeeping()

################################################################################

def housekeeping():

    initialize_bycon_service(byc)
    
    select_dataset_ids(byc)
    if len(byc["dataset_ids"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()

    ds_id = byc["dataset_ids"][0]

    # collecting the actions
    todos = {
        "individual_age_days": input("Recalculate `age_days` in individuals?\n(y|N): "),
        "update_cs_statusmaps": input(f'Update statusmaps in `callsets` for {ds_id}?\n(y|N): '),
        "update_collations": input(f'Create `collations` for {ds_id}?\n(Y|n): '),
        "update_frequencymaps": input(f'Create `frequencymaps` for {ds_id} collations?\n(Y|n): '),
        "datasets_counts": input("Recalculate counts for all datasets?\n(y|N): "),
        "mongodb_index_creation": input("Check & create MongoDB indexes?\n(y|N): ")
    }

    data_db = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))[ ds_id ]

    #>------------------------- callsets -------------------------------------<#

    if "y" in todos.get("update_cs_statusmaps", "y").lower():
        print(f'==> executing "{dir_path}/callsetsStatusmapsRefresher.py -d {ds_id}"')
        system(f'{dir_path}/callsetsStatusmapsRefresher.py -d {ds_id}')

    #>------------------------ / callsets ------------------------------------<#

    #>------------------------ individuals -----------------------------------<#

    ind_coll = data_db[ "individuals" ]

    # age_days
    if "y" in todos.get("individual_age_days", "n").lower():

        query = {"index_disease.onset.age": {"$regex": "^P\d"}}
        no = ind_coll.count_documents(query)
        bar = Bar(f"=> `age_days` for {no} individuals", max = no, suffix='%(percent)d%%'+" of "+str(no) )

        age_c = 0
        for ind in ind_coll.find(query):
            age_days = days_from_iso8601duration(ind["index_disease"]["onset"]["age"])
            if age_days is False:
                continue
            ind_coll.update_one({"_id": ind["_id"]}, {"$set": {"index_disease.onset.age_days": age_days}})
            age_c += 1
            bar.next()

        bar.finish()

        print(f'=> {age_c} individuals received an `index_disease.onset.age_days` value.')

    #>----------------------- / individuals ----------------------------------<#

    #>---------------------- info db update ----------------------------------<#

    if "y" in todos.get("datasets_counts", "n").lower():

        i_db = byc[ "config" ][ "housekeeping_db" ]
        i_coll = byc[ "config" ][ "beacon_info_coll"]

        print(f'\n{__hl()}==> Updating dataset statistics in "{i_db}.{i_coll}"')

        b_info = __dataset_update_counts(byc)

        info_coll = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))[ i_db ][ i_coll ]
        info_coll.delete_many( { "date": b_info["date"] } ) #, upsert=True
        info_coll.insert_one( b_info ) #, upsert=True 

        print(f'\n{__hl()}==> updated entry {b_info["date"]} in {i_db}.{i_coll}')

    #>--------------------- / info db update ---------------------------------<#

    #>---------------------- update collations -------------------------------<#

    if not "n" in todos.get("update_collations", "y").lower():
        print(f'\n{__hl()}==> executing "{dir_path}/collationsCreator.py -d {ds_id}"\n')
        system(f'{dir_path}/collationsCreator.py -d {ds_id}')

    #>--------------------- / update collations ------------------------------<#

    #>--------------------- update frequencymaps -----------------------------<#

    if not "n" in todos.get("update_frequencymaps", "y").lower():
        print(f'\n{__hl()}==> executing "{dir_path}/frequencymapsCreator.py -d {ds_id}"\n')
        system(f'{dir_path}/frequencymapsCreator.py -d {ds_id}')

    #>-------------------- / update frequencymaps ----------------------------<#

    #>-------------------- MongoDB index updates -----------------------------<#

    if "y" in todos.get("mongodb_index_creation", "n").lower():
        print(f'\n{__hl()}==> executing "{dir_path}/frequencymapsCreator.py -d {ds_id}"')
        mongodb_update_indexes(ds_id, byc)

    #>------------------- / MongoDB index updates ----------------------------<#

################################################################################
#################################### subs ######################################
################################################################################

def __dataset_update_counts(byc):

    b_info = { "date": date_isoformat(datetime.datetime.now()), "datasets": { } }
    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))

    # this is independend of the dataset selected for the script & will update
    # for all in any run
    dbs = mongo_client.list_database_names()
    print(byc["dataset_definitions"].keys())
    for i_ds_id in byc["dataset_definitions"].keys():
        if not i_ds_id in dbs:
            print(f'¡¡¡ Dataset "{i_ds_id}" does not exist !!!')
            continue

        ds_db = mongo_client[ i_ds_id ]
        b_i_ds = { "counts": { }, "updated": datetime.datetime.now().isoformat() }
        c_n = ds_db.list_collection_names()
        for c in byc["config"]["queried_collections"]:
            if c not in c_n:
                continue

            no = ds_db[ c ].estimated_document_count()
            b_i_ds["counts"].update( { c: no } )
            if c == "variants":
                v_d = { }
                bar = Bar(i_ds_id+' variants', max = no, suffix='%(percent)d%%'+" of "+str(no) )
                for v in ds_db[ c ].find({ "variant_internal_id": {"$exists": True }}):
                    v_d[ v["variant_internal_id"] ] = 1
                    bar.next()
                bar.finish()
                b_i_ds["counts"].update( { "variants_distinct": len(v_d.keys()) } )

        b_info["datasets"].update({i_ds_id: b_i_ds})
    
    return b_info

################################################################################

def __hl():
    return "".join(["#"] * 80) + "\n"

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
