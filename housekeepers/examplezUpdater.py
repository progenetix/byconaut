#!/usr/bin/env python3

from pymongo import MongoClient
from os import path, pardir, system, environ
from pathlib import Path
from progress.bar import Bar

import pandas as pd
import argparse

from bycon import *

dir_path = path.dirname( path.abspath(__file__) )
pkg_path = path.join( dir_path, pardir )

################################################################################
################################################################################
################################################################################

def main():
    """
    ./bin/examplezUpdater -d examplez
    """
    examplez_updater()

################################################################################

def examplez_updater():
    # Note: This doesn't use the standard `bycon` dataset id argument input since
    # you may want to create a new database not in the configuration list ...
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='Specify a target database name with the `-d` flag')
    args = parser.parse_args()

    # Access the input value with '-d' flag
    e_ds_id = args.database
    mongo_client = MongoClient(host=DB_MONGOHOST)
    db_names = list(mongo_client.list_database_names())
    todos = {
        "drop_existing_database": False,
        "replace_matching_records": True,
        "update_collations": input(f'Create `collations` for {e_ds_id}?\n(Y|n): '),
        "update_frequencymaps": input(f'Create `frequencymaps` for {e_ds_id} collations?\n(Y|n): ')
    }

    # collecting the actions
    print('Database to create/update:', e_ds_id)
    if e_ds_id in db_names:
        ddb_resp = input(f'Drop existing {e_ds_id} database first?\n(Y|n): ')
        if not "n" in ddb_resp.lower():
            todos.update({ "drop_existing_database": True })
        else:
            rep_rec = input(f'Replace records with matching `biosample_id`?\n(Y|n): ')
            if "n" in rep_rec.lower():
                todos.update({ "replace_matching_records": False })


    # NOTE: I wouldn't use a large framework lime pandas for reading in just
    # some ids... (Michael)
    # Also: I would give feedback/allow selection of id files
    # The id files could also be read in the loop below but I guess it is good
    # to have this potential point of failure up here ...
    pgx_id_f = Path( path.join( pkg_path, "imports", "examplez_progenetix_biosamples.csv" ) )
    cellz_id_f = Path( path.join( pkg_path, "imports", "examplez_cellz_biosamples.csv" ) )
    bios_example_ids = {
        "progenetix": list(pd.read_csv(pgx_id_f, sep=',', index_col=0)),
        "cellz": list(pd.read_csv(cellz_id_f, sep=',', index_col=0))
    }

    mongo_client = MongoClient(host=DB_MONGOHOST)
    db = mongo_client[e_ds_id]

    if todos["drop_existing_database"] is True:
        print(f'Database {e_ds_id} existed but is deleted & re-created...')
        mongo_client.drop_database(e_ds_id)
    else:
        print(f'Adding records in {e_ds_id}...')

    e_bios_coll = mongo_client[ e_ds_id ]['biosamples']
    e_cs_coll = mongo_client[ e_ds_id ]['analyses']
    e_ind_coll = mongo_client[ e_ds_id ]['individuals']
    e_var_coll = mongo_client[ e_ds_id ]['variants']

    for s_ds_id, bios_ids in bios_example_ids.items():

        bar = Bar(f"Writing {len(bios_ids)} from {s_ds_id}...", max = len(bios_ids), suffix='%(percent)d%%'+" of "+str(len(bios_ids)) )

        s_bios_coll = mongo_client[ s_ds_id ]['biosamples']
        s_cs_coll = mongo_client[ s_ds_id ]['analyses']
        s_ind_coll = mongo_client[ s_ds_id ]['individuals']
        s_var_coll = mongo_client[ s_ds_id ]['variants']

        for bs_id in bios_ids:

            bar.next()

            e_bs = e_bios_coll.find_one({'id': bs_id})
            biosid_q = {"biosample_id": bs_id}
            if e_bs:
                if todos["replace_matching_records"] is True:
                    e_bios_coll.delete_many({'id': bs_id})
                    e_cs_coll.delete_many(biosid_q)
                    e_var_coll.delete_many(biosid_q)
                else:
                    continue

            s_bs = s_bios_coll.find_one({'id': bs_id})

            if not s_bs:
                print(f'¡¡¡ sample {bs_id} could not be found in {s_ds_id} !!!')
                continue

            ind_id = s_bs.get("individual_id")
            if not ind_id:
                print(f'¡¡¡ sample {bs_id} did not have an `individual_id` value - excluded !!!')
                continue
            
            # clean out dependend records ...
            e_cs_coll.delete_many(biosid_q)
            e_var_coll.delete_many(biosid_q)

            ind_records = s_ind_coll.find({'id': ind_id})
            if len(list(ind_records)) != 1:
                print(f'¡¡¡ {len(list(ind_records))} individuals for {bs_id} - excluded !!!')
                continue
            ind_record = s_ind_coll.find_one({'id': ind_id})

            cal_records = list(s_cs_coll.find(biosid_q))
            if len(cal_records) < 1:
                print(f'¡¡¡ no analyses for {bs_id} - excluded !!!')
                continue

            var_records = list(s_var_coll.find(biosid_q))
            if len(var_records) < 1:
                print(f'¡¡¡ no variants for {bs_id} - excluded !!!')
                continue

            # now we can insert...

            e_bios_coll.update_one({'id': bs_id}, {"$set": s_bs}, upsert=True)
            e_ind_coll.update_one({'id': ind_id}, {"$set": ind_record}, upsert=True)
            for cs in cal_records:
                e_cs_coll.insert_one(cs)
            for var in var_records:
                e_var_coll.insert_one(var)

        bar.finish()

    print(f'{e_bios_coll.count_documents({})} biosamples updated/created')
    print(f'{e_ind_coll.count_documents({})} individuals updated/created')
    print(f'{e_cs_coll.count_documents({})} analyses updated/created')
    print(f'{e_var_coll.count_documents({})} variants updated/created')

    ############################################################################

    #>---------------------- update collations -------------------------------<#

    if not "n" in todos.get("update_collations", "y").lower():
        print(f'==> executing "{dir_path}/collationsCreator.py -d {e_ds_id}"')
        system(f'{dir_path}/collationsCreator.py -d {e_ds_id}')

    #>--------------------- update frequencymaps -----------------------------<#

    if not "n" in todos.get("update_frequencymaps", "y").lower():
        print(f'==> executing "{dir_path}/frequencymapsCreator.py -d {e_ds_id}"')
        system(f'{dir_path}/frequencymapsCreator.py -d {e_ds_id}')

    ############################################################################

    for db in (e_ds_id, "_byconServicesDB"):

        rsrc_dir = path.join( pkg_path, "rsrc" )
        mongo_dir = Path( path.join( rsrc_dir, "mongodump" ) )
        e_ds_dir = Path( path.join( mongo_dir, db ) )
        e_ds_archive = f'{db}.tar.gz'
        system(f'rm -rf {db}')
        system(f'mongodump --db {db} --out {mongo_dir}')
        system(f'cd {mongo_dir} && tar -czf {e_ds_archive} {db} && rm -rf {db}')

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
