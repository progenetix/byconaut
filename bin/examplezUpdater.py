#!/usr/bin/env python3

from pymongo import MongoClient
import pandas as pd
import argparse

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

    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add the '-d' flag argument
    # Note: This doesn't use the standard `bycon` dataset id argument input since
    # you may want to create a new database not in the configuration list ...
    parser.add_argument('-d', '--database', help='Input value with -d flag')
    args = parser.parse_args()

    # Access the input value with '-d' flag
    e_ds_id = args.database
    mongo_client = MongoClient()
    db_names = list(mongo_client.list_database_names())

    # NOTE: I wouldn't use a large framework lime pandas for reading in just
    # some ids... (Michael)
    # Also: I would give feedback/allow selection of id files
    # The id files could also be read in the loop below but I guess it is good
    # to have this potential point of failure up here ...
    bios_example_ids = {
        "progenetix": pd.read_csv('../imports/examplez_progenetix_biosamples.csv', sep=',', low_memory=False, error_bad_lines=False, index_col=0),
        "cellz": pd.read_csv('../imports/examplez_cellz_biosamples.csv', sep=',', low_memory=False, error_bad_lines=False, index_col=0)
    }

    print('Database to create/update:', e_ds_id)

    mongo_client = MongoClient()
    db = mongo_client[e_ds_id]
    # TODO: One needs an _option_ to select for deletion here!
    if e_ds_id in db_names:
        print("Database " + e_ds_id + " exists, delete " + e_ds_id + " database...")
        mongo_client.drop_database(e_ds_id)
        print("Database " + e_ds_id + " deleted, re-creating " + e_ds_id + " database...")
    else:
        print("Database " + e_ds_id + " does not exist, creating " + e_ds_id + " database...")

    e_bios_coll = mongo_client[ e_ds_id ]['biosamples']
    e_cs_coll = mongo_client[ e_ds_id ]['callsets']
    e_ind_coll = mongo_client[ e_ds_id ]['individuals']
    e_var_coll = mongo_client[ e_ds_id ]['variants']

    for s_ds_id, bios_ids in bios_example_ids.items():

        s_bios_coll = mongo_client[ s_ds_id ]['biosamples']
        s_cs_coll = mongo_client[ s_ds_id ]['callsets']
        s_ind_coll = mongo_client[ s_ds_id ]['individuals']
        s_var_coll = mongo_client[ s_ds_id ]['variants']

        print(f"Using data from {s_ds_id}...")

        for bs_id in bios_ids:

            # NOTE: If one checks the biosample at all & keeps existing I would
            # do just a check if it exists and if (and update ... false) then
            # just do a continue here, not touching the variants etc.
            # OTOH, if inserting / replacing a biosample you have to delete all
            # existing associated varriants, callsets ... and then re-importing
            # ... o.k., I've modified...

            e_bs = e_bios_coll.find_one({'id': bs_id}):
            if e_bs:
                continue

            s_bs = s_bios_coll.find_one({'id': bs_id})
            if not s_bs:
                print(f'¡¡¡ sample {bs_id} could not be found in {s_ds_id} !!!')
                continue

            ind_id = s_bs.get("individual_id")
            if not ind_id:
                print(f'¡¡¡ sample {bs_id} did not have an `individual_id` value - excluded !!!')
                continue

            biosid_q = {"biosample_id": bs_id}
            # besides for variants the `delete_many` instead of `...one` seems overkill
            # but better safe ...
            e_cs_coll.delete_many(biosid_q)
            e_var_coll.delete_many(biosid_q)
            e_ind_coll.delete_many({"id": ind_id})

            ind_records = s_ind_coll.find({'id': ind_id})
            if len(list(ind_records)) != 1:
                print(f'¡¡¡ {len(list(ind_records))} individuals for {bs_id} - excluded !!!')
                continue

            cal_records = s_cs_coll.find(biosid_q)
            if len(list(cal_records)) < 1:
                print(f'¡¡¡ no callsets for {bs_id} - excluded !!!')
                continue

            var_records = s_var_coll.find(biosid_q)
            if len(list(var_records)) < 1:
                print(f'¡¡¡ no variants for {bs_id} - excluded !!!')
                continue

            # now we can insert...

            e_bios_coll.insert_one(s_bs)
            e_ind_coll.insert_one(ind_records[0])
            e_cs_coll.insert_many(cal_records)
            e_var_coll.insert_many(var_records)


    print(str(e_bios_coll.count_documents({}))+" biosamples updated/created")
    print(str(e_ind_coll.count_documents({}))+" individuals updated/created")
    print(str(e_cs_coll.count_documents({}))+" callsets updated/created")
    print(str(e_var_coll.count_documents({}))+" variants updated/created")

