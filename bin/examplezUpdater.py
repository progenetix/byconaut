#!/usr/bin/env python3

from pymongo import MongoClient
from os import path, pardir, system
from pathlib import Path
from progress.bar import Bar

import pandas as pd
import argparse

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
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--option', help='Specify delete or keep the existing target database name with the `-o` flag')
    args = parser.parse_args()


    # Access the input value with '-d' flag
    e_ds_id = args.database
    option = args.option
    mongo_client = MongoClient()
    db_names = list(mongo_client.list_database_names())

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

    print('Database to create/update:', e_ds_id)

    mongo_client = MongoClient()
    db = mongo_client[e_ds_id]
    # TODO: One needs an _option_ to select for deletion here!
    if e_ds_id in db_names and option == "delete":
        print("Database " + e_ds_id + " exists, delete " + e_ds_id + " database...")
        mongo_client.drop_database(e_ds_id)
        print("Database " + e_ds_id + " deleted, re-creating " + e_ds_id + " database...")
    if e_ds_id in db_names and option == "keep":
        print("Database " + e_ds_id + " exists, updating " + e_ds_id + " database...")
    if e_ds_id not in db_names:
        print("Database " + e_ds_id + " does not exist, creating " + e_ds_id + " database...")

    e_bios_coll = mongo_client[ e_ds_id ]['biosamples']
    e_cs_coll = mongo_client[ e_ds_id ]['callsets']
    e_ind_coll = mongo_client[ e_ds_id ]['individuals']
    e_var_coll = mongo_client[ e_ds_id ]['variants']

    for s_ds_id, bios_ids in bios_example_ids.items():

        bar = Bar(f"Writing {len(bios_ids)} from {s_ds_id}...", max = len(bios_ids), suffix='%(percent)d%%'+" of "+str(len(bios_ids)) )

        s_bios_coll = mongo_client[ s_ds_id ]['biosamples']
        s_cs_coll = mongo_client[ s_ds_id ]['callsets']
        s_ind_coll = mongo_client[ s_ds_id ]['individuals']
        s_var_coll = mongo_client[ s_ds_id ]['variants']

        for bs_id in bios_ids:

            bar.next()

            # NOTE: If one checks the biosample at all & keeps existing I would
            # do just a check if it exists and if (and update ... false) then
            # just do a continue here, not touching the variants etc.
            # OTOH, if inserting / replacing a biosample you have to delete all
            # existing associated varriants, callsets ... and then re-importing
            # ... o.k., I've modified...


            e_bs = e_bios_coll.find_one({'id': bs_id})
            if e_bs and option == "keep":
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
            ind_record = s_ind_coll.find_one({'id': ind_id})


            cal_records = list(s_cs_coll.find(biosid_q))
            if len(cal_records) < 1:
                print(f'¡¡¡ no callsets for {bs_id} - excluded !!!')
                continue

            var_records = list(s_var_coll.find(biosid_q))
            if len(var_records) < 1:
                print(f'¡¡¡ no variants for {bs_id} - excluded !!!')
                continue

            # now we can insert...

            e_bios_coll.insert_one(s_bs)
            e_ind_coll.insert_one(ind_record)
            for cs in cal_records:
                e_cs_coll.insert_one(cs)
            for var in var_records:
                e_var_coll.insert_one(var)

        bar.finish()

    print(f'{e_bios_coll.count_documents({})} biosamples updated/created')
    print(f'{e_ind_coll.count_documents({})} individuals updated/created')
    print(f'{e_cs_coll.count_documents({})} callsets updated/created')
    print(f'{e_var_coll.count_documents({})} variants updated/created')

    ############################################################################

    rsrc_dir = path.join( pkg_path, "rsrc" )
    mongo_dir = Path( path.join( rsrc_dir, "mongodump" ) )
    e_ds_dir = Path( path.join( mongo_dir, e_ds_id ) )
    e_ds_archive = f'{e_ds_id}.tar.gz'
    system(f'rm -rf {e_ds_dir}')
    system(f'mongodump --db {e_ds_id} --out {mongo_dir}')
    system(f'cd {mongo_dir} && tar -czf {e_ds_archive} {e_ds_id} && rm -rf {e_ds_id}')

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
