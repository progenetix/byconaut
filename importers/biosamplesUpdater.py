#!/usr/bin/env python3

import re, json, yaml
from copy import deepcopy
from progress.bar import Bar
import sys, datetime

from bycon import *

loc_path = path.dirname( path.abspath(__file__) )
services_lib_path = path.join( loc_path, pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from datatable_utils import import_datatable_dict_line
from file_utils import read_tsv_to_dictlist, write_log
"""

"""

################################################################################
################################################################################
################################################################################

def main():
    biosamples_updater()

################################################################################

def biosamples_updater():
    initialize_bycon_service()
    if len(BYC["BYC_DATASET_IDS"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()
    ds_id = BYC["BYC_DATASET_IDS"][0]

    input_file = BYC_PARS.get("inputfile")
    if not input_file:
        print("No input file file specified (-i, --inputfile) => quitting ...")
        exit()

    if not BYC["TEST_MODE"]:
        tmi = input("Do you want to run in TEST MODE (i.e. no database insertions/updates)?\n(Y|n): ")
        if not "n" in tmi.lower():
            BYC.update({"TEST_MODE": True})
    if BYC["TEST_MODE"]:
            print("... running in TEST MODE")
 
    log = []

    #--------------------- Read biosample data from file ---------------------#

    bb = ByconBundler()
    biosamples = bb.read_pgx_file(input_file)
    data_no = len(biosamples.data)
    print(f'=> The input file contains {data_no} items ()')

    bar = Bar("Writing ", max = data_no, suffix='%(percent)d%%'+" of "+str(data_no) ) if not BYC["TEST_MODE"] else False

    up_no = 0

    mongo_client = MongoClient(host=DB_MONGOHOST)    
    bios_coll = mongo_client[ ds_id ][ "biosamples" ]
    for c, b in enumerate(biosamples.data):      
        if not BYC["TEST_MODE"]:
            bar.next()
        else:
            print(f'... next sample {c+1}')
            if c == BYC_PARS.get("limit", 10):
                exit()
        if not (bs_id := b.get("biosample_id")):
            print("¡¡¡ The `biosample_id` parameter is required for updating !!!")
            exit()
        if not (bios := bios_coll.find_one({"id": bs_id})):
            log.append(f'{bs_id}\tmissing in {ds_id}.biosamples')
            continue

        # !!!! custom here !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #

        i_s = import_datatable_dict_line({}, biosamples.fieldnames, b, "biosample")

        if not BYC["TEST_MODE"]:
            up_no += 1
            bios_coll.update_one({'_id': bios.get("_id")}, {'$set': i_s})
        else:
            prjsonnice(i_s)

    #----------------------------- Summary ------------------------------------#

    if not BYC["TEST_MODE"]:
        bar.finish()
        print(f'==> updated {up_no} biosamples')
    else:
        print(up_i_no)

    write_log(log, input_file)


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
