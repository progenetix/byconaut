#!/usr/bin/env python3

import re, json, yaml
from copy import deepcopy
from progress.bar import Bar
import sys, datetime

from bycon import *

loc_path = path.dirname( path.abspath(__file__) )
services_lib_path = path.join( loc_path, pardir, "services", "lib" )
sys.path.append( services_lib_path )
from datatable_utils import import_datatable_dict_line
from file_utils import read_tsv_to_dictlist
"""

"""

################################################################################
################################################################################
################################################################################

def main():
    individuals_updater()

################################################################################

def individuals_updater():
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

    #--------------------- Read individuals data from file --------------------#

    data, fieldnames = read_tsv_to_dictlist(input_file, int(BYC_PARS.get("limit", 0)))
    data_no = len(data)
    print(f'=> The input file contains {data_no} items')

    #------------------- Check individual ids from file data ------------------#

    mongo_client = MongoClient(host=DB_MONGOHOST)    
    ind_coll = mongo_client[ ds_id ][ "individuals" ]
    lno = 0
    ilog = []
    itrue = []
    for l in data:
        lno += 1
        if not (iid := l.get("individual_id")):
            ilog.append(f'{lno}\t\tno individual_id')
            continue
        if not (ind := ind_coll.find_one({"id": iid})):
            ilog.append(f'{lno}\t{iid}\tid not found')
            continue
        itrue.append(iid)

    #-------------------- Chance for a clean interrupt... ---------------------#

    # TODO: interrupt for missing entries etc.

    if BYC["TEST_MODE"]:
        proceed = input(f'Do you want to continue TESTING the database **{ds_id}**?\n(Y|n): ')
        if "n" in proceed.lower():
            exit()
    else:
        proceed = input(f'Do you want to continue to update database **{ds_id}**?\n(y|N): ')
        if not "y" in proceed.lower():
            exit()

    bar = Bar("Processing ", max = len(itrue), suffix='%(percent)d%%'+" of "+str(len(itrue)) ) if not BYC["TEST_MODE"] else False

    up_i_no = 0
    for l in data:
        iid = l.get("individual_id", "___none___")
        if iid not in itrue:
            continue

        ind = ind_coll.find_one({"id": iid})
        update_i =  import_datatable_dict_line(ind, fieldnames, l, "individual")
        update_i.update({"updated": datetime.datetime.now().isoformat()})

        if not BYC["TEST_MODE"]:
            up_i_no += 1
            ind_coll.update_one({'id':iid},{'$set':update_i})
        else:
            prjsonnice(update_i)

    #----------------------------- Summary ------------------------------------#

    if not BYC["TEST_MODE"]:
        bar.finish()
        print(f'==> updated {up_i_no} individuals')
    else:
        print(up_i_no)


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
