#!/usr/bin/env python3

import re, json, yaml
from os import path
from progress.bar import Bar
import sys, datetime

from pathlib import Path

from bycon import *

loc_path = path.dirname( path.abspath(__file__) )
lib_path = path.join(loc_path , "lib")
sys.path.append( lib_path )
from importer_helpers import *

services_lib_path = path.join( loc_path, pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from file_utils import write_log
from datatable_utils import import_datatable_dict_line
"""

"""

################################################################################
################################################################################
################################################################################

def main():
    initialize_bycon_service()
    ds_id, input_file = initialize_importer()
    
    log = []

    delMatchedVars = "n"
    deletion_state = "DELETE_ANALYSIS_VARIANTS"

    #--------------------- Read variants data from file -----------------------#

    bb = ByconBundler()
    variants = bb.read_pgx_file(input_file)
    var_no = len(variants.data)
    up_v_no = 0

    #---- Identify biosample_id and variant_state.id values from file data ----#

    mongo_client = MongoClient(host=DB_MONGOHOST)    
    var_coll = mongo_client[ ds_id ][ "variants" ]
    bios_ids = set()
    cs_ids = set()
    for c, v in enumerate(variants.data):      
        if BYC["TEST_MODE"]:
            if c == BYC_PARS.get("limit", 10) and c > 0:
                continue
            print(f'... next {c+1}')
        if not (bs_id := v.get("biosample_id")):
            print(f"¡¡¡ The `biosample_id` parameter is required for variant assignment in line {c}!!!")
            exit()
        if not (cs_id := v.get("analysis_id")):
            print(f"¡¡¡ The `analysis_id` parameter is required for variant assignment  line {c}!!!")
            exit()
        if not (vs_id := v.get("variant_state_id")):
            print(f"¡¡¡ The `variant_state_id` parameter is required for variant assignment  line {c}!!!")
            exit()
        if len(bs_id) < 8:
            print("¡¡¡ The `biosample_id` parameter is too short... (< 8) !!!")
            exit()
        bios_ids.add(bs_id)
        cs_ids.add(cs_id)
    print(f'=> The variants file contains {var_no} variants from {len(bios_ids)} samples ({len(cs_ids)} analyses).')

    #---------- Checking if biosamples and analyses exist... ------------------#

    bios_coll = mongo_client[ds_id]["biosamples"]
    for bs_id in bios_ids:
        if not (b := bios_coll.find_one({"id": bs_id})):
            log.append(f'{bs_id}\tmissing in {ds_id}.biosamples')
    cs_coll = mongo_client[ ds_id ][ "analyses" ]
    for cs_id in cs_ids:
        if not (a := cs_coll.find_one({"id": cs_id})):
            log.append(f'{cs_id}\tmissing in {ds_id}.analyses')

    if len(log) > 0:
        write_log(log, input_file)
        exit()

    #-------------------- Chance for a clean interrupt... ---------------------#

    if not BYC["TEST_MODE"]:
        delMatchedVars = input(f'Delete the variants of the same analysis ids as in the input file?\n(y|N): ')

    if BYC["TEST_MODE"]:
        proceed = input(f'Do you want to continue TESTING the database **{ds_id}**?\n(Y|n): ')
        if "n" in proceed.lower():
            exit()
    else:
        proceed = input(f'Do you want to continue to update database **{ds_id}**?\n(y|N): ')
        if not "y" in proceed.lower():
            exit()

    #------------ Deletion of existing variants based on selection ------------#

    if "y" in delMatchedVars.lower():
        for ana_id in cs_ids:
            v_dels = var_coll.delete_many({"analysis_id": ana_id})
            print(f'==>> deleted {v_dels.deleted_count} variants from {ana_id}')

    #----------- / Deletion of existing variants based on selection -----------#

    bios_v_counts = {}
    bar = Bar("Writing ", max = var_no, suffix='%(percent)d%%'+" of "+str(var_no) ) if not BYC["TEST_MODE"] else False

    for c, v in enumerate(variants.data, 1):
        if BYC["TEST_MODE"]:
            if c == BYC_PARS.get("limit", 10) and c > 0:
                continue
        if str(v.get("variant_state_id")) == deletion_state:
            continue
        bar.next() if not BYC["TEST_MODE"] else print(f'... next variant {c}')           
        bs_id = v.get("biosample_id", False)
        if not bs_id in bios_v_counts.keys():
            bios_v_counts.update({bs_id: 0})
        bios_v_counts[bs_id] += 1

        # if len(sid) < 5 and len(chro) < 1:
        #     continue

        insert_v = import_datatable_dict_line({}, variants.fieldnames, v, "genomicVariant")
        insert_v = ByconVariant().pgxVariant(insert_v)
        insert_v.update({"updated": datetime.datetime.now().isoformat()})

        if not BYC["TEST_MODE"]:
            up_v_no += 1
            vid = var_coll.insert_one(insert_v).inserted_id
            vstr = f'pgxvar-{vid}'
            var_coll.update_one({'_id':vid},{'$set':{ 'id':vstr }})
        else:
            prjsonnice(insert_v)

    #----------------------------- Summary ------------------------------------#

    if not BYC["TEST_MODE"]:
        bar.finish()
        print(f'==> inserted {up_v_no} variants for {len(bios_v_counts.keys())} samples')
    else:
        prjsonnice(bios_v_counts)




################################################################################

# TODO: Only here as local copy of a ByconautImporter function; might use this...
def initialize_importer():
    BYC.update({"BYC_DATASET_IDS": BYC_PARS.get("dataset_ids")})
    if len(BYC["BYC_DATASET_IDS"]) != 1:
        print("No single dataset was provided with -d ...")
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
    ask_limit_reset()

    return ds_id, input_file

################################################################################
################################################################################

if __name__ == '__main__':
    main()
