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
"""

"""

################################################################################
################################################################################
################################################################################

def main():
    variants_inserter()

################################################################################

def variants_inserter():
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

    delSOvars, delCNVvars = ["n", "n"]
    if not BYC["TEST_MODE"]:
        delSOvars = input('Delete only the sequence variants ("SO:...") from matched biosamples before insertion?\n(y|N): ')
        delCNVvars = input('Delete only the CNV variants ("EFO:...") from matched biosamples before insertion?\n(y|N): ')

    #--------------------- Read variants data from file -----------------------#

    vb = ByconBundler()
    variants = vb.read_pgx_file(input_file)
    var_no = len(variants.data)
    up_v_no = 0

    #-------------- Identify biosample_id values from file data ---------------#

    mongo_client = MongoClient(host=DB_MONGOHOST)    
    var_coll = mongo_client[ ds_id ][ "variants" ]
    bios_ids = set()
    for c, v in enumerate(variants.data):
        bs_id = v.get("biosample_id", False)
        if not bs_id:
            print("¡¡¡ The `biosample_id` parameter is required for variant assignment !!!")
            exit()
        if not "pgxbs-" in bs_id:
            print("¡¡¡ The `biosample_id` parameter has to start with 'pgxbs-' !!!")
            exit()
        bios_ids.add(bs_id)
    print(f'=> The variants file contains {var_no} variants from {len(bios_ids)} samples.')

    #-------------------- Chance for a clean interrupt... ---------------------#

    if BYC["TEST_MODE"]:
        proceed = input(f'Do you want to continue TESTING the database **{ds_id}**?\n(Y|n): ')
        if "n" in proceed.lower():
            exit()
    else:
        proceed = input(f'Do you want to continue to update database **{ds_id}**?\n(y|N): ')
        if not "y" in proceed.lower():
            exit()

    #------------ Deletion of existing variants based on selection ------------#
    if "y" in delSOvars.lower():
        for b_del in bios_ids:
            v_dels = var_coll.delete_many({"biosample_id": b_del, "variant_state.id":{"$regex":"SO:"}})
            print(f'==>> deleted {v_dels.deleted_count} variants from {b_del}')

    if "y" in delCNVvars.lower():
        for b_del in bios_ids:
            v_dels = var_coll.delete_many({"biosample_id": b_del, "variant_state.id":{"$regex":"EFO:"}})
            print(f'==>> deleted {v_dels.deleted_count} variants from {b_del}')


    #------------ Deletion of existing variants based on selection ------------#

    bios_v_counts = {}
    bar = Bar("Writing ", max = var_no, suffix='%(percent)d%%'+" of "+str(var_no) ) if not BYC["TEST_MODE"] else False

    for c, v in enumerate(variants.data, 1):
        bar.next() if not BYC["TEST_MODE"] else print(f'... next variant {c}')           
        bs_id = v.get("biosample_id", False)
        if not bs_id in bios_v_counts.keys():
            bios_v_counts.update({bs_id: 0})
        bios_v_counts[bs_id] += 1

        sid = v.get("variant_state_id", "")
        chro = v.get("reference_name", "")

        if len(sid) < 5 and len(chro) < 1:
            continue

        insert_v = {}
        # TODO: This is a bit of a double definition; disentangle ...
        insert_v.update( {
            "biosample_id": bs_id,            
            "analysis_id": v.get("analysis_id", re.sub("pgxbs-", "pgxcs-", bs_id)),
            "individual_id": v.get("individual_id", re.sub("pgxbs-", "pgxind-", bs_id))
        })

        insert_v = import_datatable_dict_line(insert_v, variants.fieldnames, v, "genomicVariant")
        insert_v = ByconVariant().pgxVariant(insert_v)
        insert_v.update({"updated": datetime.datetime.now().isoformat()})

        if not BYC["TEST_MODE"]:
            up_v_no += 1
            vid = var_coll.insert_one(insert_v).inserted_id
            vstr = f'pgxvar-{vid}'
            # print(f'\n{vstr}')
            var_coll.update_one({'_id':vid},{'$set':{ 'id':vstr }})
            # print(f'==> inserted {vstr} for sample {bs_id}')
        else:
            prjsonnice(insert_v)

    #----------------------------- Summary ------------------------------------#

    if not BYC["TEST_MODE"]:
        bar.finish()
        print(f'==> inserted {up_v_no} variants for {len(bios_v_counts.keys())} samples')
    else:
        print(bios_v_counts)


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
