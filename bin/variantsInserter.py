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
    variantsInserter()

################################################################################

def variantsInserter():

    initialize_bycon_service(byc)
    parse_variants(byc)
    select_dataset_ids(byc)

    v_d = byc["variant_parameters"]
    args = byc.get("args", {})

    if len(byc["dataset_ids"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()

    ds_id = byc["dataset_ids"][0]

    if not args.inputfile:
        print("No input file file specified (-i, --inputfile) => quitting ...")
        exit()

    if not byc["test_mode"]:
        tmi = input("Do you want to run in TEST MODE (i.e. no database insertions/updates)?\n(Y|n): ")
        if not "n" in tmi.lower():
            byc.update({"test_mode": True})

    if byc["test_mode"] is True:
            print("... running in TEST MODE")

    vb = ByconBundler(byc)
    variants = vb.read_pgx_file(args.inputfile)

    var_no = len(variants.data)
    up_v_no = 0

    delBiosVars, delSOvars, delCNVvars = ["n", "n", "n"]

    if not byc["test_mode"]:
        delBiosVars = input("Delete variants from matched biosamples before insertion?\n¡¡¡ This will remove ALL variants for each  `biosample_id` !!!\n(y|N): ")
        if not "y" in delBiosVars.lower():
            delSOvars = input('Delete only the sequence variants ("SO:...") from matched biosamples before insertion?\n(y|N): ')
            delCNVvars = input('Delete only the CNV variants ("EFO:...") from matched biosamples before insertion?\n(y|N): ')

    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))    
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
    proceed = input(f'Do you want to continue to update database **{ds_id}**?\n(Y|n): ')
    if "n" in proceed.lower():
        exit()

    if "y" in delBiosVars.lower():
        for b_del in bios_ids:
            v_dels = var_coll.delete_many({"biosample_id": b_del})
            print(f'==>> deleted {v_dels.deleted_count} variants from {b_del}')

    if not "y" in delBiosVars.lower():
        
        if "y" in delSOvars.lower():
            for b_del in bios_ids:
                v_dels = var_coll.delete_many({"biosample_id": b_del, "variant_state.id":{"$regex":"SO:"}})
                print(f'==>> deleted {v_dels.deleted_count} variants from {b_del}')

        if "y" in delCNVvars.lower():
            for b_del in bios_ids:
                v_dels = var_coll.delete_many({"biosample_id": b_del, "variant_state.id":{"$regex":"EFO:"}})
                print(f'==>> deleted {v_dels.deleted_count} variants from {b_del}')

    bios_v_counts = {}
    
    bar = Bar("Writing ", max = var_no, suffix='%(percent)d%%'+" of "+str(var_no) ) if not byc["test_mode"] else False

    for c, v in enumerate(variants.data, 1):

        bar.next() if not byc["test_mode"] else False
                
        bs_id = v.get("biosample_id", False)
        if not bs_id in bios_v_counts.keys():
            bios_v_counts.update({bs_id: 0})
        bios_v_counts[bs_id] += 1

        insert_v = {}

        # TODO: This is a bit of a double definition; disentangle ...
        insert_v.update( {
            "biosample_id": bs_id,            
            "callset_id": v.get("callset_id", re.sub("pgxbs-", "pgxcs-", bs_id)),
            "individual_id": v.get("individual_id", re.sub("pgxbs-", "pgxind-", bs_id))
        })

        insert_v = import_datatable_dict_line(byc, insert_v, variants.fieldnames, v, "genomicVariant")
        prdbug(byc, insert_v)
        insert_v = ByconVariant(byc).pgxVariant(insert_v)
        insert_v.update({"updated": datetime.datetime.now().isoformat()})

        if not byc["test_mode"]:
            up_v_no += 1
            vid = var_coll.insert_one(insert_v).inserted_id
            vstr = f'pgxvar-{vid}'
            print(f'\n{vstr}')
            var_coll.update_one({'_id':vid},{'$set':{ 'id':vstr }})
            print(f'==> inserted {vstr} for sample {bs_id}')
        else:
            prjsonnice(insert_v)

    if not byc["test_mode"]:
        bar.finish()
        print(f'==> inserted {up_v_no} variants for {len(bios_v_counts.keys())} samples')
    else:
        print(bios_v_counts)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
