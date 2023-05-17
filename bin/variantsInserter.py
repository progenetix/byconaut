#!/usr/local/bin/python3

import re, json, yaml
from os import path, environ, pardir
import sys, datetime

from bycon import *

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
    parse_variant_parameters(byc)
    select_dataset_ids(byc)

    if len(byc["dataset_ids"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()

    ds_id = byc["dataset_ids"][0]

    if not byc["args"].inputfile:
        print("No input file file specified (-i, --inputfile) => quitting ...")
        exit()

    inputfile = byc["args"].inputfile
    variants, fieldnames = read_tsv_to_dictlist(inputfile, int(byc["args"].limit))

    var_no = len(variants)
    up_v_no = 0

    delBiosVars = input("Delete variants from matched biosamples before insertion?\n(y|N): ")

    mongo_client = MongoClient( )
    var_coll = MongoClient( )[ ds_id ][ "variants" ]

    bios_ids = set()
    for c, v in enumerate(variants):
        bs_id = v.get("biosample_id", False)
        if not bs_id:
            print("¡¡¡ The `biosample_id` parameter is required for variant assignment !!!")
            exit()
        if not "pgxbs-" in bs_id:
            print("¡¡¡ The `biosample_id` parameter has to start with 'pgxbs-' !!!")
            exit()
        bios_ids.add(bs_id)

    print("=> The variants file contains {} variants from {} samples.".format(var_no, len(bios_ids)))
    proceed = input("Do you want to continue to update database **{}**?\n(Y|n): ".format(ds_id))
    if "n" in proceed.lower():
        exit()

    if "y" in delBiosVars.lower():
        for b_del in bios_ids:
            v_d = var_coll.delete_many({"biosample_id": b_del})
            print("==>> deleted {} variants from {}".format(v_d.deleted_count, b_del))

    for c, v in enumerate(variants):

        bs_id = v.get("biosample_id", False)

        n = str(c+1)

        # TODO: variant prototype from schema
        insert_v = {}
 
        # TODO: This is a bit of a double definition; disentangle ...
        insert_v.update( {
            "legacy_id": v.get("variant_id", "pgxvar-"+n),
            "biosample_id": bs_id,            
            "callset_id": v.get("callset_id", re.sub("pgxbs-", "pgxcs-", bs_id)),
            "individual_id": v.get("individual_id", re.sub("pgxbs-", "pgxind-", bs_id))
        })

        insert_v = import_datatable_dict_line(byc, insert_v, fieldnames, v, "variant")
        insert_v.update({
            "variant_internal_id": variant_create_digest(insert_v, byc),
            "updated": datetime.datetime.now().isoformat()
        })

        if not byc["test_mode"]:
            vid = var_coll.insert_one( insert_v  ).inserted_id
            vstr = 'pgxvar-'+str(vid)
            var_coll.update_one({'_id':vid},{'$set':{ 'id':vstr }})
            print("=> inserted {}".format(vstr))
        else:
            prjsonnice(insert_v)

    exit()

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
