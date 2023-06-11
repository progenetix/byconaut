#!/usr/bin/env python3

import re, json, yaml
from copy import deepcopy
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

    v_d = byc["variant_definitions"]

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
            v_dels = var_coll.delete_many({"biosample_id": b_del})
            print("==>> deleted {} variants from {}".format(v_dels.deleted_count, b_del))

    v_proto = object_instance_from_schema_name(byc, "pgxVariant", "") #pgxVariant

    for c, v in enumerate(variants, 1):

        bs_id = v.get("biosample_id", False)

        # variant prototype from schema
        insert_v = deepcopy(v_proto)

        # TODO: This is a bit of a double definition; disentangle ...
        insert_v.update( {
            "legacy_id": v.get("variant_id", f'pgxvar-{c:09d}'),
            "biosample_id": bs_id,            
            "callset_id": v.get("callset_id", re.sub("pgxbs-", "pgxcs-", bs_id)),
            "individual_id": v.get("individual_id", re.sub("pgxbs-", "pgxind-", bs_id))
        })

        insert_v = import_datatable_dict_line(byc, insert_v, fieldnames, v, "variant")

        seq_id = insert_v["location"].get("sequence_id")
        chromosome = insert_v["location"].get("chromosome")
        var_id = insert_v["variant_state"].get("id")
        var_label = insert_v["variant_state"].get("label")

        if not seq_id and not chromosome:
            print(f"Neither `reference_name` (chromosome) nor `sequence_id` were specified in line {c}...")
            exit()

        if not seq_id:
            if chromosome in v_d["refseq_aliases"].keys():
                r_id = v_d["refseq_aliases"][chromosome]
                insert_v["location"].update({"sequence_id": r_id})
        if not chromosome:
            if seq_id in v_d["chro_aliases"].keys():
                c_id = v_d["refseq_aliases"][seq_id]
                insert_v["location"].update({"chromosome": v_d["chro_aliases"].get(seq_id)})
        if not var_label:
            if var_id in v_d["efo_dupdel_map"].keys():
                insert_v["variant_state"].update({"label": v_d["efo_dupdel_map"][var_id].get("label")})

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
