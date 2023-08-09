#!/usr/bin/env python3

import re, json, yaml
from copy import deepcopy
from progress.bar import Bar
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
    parse_variants(byc)
    select_dataset_ids(byc)

    v_d = byc["variant_parameters"]

    if len(byc["dataset_ids"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()

    ds_id = byc["dataset_ids"][0]

    if not byc["args"].inputfile:
        print("No input file file specified (-i, --inputfile) => quitting ...")
        exit()

    vb = ByconBundler(byc)
    variants = vb.read_pgx_file(byc["args"].inputfile)

    var_no = len(variants.data)
    up_v_no = 0

    delBiosVars, delSOvars, delCNVvars = ["n", "n", "n"]

    delBiosVars = input("Delete variants from matched biosamples before insertion?\n¡¡¡ This will remove ALL variants for each  `biosample_id` !!!\n(y|N): ")
    if not "y" in delBiosVars.lower():
        delSOvars = input('Delete only the sequence variants ("SO:...") from matched biosamples before insertion?\n(y|N): ')
        delCNVvars = input('Delete only the CNV variants ("EFO:...") from matched biosamples before insertion?\n(y|N): ')

    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    var_coll = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))[ ds_id ][ "variants" ]

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

    print("=> The variants file contains {} variants from {} samples.".format(var_no, len(bios_ids)))
    proceed = input("Do you want to continue to update database **{}**?\n(Y|n): ".format(ds_id))
    if "n" in proceed.lower():
        exit()

    if "y" in delBiosVars.lower():
        for b_del in bios_ids:
            v_dels = var_coll.delete_many({"biosample_id": b_del})
            print("==>> deleted {} variants from {}".format(v_dels.deleted_count, b_del))

    if not "y" in delBiosVars.lower():
        
        if "y" in delSOvars.lower():
            for b_del in bios_ids:
                v_dels = var_coll.delete_many({"biosample_id": b_del, "variant_state.id":{"$regex":"SO:"}})
                print("==>> deleted {} variants from {}".format(v_dels.deleted_count, b_del))

        if "y" in delCNVvars.lower():
            for b_del in bios_ids:
                v_dels = var_coll.delete_many({"biosample_id": b_del, "variant_state.id":{"$regex":"EFO:"}})
                print("==>> deleted {} variants from {}".format(v_dels.deleted_count, b_del))

    v_proto = object_instance_from_schema_name(byc, "pgxVariant", "") #pgxVariant

    bios_v_counts = {}

    if not byc["test_mode"]:
        bar = Bar("Writing ", max = var_no, suffix='%(percent)d%%'+" of "+str(var_no) )

    for c, v in enumerate(variants.data, 1):

        if not byc["test_mode"]:
                bar.next()

        bs_id = v.get("biosample_id", False)
        if not bs_id in bios_v_counts.keys():
            bios_v_counts.update({bs_id: 0})
        bios_v_counts[bs_id] += 1

        # variant prototype from schema
        insert_v = deepcopy(v_proto)

        # TODO: This is a bit of a double definition; disentangle ...
        insert_v.update( {
            "biosample_id": bs_id,            
            "callset_id": v.get("callset_id", re.sub("pgxbs-", "pgxcs-", bs_id)),
            "individual_id": v.get("individual_id", re.sub("pgxbs-", "pgxind-", bs_id))
        })

        insert_v = import_datatable_dict_line(byc, insert_v, variants.fieldnames, v, "genomicVariant")
        insert_v, errors = __normalize_pgx_variant(insert_v, byc, c)
        if len(errors) > 0:
            print("\n".join(errors))
            print(f'==> exit at variant line {c}; last import from line {c-1} <==')
            exit()
        insert_v.update({
            "variant_internal_id": __variant_create_digest(insert_v, byc),
            "updated": datetime.datetime.now().isoformat()
        })

        if not byc["test_mode"]:
            up_v_no += 1
            vid = var_coll.insert_one( insert_v  ).inserted_id
            vstr = 'pgxvar-'+str(vid)
            var_coll.update_one({'_id':vid},{'$set':{ 'id':vstr }})
            # print(f'==> inserted {vstr} for sample {bs_id}')
        else:
            prjsonnice(insert_v)

    if not byc["test_mode"]:
        bar.finish()
        print(f'==> inserted {up_v_no} variants for {len(bios_v_counts.keys())} samples')
    else:
        print(bios_v_counts)

################################################################################

def __variant_create_digest(v, byc):

    # TODO: remove; it is part of ByconVariant

    t = v["variant_state"]["id"]
    t = re.sub(":", "_", t)

    v_i = v["location"]
    return f'{v_i["chromosome"]}:{v_i["start"]}-{v_i["end"]}:{t}'

################################################################################

def __normalize_pgx_variant(variant, byc, counter=1):
    g_a = byc.get("genome_aliases", {})
    r_a = g_a.get("refseq_aliases", {})
    c_a = g_a.get("chro_aliases", {})
    v_t_defs = byc["variant_type_definitions"]
    errors = []

    var_id = variant.get("id", counter)

    seq_id = variant["location"].get("sequence_id")
    chromosome = variant["location"].get("chromosome")
    start = variant["location"].get("start")
    end = variant["location"].get("end")
    if not seq_id:
        if chromosome:
            variant["location"].update({"sequence_id": r_a.get(str(chromosome))})
    if not chromosome:
        if seq_id:
            variant["location"].update({"chromosome": c_a.get(str(seq_id))})
    if not isinstance(end, int):
        try:
            ref = variant.get("reference_sequence")
            alt = variant.get("sequence")
            v_l = len(ref) - len(alt)
            end_pos = start + abs(v_l) + 1
            # TODO: VRS would do a left-clipping -> start shift ...
            variant["location"].update({"end": end_pos})
        except:
            pass

    # TODO: Some fixes ...
    if "-" in variant.get("sequence", "."):
        variant["sequence"] = re.sub("-", "", variant["sequence"])
    if "-" in variant.get("reference_sequence", "."):
        variant["sequence"] = re.sub("-", "", variant["reference_sequence"])

    var_state_id = variant["variant_state"].get("id")
    variant_type = variant.get("variant_type")
    if not var_state_id:
        if variant_type:
            variant.update({ "variant_state": variant_state_from_variant_par(variant_type, byc) })

    try:
        variant["variant_state"].update({"label": v_t_defs[var_state_id].get("label")})
    except:
        pass


    for v_l_k in [ "sequence_id", "chromosome", "start", "end" ]:
        if not variant["location"].get(v_l_k):
            errors.append(f'¡¡¡ Parameter `location.{v_l_k}` undefined in variant {var_id} !!!')
    for v_s_k in [ "id", "label" ]:
        if not variant["variant_state"].get(v_s_k):
            errors.append(f'¡¡¡ Parameter `variant_state.{v_s_k}` undefined in variant {var_id} !!!')

    return variant, errors


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
