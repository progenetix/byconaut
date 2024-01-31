#!/usr/bin/env python3
from os import environ
import sys, datetime
from isodate import date_isoformat
from pymongo import MongoClient
from progress.bar import Bar

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from bycon_plot import *
from interval_utils import generate_genome_bins, interval_cnv_arrays

"""

## `callsetsStatusmapsRefresher`

"""

################################################################################
"""
* `bin/callsetsStatusmapsRefresher.py -d 1000genomesDRAGEN -s variants`
* `bin/callsetsStatusmapsRefresher.py -d progenetix -s biosamples -f "icdom-81703"`
* `bin/callsetsStatusmapsRefresher.py`
  - default; new statusmaps for all `progenetix` analyses
"""
################################################################################

def main():

    callsets_refresher()

################################################################################

def callsets_refresher():

    initialize_bycon_service(byc)
    run_beacon_init_stack(byc)

    if len(byc["dataset_ids"]) > 1:
        print("Please give only one dataset using -d")
        exit()

    ds_id = byc["dataset_ids"][0]
    print(f'=> Using data values from {ds_id}')

    # re-doing the interval generation for non-standard CNV binning
    # genome_binning_from_args(byc)
    set_genome_rsrc_path(byc)
    generate_genome_bins(byc)

    data_client = MongoClient(host=byc["mongohost"])
    data_db = data_client[ ds_id ]
    cs_coll = data_db[ "analyses" ]
    v_coll = data_db[ "variants" ]

    record_queries = ByconQuery(byc).recordsQuery()

    execute_bycon_queries( ds_id, record_queries, byc )

    ds_results = byc["dataset_results"][ds_id]

    no_cnv_type = 0

    if not "analyses._id" in ds_results.keys():
        cs_ids = []
        for cs in cs_coll.find( {} ):
            cs_ids.append(cs["_id"])
        cs_no = len(cs_ids)
        print(f'¡¡¡ Using all {cs_no} analyses from {ds_id} !!!')
    else:
        cs_ids = ds_results["analyses._id"]["target_values"]
        cs_no = len(cs_ids)

    print(f'Re-generating statusmaps with {byc["genomic_interval_count"]} intervals for {cs_no} analyses...')
    bar = Bar("{} analyses".format(ds_id), max = cs_no, suffix='%(percent)d%%'+" of "+str(cs_no) )
    counter = 0
    updated = 0

    proceed = input(f'Do you want to continue to update database **{ds_id}**?\n(Y|n): ')
    if "n" in proceed.lower():
        exit()

    for _id in cs_ids:

        cs = cs_coll.find_one( { "_id": _id } )
        csid = cs["id"]
        counter += 1

        bar.next()

        if not "CNV" in cs.get("variant_class", "CNV"):
            no_cnv_type += 1
            continue

        # only the defined parameters will be overwritten
        cs_update_obj = { "info": cs.get("info", {}) }
        cs_update_obj["info"].pop("statusmaps", None)
        cs_update_obj["info"].pop("cnvstatistics", None)

        cs_vars = v_coll.find({ "callset_id": csid })
        maps, cs_cnv_stats, cs_chro_stats = interval_cnv_arrays(cs_vars, byc)

        cs_update_obj.update({"cnv_statusmaps": maps})
        cs_update_obj.update({"cnv_stats": cs_cnv_stats})
        cs_update_obj.update({"cnv_chro_stats": cs_chro_stats})
        cs_update_obj.update({ "updated": datetime.now().isoformat() })

        if not byc["test_mode"]:
            cs_coll.update_one( { "_id": _id }, { '$set': cs_update_obj }  )
            updated += 1
        else:
            prjsonnice(cs_chro_stats)

        ####################################################################
        ####################################################################
        ####################################################################

    bar.finish()

    print(f"{counter} analyses were processed")
    print(f"{no_cnv_type} analyses were not from CNV calling")
    print(f'{updated} analyses were updated for\n    `cnv_statusmaps`\n    `cnv_stats`\n    `cnv_chro_stats`\nusing {byc["genomic_interval_count"]} bins ({byc["interval_definitions"].get("genome_binning", "")})')

################################################################################
################################################################################
################################################################################


if __name__ == '__main__':
    main()
