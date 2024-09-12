#!/usr/bin/env python3
from os import environ
import sys, datetime
from isodate import date_isoformat
from pymongo import MongoClient
from progress.bar import Bar

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from interval_utils import generate_genome_bins, interval_cnv_arrays
from collation_utils import set_collation_types
from service_helpers import ask_limit_reset

"""

## `analysesStatusmapsRefresher`

"""

################################################################################
"""
* `bin/analysesStatusmapsRefresher.py -d progenetix -f "icdom-81703"`
* `bin/analysesStatusmapsRefresher.py`
  - default; new statusmaps for all `progenetix` analyses
"""
################################################################################

def main():
    analyses_refresher()

################################################################################

def analyses_refresher():
    initialize_bycon_service()
    generate_genome_bins()
    ask_limit_reset()

    if len(BYC["BYC_DATASET_IDS"]) > 1:
        print("Please give only one dataset using -d")
        exit()

    ds_id = BYC["BYC_DATASET_IDS"][0]
    set_collation_types()
    print(f'=> Using data values from {ds_id} for {BYC.get("genomic_interval_count", 0)} intervals...')

    limit = BYC_PARS.get("limit", 0)
    data_client = MongoClient(host=DB_MONGOHOST)
    data_db = data_client[ ds_id ]
    cs_coll = data_db[ "analyses" ]
    v_coll = data_db[ "variants" ]

    record_queries = ByconQuery().recordsQuery()
    ds_results = {}
    if len(record_queries["entities"].keys()) > 0:
        DR = ByconDatasetResults(ds_id, record_queries)
        ds_results = DR.retrieveResults()

    if not ds_results.get("analyses.id"):
        print(f'... collecting analysis id values from {ds_id} ...')
        cs_ids = []
        c_i = 0
        for ana in cs_coll.find( {} ):
            c_i += 1
            cs_ids.append(ana["id"])
            if limit > 0:
                if limit == c_i:
                    break
        cs_no = len(cs_ids)
        print(f'¡¡¡ Using {cs_no} analyses from {ds_id} !!!')
    else:
        cs_ids = ds_results["analyses.id"]["target_values"]
        cs_no = len(cs_ids)

    print(f'Re-generating statusmaps with {BYC["genomic_interval_count"]} intervals for {cs_no} analyses...')
    bar = Bar("{} analyses".format(ds_id), max = cs_no, suffix='%(percent)d%%'+" of "+str(cs_no) )
    counter = 0
    updated = 0

    proceed = input(f'Do you want to continue to update database **{ds_id}**?\n(Y|n): ')
    if "n" in proceed.lower():
        exit()

    no_cnv_type = 0
    for ana_id in cs_ids:

        ana = cs_coll.find_one( { "id": ana_id } )
        _id = ana.get("_id")
        counter += 1

        bar.next()

        if "SNV" in ana.get("variant_class", "CNV"):
            no_cnv_type += 1
            continue

        # only the defined parameters will be overwritten
        cs_update_obj = { "info": ana.get("info", {}) }
        cs_update_obj["info"].pop("statusmaps", None)
        cs_update_obj["info"].pop("cnvstatistics", None)

        cs_vars = v_coll.find({ "analysis_id": ana_id })
        maps, cs_cnv_stats, cs_chro_stats = interval_cnv_arrays(cs_vars)

        cs_update_obj.update({"cnv_statusmaps": maps})
        cs_update_obj.update({"cnv_stats": cs_cnv_stats})
        cs_update_obj.update({"cnv_chro_stats": cs_chro_stats})
        cs_update_obj.update({ "updated": datetime.datetime.now().isoformat() })

        if BYC.get("TEST_MODE", False) is True: 
            prjsonnice(cs_chro_stats)
        else:
            cs_coll.update_one( { "_id": _id }, { '$set': cs_update_obj }  )
            updated += 1

        ####################################################################
        ####################################################################
        ####################################################################

    bar.finish()

    print(f"{counter} analyses were processed")
    print(f"{no_cnv_type} analyses were not from CNV calling")
    print(f'{updated} analyses were updated for\n    `cnv_statusmaps`\n    `cnv_stats`\n    `cnv_chro_stats`\nusing {BYC["genomic_interval_count"]} bins ({BYC_PARS.get("genome_binning", "")})')

################################################################################
################################################################################
################################################################################


if __name__ == '__main__':
    main()
