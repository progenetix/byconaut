#!/usr/bin/env python3

import re, json, yaml, sys, datetime
from copy import deepcopy
from isodate import date_isoformat
from os import path, environ, pardir, system
from pymongo import MongoClient
from progress.bar import Bar

from bycon import *

dir_path = path.dirname( path.abspath(__file__) )
lib_path = path.join(dir_path , "lib")
sys.path.append( lib_path )
from mongodb_utils import mongodb_update_indexes
from doc_generator import doc_generator

services_conf_path = path.join( dir_path, "config" )
services_lib_path = path.join( dir_path, pardir, "services", "lib" )
sys.path.append( services_lib_path )
from collation_utils import *
from service_helpers import *

"""
The housekeeping script contains **non-destructive** maintenance scripts which
e.g. may insert derived helper values (e.g. `age_days`).
"""

################################################################################
################################################################################
################################################################################

def main():
    housekeeping()

################################################################################

def housekeeping():
    initialize_bycon_service(byc, "housekeeping")
    read_service_prefs("housekeeping", services_conf_path, byc)

    # TODO: rewrap, use config etc.
    generated_docs_path = path.join( dir_path, pardir, "docs", "generated")
    bycon_generated_docs_path = path.join( dir_path, pardir, pardir, "bycon", "docs", "generated")
    doc_generator(byc, generated_docs_path)
    doc_generator(byc, bycon_generated_docs_path)

    if len(byc["dataset_ids"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()

    ds_id = byc["dataset_ids"][0]

    # collecting the actions
    todos = {
        "mongodb_index_creation": input("Check & refresh MongoDB indexes?\n(y|N): "),
        "individual_age_days": input("Recalculate `age_days` in individuals?\n(y|N): "),
        "analyses_labels": input("Create/update `label` field for analyses, from biosamples?\n(y|N): "),
        "update_cs_statusmaps": input(f'Update statusmaps in `analyses` for {ds_id}?\n(y|N): '),
        "update_collations": input(f'Create `collations` for {ds_id}?\n(Y|n): '),
        "update_frequencymaps": input(f'Create `frequencymaps` for {ds_id} collations?\n(Y|n): '),
        "datasets_counts": input("Recalculate counts for all datasets?\n(y|N): ")
    }

    data_db = MongoClient(host=DB_MONGOHOST)[ ds_id ]

    #>-------------------- MongoDB index updates -----------------------------<#

    if "y" in todos.get("mongodb_index_creation", "n").lower():
        print(f'\n{__hl()}==> updating indexes for {ds_id}"')
        mongodb_update_indexes(ds_id, byc)

    #>------------------- / MongoDB index updates ----------------------------<#

    #>------------------------- analyses -------------------------------------<#

    if "y" in todos.get("analyses_labels", "n").lower():
        cs_query = {}
        analyses_coll = data_db["analyses"]
        bios_coll = data_db["biosamples"]
        no = analyses_coll.count_documents(cs_query)
        if not BYC["TEST_MODE"]:
            bar = Bar(f"=> `labels` for {no} analyses", max = no, suffix='%(percent)d%%'+" of "+str(no) )
        for cs in analyses_coll.find(cs_query):
            if not BYC["TEST_MODE"]:
                bar.next()
            bs_id = cs.get("biosample_id", "___none___")
            bios = bios_coll.find_one({"id": bs_id})
            if not bios:
                continue
            bs_label = bios.get("label", "")
            if len(bs_label) < 2:
                bs_label = bios.get("notes", "")
            if len(bs_label) < 2:
                bs_label = bs_id

            # TODO: this is very temporary .........
            bs_label = re.sub(r'Acute myeloid leukemia', 'AML', bs_label, flags=re.I)
            bs_label = re.sub(r'Acute myeloblastic leukemia with maturation', 'AML', bs_label, flags=re.I)
            bs_label = re.sub(r'Acute myeloblastic leukemia without maturation', 'AML', bs_label, flags=re.I)
            bs_label = re.sub(r'Acute myeloblastic leukemia with minimal differentiation', 'AML', bs_label, flags=re.I)
            bs_label = re.sub(r'Acute lymphoblastic leukemia', 'AML', bs_label, flags=re.I)
            bs_label = re.sub(r'cell line', 'CL', bs_label, flags=re.I)
            bs_label = re.sub(r'myelodysplastic syndrome', 'MDS', bs_label, flags=re.I)
            bs_label = re.sub(r'Refractory cytopenia with multilineage dysplasia and ring sideroblasts$', 'MDS [RCMD]', bs_label, flags=re.I)
            bs_label = re.sub(r'B-cell chronic lymphocytic leukemia/small lymphocytic lymphoma', 'CLL', bs_label, flags=re.I)
            bs_label = re.sub(r'chronic lymphocytic leukemia', 'CLL', bs_label, flags=re.I)
            bs_label = re.sub(r'myeloid proliferative disorder', 'MPS', bs_label, flags=re.I)
            bs_label = re.sub(r'non-Hodgkins Lymphoma', 'NHL', bs_label, flags=re.I)
            bs_label = re.sub(r'Diffuse large B-cell lymphoma', 'DLBCL', bs_label, flags=re.I)
            bs_label = re.sub(r'classic medulloblastoma', 'medulloblastoma', bs_label, flags=re.I)
            bs_label = re.sub(r'nodular/desmoplastic medulloblastoma', 'medulloblastoma [N/D]', bs_label, flags=re.I)
            bs_label = re.sub(r'Anaplastic pleomorphic xanthoastrocytoma', 'xanthoastrocytoma [AP]', bs_label, flags=re.I)
            bs_label = re.sub(r'colorectal cancer', 'CRC', bs_label, flags=re.I)
            bs_label = re.sub(r'ovarian carcinoma', 'OvCa', bs_label, flags=re.I)
            bs_label = re.sub(r'breast carcinoma', 'BrCa', bs_label, flags=re.I)
            bs_label = re.sub(r'prostate carcinoma', 'PrCa', bs_label, flags=re.I)
            bs_label = re.sub(r'Hepatocellular carcinoma', 'HepCa', bs_label, flags=re.I)
            bs_label = re.sub(r'Myelodysplasia', 'MDS', bs_label, flags=re.I)
            bs_label = re.sub(r'Malignant peripheral nerve sheath tumor', 'MPNST', bs_label, flags=re.I)
            bs_label = re.sub(r'neurofibromatosis', 'NF', bs_label, flags=re.I)
            bs_label = re.sub(r'glioblastoma multiforme', 'GBM', bs_label, flags=re.I)
            bs_label = re.sub(r'glioblastoma$', 'GBM', bs_label, flags=re.I)
            bs_label = re.sub(r'pilocytic astrocytoma', 'pil. astrocytoma', bs_label, flags=re.I)
            bs_label = re.sub(r'Pediatric undifferentiated sarcoma', 'ped. undiff. sarcoma', bs_label, flags=re.I)
            bs_label = re.sub(r'mycosis fungoides', 'MF', bs_label, flags=re.I)
            bs_label = re.sub(r'burkitt lymphoma', 'BL', bs_label, flags=re.I)
            bs_label = re.sub(r'Anaplastic large cell lymphoma', 'ALCL', bs_label, flags=re.I)
            bs_label = re.sub(r'Peripheral T-cell lymphoma', 'PTNHL', bs_label, flags=re.I)
            bs_label = re.sub(r'follicular lymphoma', 'FCL', bs_label, flags=re.I)
            bs_label = re.sub(r'renal cell carcinoma', 'RCC', bs_label, flags=re.I)
            bs_label = re.sub(r'Mantle cell lymphoma', 'MCL', bs_label, flags=re.I)
            bs_label = re.sub(r'Marginal zone lymphoma', 'MZL', bs_label, flags=re.I)
            bs_label = re.sub(r'marginal zone B-cell lymphoma', 'MZL', bs_label, flags=re.I)
            bs_label = re.sub(r'squamous cell carcinoma', 'SCC', bs_label, flags=re.I)
            bs_label = re.sub(r'pancreas ductal adenocarcinoma', 'PDAC', bs_label, flags=re.I)
            bs_label = re.sub(r'gastrointestinal stromal tumor', 'GIST', bs_label, flags=re.I)
            bs_label = re.sub(r'diffuse intrinsic pontine glioma', 'DIPG', bs_label, flags=re.I)
            bs_label = re.sub(r' Head and neck SCC', 'HNSCC', bs_label, flags=re.I)
            bs_label = re.sub(r'T-cell lymphoblastic lymphoma', 'T-NHL', bs_label, flags=re.I)
            bs_label = re.sub(r'Infant Acute Lymphoblastic Leukaemia', 'ALL', bs_label, flags=re.I)
            bs_label = re.sub(r'Wilms\' Tumor', 'retinoblastoma', bs_label, flags=re.I)
            bs_label = re.sub(r'Unclassified RCC', 'RCC', bs_label, flags=re.I)
            bs_label = re.sub(r'Thyroid Carcinoma', 'ThyCa', bs_label, flags=re.I)
            bs_label = re.sub(r'adenocarcinoma', 'AdCa', bs_label, flags=re.I)
            bs_label = re.sub(r'  ', ' ', bs_label, flags=re.I)
            bs_label = re.sub(r'Nasopharyngeal carcinoma', 'NPC', bs_label, flags=re.I)
            bs_label = re.sub(r'microsatellite stable', 'MSS', bs_label, flags=re.I)
            bs_label = re.sub(r'microsatellite unstable', 'MSI', bs_label, flags=re.I)
            bs_label = re.sub(r'Non-small cell carcinoma', 'NSCC', bs_label, flags=re.I)
            bs_label = re.sub(r'primitive neuroectodermal tumor', 'PNET', bs_label, flags=re.I)
            bs_label = re.sub(r'Central nervous system', 'CNS', bs_label, flags=re.I)

            if BYC["TEST_MODE"] is True:
                print(f'{cs["id"]} => {bs_label}')
            else:
                analyses_coll.update_one({"_id": cs["_id"]}, {"$set": {"label": bs_label}})
                
        if not BYC["TEST_MODE"]:
            bar.finish()

    #>------------------------------------------------------------------------<#

    if "y" in todos.get("update_cs_statusmaps", "y").lower():
        print(f'==> executing "{dir_path}/analysesStatusmapsRefresher.py -d {ds_id}"')
        system(f'{dir_path}/analysesStatusmapsRefresher.py -d {ds_id}')

    #>------------------------ / analyses ------------------------------------<#

    #>------------------------ individuals -----------------------------------<#

    ind_coll = data_db["individuals"]

    # age_days
    if "y" in todos.get("individual_age_days", "n").lower():
        query = {"index_disease.onset.age": {"$regex": "^P\d"}}
        no = ind_coll.count_documents(query)
        bar = Bar(f"=> `age_days` ...", max = no, suffix='%(percent)d%%'+" of "+str(no) )

        age_c = 0
        for ind in ind_coll.find(query):
            age_days = days_from_iso8601duration(ind["index_disease"]["onset"]["age"])
            if age_days is False:
                continue
            ind_coll.update_one({"_id": ind["_id"]}, {"$set": {"index_disease.onset.age_days": age_days}})
            age_c += 1
            bar.next()

        bar.finish()

        print(f'=> {age_c} individuals received an `index_disease.onset.age_days` value.')

    #>----------------------- / individuals ----------------------------------<#

    #>---------------------- info db update ----------------------------------<#

    if "y" in todos.get("datasets_counts", "n").lower():

        print(f'\n{__hl()}==> Updating dataset statistics in "{HOUSEKEEPING_DB}.{HOUSEKEEPING_INFO_COLL}"')

        b_info = __dataset_update_counts(byc)

        info_coll = MongoClient(host=DB_MONGOHOST)[ HOUSEKEEPING_DB ][ HOUSEKEEPING_INFO_COLL ]
        info_coll.delete_many( { "date": b_info["date"] } ) #, upsert=True
        info_coll.insert_one( b_info ) #, upsert=True 

        print(f'\n{__hl()}==> updated entry {b_info["date"]} in {HOUSEKEEPING_DB}.{HOUSEKEEPING_INFO_COLL}')

    #>--------------------- / info db update ---------------------------------<#

    #>---------------------- update collations -------------------------------<#

    if not "n" in todos.get("update_collations", "y").lower():
        print(f'\n{__hl()}==> executing "{dir_path}/collationsCreator.py -d {ds_id}"\n')
        system(f'{dir_path}/collationsCreator.py -d {ds_id}')

    #>--------------------- / update collations ------------------------------<#

    #>--------------------- update frequencymaps -----------------------------<#

    if not "n" in todos.get("update_frequencymaps", "y").lower():
        print(f'\n{__hl()}==> executing "{dir_path}/frequencymapsCreator.py -d {ds_id}"\n')
        system(f'{dir_path}/frequencymapsCreator.py -d {ds_id}')

    #>-------------------- / update frequencymaps ----------------------------<#

################################################################################
#################################### subs ######################################
################################################################################

def __dataset_update_counts(byc):

    b_info = { "date": date_isoformat(datetime.datetime.now()), "datasets": { } }
    mongo_client = MongoClient(host=DB_MONGOHOST)

    # this is independend of the dataset selected for the script & will update
    # for all in any run
    dbs = mongo_client.list_database_names()
    for i_ds_id in byc["dataset_definitions"].keys():
        if not i_ds_id in dbs:
            print(f'¡¡¡ Dataset "{i_ds_id}" does not exist !!!')
            continue

        ds_db = mongo_client[ i_ds_id ]
        b_i_ds = { "counts": { }, "updated": datetime.datetime.now().isoformat() }
        c_n = ds_db.list_collection_names()
        for c in ["biosamples", "individuals", "variants", "analyses"]:
            if c not in c_n:
                continue

            no = ds_db[ c ].estimated_document_count()
            b_i_ds["counts"].update( { c: no } )
            if c == "variants":
                v_d = { }
                bar = Bar(i_ds_id+' variants', max = no, suffix='%(percent)d%%'+" of "+str(no) )
                for v in ds_db[ c ].find({ "variant_internal_id": {"$exists": True }}):
                    v_d[ v["variant_internal_id"] ] = 1
                    bar.next()
                bar.finish()
                b_i_ds["counts"].update( { "variants_distinct": len(v_d.keys()) } )

        b_info["datasets"].update({i_ds_id: b_i_ds})
    
    return b_info

################################################################################

def __hl():
    return "".join(["#"] * 80) + "\n"

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
