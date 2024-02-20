#!/usr/bin/env python3
import pymongo
from pymongo import  MongoClient
from os import environ

biosample_id_list=[]
client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))

progenetix_biosample_id_list=[]
progenetix_individual_id_list=[]
progenetix_variant_id_list=[]
progenetix_callset_id_list=[]
client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
db=client['progenetix']
TCGA_cancers=['TCGA.MESO','TCGA.GBM','TCGA.BLCA','TCGA.UVM']
for cancer in TCGA_cancers:
    # NOTE: better use format strings: str('pgx:'+cancer) => f'pgx:{cancer}'
    bis_records=db['biosamples'].aggregate([
        { '$match': {'$and':[{'cohorts.id':{'$regex':'TCGAcancer'}},{'external_references.id':str('pgx:'+cancer)}]}},
        { '$sample': { 'size': 100 } }
    ])
    for bis_document in bis_records:
        progenetix_biosample_id_list.append(bis_document['id'])
        ind_records=db['individuals'].find({'id':bis_document['individual_id']})
        for ind_document in ind_records:
            progenetix_individual_id_list.append(ind_document['id'])
        var_records=db['variants'].find({'biosample_id':bis_document['id']})
        for var_document in var_records:
            progenetix_variant_id_list.append(var_document['id'])
        cal_records=db['variants'].find({'biosample_id':bis_document['id']})
        for cal_document in cal_records:
            progenetix_callset_id_list.append(cal_document['id'])

#TCGA samples with clinical information
TCGA_cancers=['TCGA.BRCA']
for cancer in TCGA_cancers:
    cellz_bis_records=db['biosamples'].aggregate([
        { '$match': {'$and':[{'external_references.id':str('pgx:'+cancer)},{'pathological_stage':{'$ne':None}}]}},
        { '$sample': { 'size': 100 } }
    ])
    for bis_document in cellz_bis_records:

        if len(list(client['examplez']['biosamples'].find({'id':bis_document['id']}))) ==0:
            progenetix_biosample_id_list.append(bis_document['id'])
            ind_records=db['individuals'].find({'id':bis_document['individual_id']})
            for ind_document in ind_records:
                if len(list(client['examplez']['individuals'].find({'id':ind_document['id']})))==0:
                    progenetix_individual_id_list.append(ind_document['id'])
            var_records=db['variants'].find({'biosample_id':bis_document['id']})
            for var_document in var_records:
                if len(list(client['examplez']['variants'].find({'id':var_document['id']}))) ==0:
                    progenetix_variant_id_list.append(var_document['id'])
            cal_records=db['variants'].find({'biosample_id':bis_document['id']})
            for cal_document in cal_records:
                if len(list(client['examplez']['analyses'].find({'id':cal_document['id']}))) ==0:
                    progenetix_callset_id_list.append(cal_document['id'])
cellz_biosample_id_list=[]
cellz_individual_id_list=[]
cellz_variant_id_list=[]
cellz_callset_id_list=[]
celllines=['cellosaurus:CVCL_0312']
child_cellines=[]
for celline in celllines:
    col_records=client['cellz']['collations'].find({'id':celline})
    for col_document in col_records:
        if col_document['child_terms'] !=[]:
            for child in col_document['child_terms']:
                child_cellines.append(child)
        else:
            child_cellines.append(celline)
for celline in child_cellines:
    bis_records=client['cellz']['biosamples'].find({'cellline_info.id':celline})
    for bis_document in bis_records:
        if len(list(client['examplez']['biosamples'].find({'id':bis_document['id']}))) ==0:
            cellz_biosample_id_list.append(bis_document['id'])
        ind_records=client['cellz']['individuals'].find({'id':bis_document['individual_id']})
        for ind_document in ind_records:
            if len(list(client['examplez']['individuals'].find({'id':ind_document['id']}))) ==0:
                cellz_individual_id_list.append(ind_document['id'])
        var_records=client['cellz']['variants'].find({'biosample_id':bis_document['id']})
        for var_document in var_records:
            if len(list(client['examplez']['variants'].find({'id':var_document['id']}))) ==0:
                cellz_variant_id_list.append(var_document['id'])
        cal_records=client['cellz']['variants'].find({'biosample_id':bis_document['id']})
        for cal_document in cal_records:
            if len(list(client['examplez']['analyses'].find({'id':cal_document['id']}))) ==0:
                cellz_callset_id_list.append(cal_document['id'])
import csv
with open("../imports/examplez_progenetix_biosamples.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(progenetix_biosample_id_list)
with open("../imports/examplez_cellz_biosamples.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(cellz_biosample_id_list)

