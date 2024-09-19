#!/usr/bin/env python3

import re, json, sys, datetime, requests, yaml
from copy import deepcopy
from os import path
from progress.bar import Bar
from tabulate import tabulate

from bycon import *
from bycon.services import bycon_bundler, datatable_utils, file_utils, service_helpers

loc_path = path.dirname( path.abspath(__file__) )
services_tmp_path = path.join( loc_path, pardir, "tmp" )

"""

"""

################################################################################
################################################################################
################################################################################

def main():
    initialize_bycon_service()
    if len(BYC["BYC_DATASET_IDS"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()
    ds_id = BYC["BYC_DATASET_IDS"][0]

    # sheet => export => publish to web => tsv, seelcted sheet
    g_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTrpE6-SQAT3jVavxzqQXwAs5ujr2lDJehYwKiFUWA-tEm2DyyGTwS1UcnJAYF5VZJs4SlojUtm-Rh7/pub?gid=726541419&single=true&output=tsv"
    argdefs = BYC.get("argument_definitions", {})
    cb_pat = re.compile( argdefs["cyto_bands"]["items"]["pattern"] )
    input_file = BYC_PARS.get("inputfile")
    output_file = BYC_PARS.get("outputfile")
    if not input_file:
        # print("No input file file specified (-i, --inputfile) => quitting ...")
        # exit()
        print("No inputfile file specified => pulling the online table ...")
        input_file = path.join( services_tmp_path, "iscntable.tsv" )
        print(f'... reading from {g_url}')
        r =  requests.get(g_url)
        if r.ok:
            with open(input_file, 'wb') as f:
                f.write(r.content)
            print(f"Wrote file to {input_file}")
        else:
            print(f'Download failed: status code {r.status_code}\n{r.text}')
            exit()

    if not output_file:
        output_file = re.sub(".tsv", "_defused.tsv", input_file)

    #-------------------------- Read ISCN from file ---------------------------#

    vb = bycon_bundler.ByconBundler()
    iscndata = vb.read_pgx_file(input_file)
    for h in ["biosample_id", "iscn_fusions"]:
        if h not in iscndata.fieldnames:
            print(f'¡¡¡ "{h}" missing in header => giving up !!!')
            exit()

    #----------------------------- Summary ------------------------------------#

    log = []
    analyses = []
    variants = []
    variant_state = {
        "id": "SO:0000806",
        "label": "fusion"
    }
    analysis = {
        "platform_id": 'NCIT:C16437',
        "platform_label": 'Chromosome Banding',
        "analysis_operation_id": 'NCIT:C16437',
        "analysis_operation_label": 'Chromosome Banding'
    }

    chro_names = ChroNames()
    l_no = 0
    bios_ids = []
    mongo_client = MongoClient(host=DB_MONGOHOST)    
    bios_coll = mongo_client[ ds_id ][ "biosamples" ]
    for s in iscndata.data:
        l_no += 1
        if not (bios_id := s.get("biosample_id")):
            # print(f'¡¡¡ no biosample_id value in line {l_no} => skipping !!!')
            continue
        if not (b := bios_coll.find_one({"id": bios_id})):
            log.append(f'{bios_id}\tmissing in {ds_id}.biosamples')
            continue      
        if bios_id in bios_ids:
            print(f'¡¡¡ existing biosample_id {bios_id} value in line {l_no} => skipping !!!')
            log.append(f'{bios_id}\tskipped biosample_id double in line {l_no}')        
            continue
        bios_ids.append(bios_id)
        if not (ind_id := b.get("individual_id")):
            print(f'¡¡¡ existing biosample_id {bios_id} value in line {l_no} => skipping !!!')
            log.append(f'{bios_id}\tskipped individual_id missing in line {l_no}')        
            continue
        if not (ana_id := s.get("analysis_id")):
            # this creates a deterministic analysis id to avoid re-imports
            # from new / random id generation
            ana_id = bios_id.replace("pgxbs", "pgxcsfs")
            a = {
                "biosample_id": bios_id,
                "analysis_id": ana_id,
                "individual_id": ind_id
            }
            a.update(analysis)
            analyses.append(a)

        for f_s in s.get("iscn_fusions").strip().split(','):
            # all 2 or more fusions get the same id - e.g. a three way
            # t(8;14;18)(q24;q32;q21) => 8q24::14q32&&14q32::18q21 
            f_id = service_helpers.generate_id("fusionId")
            for f_v_s in f_s.split('&&'):

                # print(f_v_s)

                v = {
                    "biosample_id": bios_id,
                    "analysis_id": ana_id,
                    "individual_id": ind_id,
                    "variant_state_id": variant_state["id"],
                    "variant_state_label": variant_state["label"],
                    "variant_fusion_id": f_id,
                    "adjoined_sequences": []
                }

                s_t = None
                for t in ["<=>", "<|>", ">=>", "<=<", ">=<", ">|>", "<|<", ">|<"]:
                    if (s_t := t) in f_v_s:
                        break
                if not s_t:
                    print(f'¡¡¡ {l_no} - {bios_id}: {f_v_s} does not contain a known fusion symbol => skipping !!!')
                    continue

                if len(f_v_s.strip().split(s_t)) != 2:
                    print(f'¡¡¡ {l_no} - {bios_id}: {f_v_s} does not split into 2 elements => skipping !!!')
                    continue

                i_v_1, i_v_2 = f_v_s.strip().split('::')
                if not cb_pat.match(i_v_1):
                    print(f'¡¡¡ {l_no} - {bios_id}: {i_v_1} looks strange => skipping !!!')
                    continue
                if not cb_pat.match(i_v_2):
                    print(f'¡¡¡ {l_no} - {bios_id}: {i_v_2} looks strange => skipping !!!')
                    continue
                cytoBands_1, chro_1, start_1, end_1, error_1 = bands_from_cytobands(i_v_1)
                chroBases_1 = "{}:{}-{}".format(chro_1, start_1, end_1)
                r_id_1 = chro_names.refseq(chro_1)
                cytoBands_2, chro_2, start_2, end_2, error_2 = bands_from_cytobands(i_v_2)
                chroBases_2 = "{}:{}-{}".format(chro_2, start_2, end_2)
                r_id_2 = chro_names.refseq(chro_2)

                v.update({
                    "adjoined_sequences": f'{r_id_1}::{chro_1}::{start_1}::{end_1},{r_id_2}::{chro_2}::{start_2}::{end_2}'
                })                    
                variants.append(v)
                # TODO: fast reverse partner
                v_comp = deepcopy(v)
                v_comp.update({
                    "adjoined_sequences": f'{r_id_2}::{chro_2}::{start_2}::{end_2},{r_id_1}::{chro_1}::{start_1}::{end_1}'
                })                    
                variants.append(v_comp)

    # print(tabulate(variants, headers='keys', tablefmt="tsv", stralign=None, numalign=None))

    print(f'=> {len(bios_ids)} samples had a total of {len(variants)} variants')
    deff = open(output_file, "w")
    deff.write(tabulate(variants, headers='keys', tablefmt="tsv", stralign=None, numalign=None))
    deff.close()
    print(f'Wrote to {output_file}')

    if len(log) > 0:
        file_utils.write_log(log, output_file)
        exit()

    if len(analyses) > 0:
        print(f'=> {len(analyses)} analyses were written to an import file')
        output_file = output_file.replace(".tsv", "_analyses.tsv")
        ana = open(output_file, "w")
        ana.write(tabulate(analyses, headers='keys', tablefmt="tsv", stralign=None, numalign=None))
        ana.close()
        print(f'Wrote to {output_file}')


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
