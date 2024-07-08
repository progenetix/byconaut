#!/usr/bin/env python3

import re, json, sys, datetime, requests, yaml
from copy import deepcopy
from os import path
from progress.bar import Bar
from tabulate import tabulate

from bycon import *

loc_path = path.dirname( path.abspath(__file__) )
services_lib_path = path.join( loc_path, pardir, "services", "lib" )
services_tmp_path = path.join( loc_path, pardir, "tmp" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from datatable_utils import import_datatable_dict_line
from service_helpers import generate_id
"""

"""

################################################################################
################################################################################
################################################################################

def main():
    iscn_defuser()

################################################################################

def iscn_defuser():
    initialize_bycon_service()

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

    vb = ByconBundler()
    iscndata = vb.read_pgx_file(input_file)
    for h in ["biosample_id", "iscn_fusions"]:
        if h not in iscndata.fieldnames:
            print(f'¡¡¡ "{h}" missing in header => giving up !!!')
            exit()

    #----------------------------- Summary ------------------------------------#

    variants = []
    v_s_id = "SO:0000806"
    v_s_label = "fusion"
    chro_names = ChroNames()
    l_no = 0
    bios_ids = []
    for s in iscndata.data:
        l_no += 1
        if not (bs_id := s.get("biosample_id")):
            print(f'¡¡¡ no biosample_id value in line {l_no} => skipping !!!')
            continue
        if bs_id in bios_ids:
            print(f'¡¡¡ existing biosample_id {bs_id} value in line {l_no} => skipping !!!')
            continue
        bios_ids.append(bs_id)
        if not (cs_id := s.get("analysis_id")):
            cs_id = generate_id("pgxfcs")
        for f_v_s in s.get("iscn_fusions").strip().split(','):
            f_id = generate_id("fusionId")
            for i_v in f_v_s.strip().split('::'):
                if not cb_pat.match(i_v):
                    print(f'¡¡¡ {l_no} - {bs_id}: {i_v} looks strange => skipping !!!')
                    continue
                cytoBands, chro, start, end, error = bands_from_cytobands(i_v)
                chroBases = "{}:{}-{}".format(chro, start, end)
                r_id = chro_names.refseq(chro)
                # print(f'{bs_id} / {cs_id}: {i_v} - {chroBases} / {r_id}')

                v = {
                    "biosample_id": bs_id,
                    "analysis_id": cs_id,
                    "sequence_id": r_id,
                    "start": str(start),
                    "end": str(end),
                    "variant_state_id": v_s_id,
                    "variant_state_label": v_s_label,
                    "variant_fusion_id": f_id
                }
                variants.append(v)

    # print(tabulate(variants, headers='keys', tablefmt="tsv", stralign=None, numalign=None))

    print(f'=> {len(bios_ids)} samples had a total of {len(variants)} variants')
    deff = open(output_file, "w")
    deff.write(tabulate(variants, headers='keys', tablefmt="tsv", stralign=None, numalign=None))
    deff.close()
    print(f'Wrote to {output_file}')

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
