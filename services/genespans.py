#!/usr/bin/env python3

import cgi
import re, json
from os import path, pardir, environ
import sys
from pymongo import MongoClient

from bycon import *

################################################################################
################################################################################
################################################################################

def main():

    try:
        genespans()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def genespans():

    byc.update({
        "request_path_root": "services",
        "request_entity_path_id": "genespans"
    })
    initialize_bycon_service(byc)        
    parse_variants(byc)
    generate_genomic_mappings(byc)
    get_global_filter_flags(byc)
    create_empty_service_response(byc)


    assembly_id = byc["assembly_id"]
    if "assembly_id" in byc[ "form_data" ]:
        aid = byc[ "form_data" ]["assembly_id"]
        if aid in byc["service_config"]["assembly_ids"]:
            assembly_id = aid
        else:
            byc["service_response"]["meta"]["warnings"].append("{} is not supported; fallback {} is being used!".format(aid, assembly_id))
            
    if not "filter_precision" in byc["form_data"]:
        byc["filter_flags"]["filter_precision"] = "start"
    for mk, mv in byc["service_config"]["meta"].items():
        byc["service_response"]["meta"].update({mk: mv})

    gene_id = rest_path_value("genespans")

    if gene_id is not None:
        byc["filter_flags"].update({"filter_precision": "exact"})
    elif "gene_id" in byc[ "form_data" ]:
        gene_id = byc[ "form_data" ]["gene_id"]
    else:
        response_add_error(byc, 422, "No geneId value provided!" )
    cgi_break_on_errors(byc)

    received_request_summary_add_custom_parameter(byc, "geneId", gene_id)

    results, e = retrieve_gene_id_coordinates(gene_id, byc)
    response_add_error(byc, 422, e )
    cgi_break_on_errors(byc)

    for gene in results:
        _gene_add_cytobands(gene, byc)

    e_k_s = byc["service_config"]["method_keys"]["genespan"]

    if "method" in byc:
        if "genespan" in byc["method"]:
            for i, g in enumerate(results):
                g_n = {}
                for k in byc["service_config"]["method_keys"]["genespan"]:
                    g_n.update({k: g.get(k, "")})
                results[i] = g_n

    if "text" in byc["output"]:
        open_text_streaming(byc["env"])
        for g in results:
            s_comps = []
            for k in e_k_s:
                s_comps.append(str(g.get(k, "")))
            print("\t".join(s_comps))
        exit()

    populate_service_response( byc, results)
    cgi_print_response( byc, 200 )

################################################################################

def _gene_add_cytobands(gene, byc):

    g_a = byc.get("genome_aliases", {})
    c_a = g_a.get("chro_aliases", {})

    gene.update({"cytobands": None})

    acc = gene.get("accession_version", "NA")
    if acc not in c_a:
        return gene

    start = gene.get("start", None)
    end = gene.get("end", None)
    if start is None or end is None:
        return gene

    chro = c_a.get( acc, "")
    chro_bases = "{}:{}-{}".format(chro, gene.get("start", ""), gene.get("end", ""))
    cytoBands, chro, start, end = bands_from_chrobases(chro_bases, byc)
    cb_label = cytobands_label( cytoBands )
    gene.update({"cytobands": "{}{}".format(chro, cb_label)})

    return gene

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
