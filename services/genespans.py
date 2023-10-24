#!/usr/bin/env python3

import re, json
from os import environ

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from service_helpers import *
from service_response_generation import *

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
    """

    """
    initialize_bycon_service(byc, sys._getframe().f_code.co_name)
    run_beacon_init_stack(byc)

    # form id assumes start match (e.g. for autocompletes)
    byc[ "config" ][ "filter_flags" ].update({"precision": "start"})

    r = ByconautServiceResponse(byc)
    byc.update({
        "service_response": r.emptyResponse(),
        "error_response": r.errorResponse()
    })

    gene_id = rest_path_value("genespans")
    if gene_id:
        # REST path id assumes exact match
        byc[ "config" ][ "filter_flags" ].update({"precision": "exact"})
    else:
        gene_id = byc[ "form_data" ].get("gene_id")

    if not gene_id:
        response_add_error(byc, 422, "No geneId value provided!" )
        cgi_break_on_errors(byc)

    # redoing this since possibly modified in the geneId retrieval above
    get_global_filter_flags(byc)
    # received_request_summary_add_custom_parameter(byc, "geneId", gene_id)

    results, e = retrieve_gene_id_coordinates(gene_id, byc["filter_flags"].get("precision", "start"), byc)
    response_add_error(byc, 422, e )
    cgi_break_on_errors(byc)

    for gene in results:
        _gene_add_cytobands(gene, byc)

    e_k_s = byc["service_config"]["method_keys"]["genespan"]

    if "genespan" in byc.get("method", "___none___"):
        for i, g in enumerate(results):
            g_n = {}
            for k in byc["service_config"]["method_keys"]["genespan"]:
                g_n.update({k: g.get(k, "")})
            results[i] = g_n

    if "text" in byc.get("output", "___none___"):
        open_text_streaming(byc["env"])
        for g in results:
            s_comps = []
            for k in e_k_s:
                s_comps.append(str(g.get(k, "")))
            print("\t".join(s_comps))
        exit()

    byc.update({"service_response": r.populatedResponse(results)})
    cgi_print_response( byc, 200 )

################################################################################

def _gene_add_cytobands(gene, byc):

    g_a = byc.get("genome_aliases", {})
    c_a = g_a.get("chro_aliases", {})

    gene.update({"cytobands": None})

    acc = gene.get("accession_version", "NA")
    if acc not in c_a:
        return gene

    chro = c_a.get( acc, "")
    start = gene.get("start")
    end = gene.get("end")
    if not start or not end:
        return gene

    gene.update({"cytobands": f'{chro}{cytobands_label_from_positions(byc, chro, start, end)}'})

    return gene

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
