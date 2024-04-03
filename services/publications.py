#!/usr/bin/env python3
import re, sys
from os import environ, path, pardir
from pymongo import MongoClient
from operator import itemgetter

from bycon import *

services_conf_path = path.join( path.dirname( path.abspath(__file__) ), "config" )
services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from geomap_utils import *
from service_helpers import *
from service_response_generation import *

"""podmd

podmd"""

################################################################################
################################################################################
################################################################################

def main():
    try:
        publications()
    except Exception:
        print_text_response(traceback.format_exc(), 302)
    
################################################################################

def publications():
    initialize_bycon_service(byc, "publications")
    read_service_prefs("publications", services_conf_path, byc)

    r = ByconautServiceResponse(byc)
    # data retrieval & response population
    query = _create_filters_query(byc)
    geo_q, geo_pars = geo_query(byc["geoloc_definitions"])

    if geo_q:
        # for g_k, g_v in geo_pars.items():
        #     received_request_summary_add_custom_parameter(byc, g_k, g_v)
        if len(query.keys()) < 1:
            query = geo_q
        else:
            query = { '$and': [ geo_q, query ] }

    if len(query.keys()) < 1:
        BYC["ERRORS"].append("No query could be constructed from the parameters provided.")
        BeaconErrorResponse(byc).response(422)

    mongo_client = MongoClient(host=DB_MONGOHOST)
    pub_coll = mongo_client[ "progenetix" ][ "publications" ]
    p_re = re.compile( byc["filter_definitions"]["pubmed"]["pattern"] )
    d_k = set_selected_delivery_keys(byc["service_config"].get("method_keys"))
    p_l = [ ]
    
    for pub in pub_coll.find( query, { "_id": 0 } ):
        s = { }
        if len(d_k) < 1:
            s = pub
        else:
            for k in d_k:
                # TODO: harmless but ugly hack
                if k in pub.keys():
                    if k == "counts":
                        s[ k ] = { }
                        for c in pub[ k ]:
                            if pub[ k ][ c ]:
                                try:
                                    s[ k ][ c ] = int(float(pub[ k ][ c ]))
                                except:
                                    s[ k ][ c ] = 0
                            else:
                                s[ k ][ c ] = 0
                    else:
                        s[ k ] = pub[ k ]
                else:
                    s[ k ] = None
        try:
            s_v = p_re.match(s[ "id" ]).group(3)
            s[ "sortid" ] = int(s_v)
        except:
            s[ "sortid" ] = -1

        p_l.append( s )

    mongo_client.close( )
    results = sorted(p_l, key=itemgetter('sortid'), reverse = True)
    __check_publications_map_response(byc, results)
    print_json_response(r.populatedResponse(results))


################################################################################
################################################################################

def __check_publications_map_response(byc, results):
    output = BYC_PARS.get("output", "___none___")
    if not "map" in output:
        return

    u_locs = {}
    for p in results:
        counts = p.get("counts", {})
        geoloc = p["provenance"].get("geo_location", None)
        if geoloc is None:
            pass
        l_k = "{}::{}".format(geoloc["geometry"]["coordinates"][1], geoloc["geometry"]["coordinates"][0])

        if not l_k in u_locs.keys():
            u_locs.update({l_k:{"geo_location": geoloc}})
            u_locs[l_k]["geo_location"]["properties"].update({"items":[]})

        m_c = counts.get("genomes", 0)
        m_s = u_locs[l_k]["geo_location"]["properties"].get("marker_count", 0) + m_c

        link = f'<a href="/publication/?id={p["id"]}">{p["id"]}</a> ({m_c})'
        u_locs[l_k]["geo_location"]["properties"].update({"marker_count":m_s})
        u_locs[l_k]["geo_location"]["properties"]["items"].append(link)
    geolocs =  u_locs.values()

    print_map_from_geolocations(byc, geolocs)

################################################################################

def _create_filters_query(byc):
    filters = byc.get("filters", [])
    filter_precision = BYC_PARS.get("filter_precision", "exact")
    f_d_s = byc.get("filter_definitions", {})
    query = { }
    error = ""

    if BYC["TEST_MODE"] is True:
        test_mode_count = int(BYC_PARS.get('test_mode_count', 5))
        mongo_client = MongoClient(host=DB_MONGOHOST)
        data_coll = mongo_client[ "progenetix" ][ "publications" ]

        rs = list(data_coll.aggregate([{"$sample": {"size": test_mode_count}}]))
        query = {"_id": {"$in": list(s["_id"] for s in rs)}}
        return query, error

    q_list = [ ]
    count_pat = re.compile( r'^(\w+?)\:([>=<])(\d+?)$' )

    fds_pres = list(f_d_s.keys())

    for f in filters:
        f_val = f["id"]
        prdbug(f_val)
        if len(f_val) < 1:
            continue
        pre_code = re.split('-|:', f_val)
        pre = pre_code[0]
        prk = pre
        if "PMID" in pre:
           prk = "pubmed" 

        if str(prk) not in f_d_s.keys():
            continue

        dbk = f_d_s[ prk ]["db_key"]
        if count_pat.match( f_val ):
            pre, op, no = count_pat.match(f_val).group(1,2,3)
            dbk = f_d_s[ pre ][ "db_key" ]
            if op == ">":
                op = '$gt'
            elif op == "<":
                op = '$lt'
            elif op == "=":
                op = '$eq'
            else:
                BYC["ERRORS"].append(f'uncaught filter error: {f_val}')
                continue
            q_list.append( { dbk: { op: int(no) } } )
        elif "start" in filter_precision or len(pre_code) == 1:
            """podmd
            If there was only prefix a regex match is enforced - basically here
            for the selection of PMID labeled publications.
            podmd"""
            q_list.append( { "id": re.compile(r'^'+f_val ) } )
        else:
            q_list.append( { "id": f_val } )

    if len(q_list) > 1:
        query = { '$and': q_list }
    elif len(q_list) < 1:
        query = {}
    else:
        query = q_list[0]

    return query

################################################################################
################################################################################

if __name__ == '__main__':
    main()
