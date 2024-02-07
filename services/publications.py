#!/usr/bin/env python3
import re, sys
from os import environ, path, pardir
from pymongo import MongoClient
from operator import itemgetter

from bycon import *

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
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def publications():

    initialize_bycon_service(byc, "publications")
    run_beacon_init_stack(byc)

    r = ByconautServiceResponse(byc)
    byc.update({
        "service_response": r.emptyResponse(),
        "error_response": r.errorResponse()
    })
    form = byc.get("form_data", {})

    # data retrieval & response population
    query, e = _create_filters_query( byc )
    geo_q, geo_pars = geo_query( byc )

    if geo_q:
        # for g_k, g_v in geo_pars.items():
        #     received_request_summary_add_custom_parameter(byc, g_k, g_v)
        if len(query.keys()) < 1:
            query = geo_q
        else:
            query = { '$and': [ geo_q, query ] }

    if len(query.keys()) < 1:
        e_m = "No query could be constructed from the parameters provided."
        e_r = BeaconErrorResponse(byc).error(e_m, 422)
        print_json_response(e_r, byc["env"])

    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    pub_coll = mongo_client[ "progenetix" ][ "publications" ]
    p_re = re.compile( byc["filter_definitions"]["PMID"]["pattern"] )
    d_k = set_selected_delivery_keys(byc["service_config"].get("method_keys"), form)
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
    print_json_response(r.populatedResponse(results), byc["env"])


################################################################################
################################################################################

def __check_publications_map_response(byc, results):

    if not "map" in byc["output"]:
        return

    u_locs = {}

    for p in results:
        if not "counts" in p:
            pass

        geoloc = p["provenance"].get("geo_location", None)
        if geoloc is None:
            pass

        l_k = "{}::{}".format(geoloc["geometry"]["coordinates"][1], geoloc["geometry"]["coordinates"][0])

        if not l_k in u_locs.keys():
            u_locs.update({l_k:{"geo_location": geoloc}})
            u_locs[l_k]["geo_location"]["properties"].update({"items":[]})

        m_c = p["counts"].get("genomes", 0)
        m_s = u_locs[l_k]["geo_location"]["properties"].get("marker_count", 0) + m_c
        # print(m_c, m_s)

        i = "<a href='/publication/?id={}'>{}</a> ({})".format(p["id"], p["id"], m_c)
        u_locs[l_k]["geo_location"]["properties"].update({"marker_count":m_s})
        u_locs[l_k]["geo_location"]["properties"]["items"].append(i)

    geolocs =  u_locs.values()

    print_map_from_geolocations(byc, geolocs)

################################################################################

def _create_filters_query( byc ):

    query = { }
    error = ""
    f_d_s = byc[ "filter_definitions" ]

    if byc.get("test_mode", False) is True:
        test_mode_count = int(byc.get('test_mode_count', 5))
        mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
        data_coll = mongo_client[ "progenetix" ][ "publications" ]

        rs = list(data_coll.aggregate([{"$sample": {"size": test_mode_count}}]))
        query = {"_id": {"$in": list(s["_id"] for s in rs)}}
        return query, error

    q_list = [ ]
    count_pat = re.compile( r'^(\w+?)\:([>=<])(\d+?)$' )

    # TODO: This doesn't apply any more?

    for f in byc[ "filters" ]:
        f_val = f["id"]
        if len(f_val) < 1:
            continue
        pre_code = re.split('-|:', f_val)
        pre = pre_code[0]
        if str(pre) not in f_d_s.keys():
            continue

        dbk = byc[ "filter_definitions" ][ pre ][ "db_key" ]

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
                error = "uncaught filter error: {}".format(f_val)
                continue
            q_list.append( { dbk: { op: int(no) } } )
        elif "start" in byc[ "filter_flags" ][ "precision" ] or len(pre_code) == 1:
            """podmd
            If there was only prefix a regex match is enforced - basically here
            for the selection of PMID labeled publications.
            podmd"""
            q_list.append( { "id": re.compile(r'^'+f_val ) } )
        elif pre in f_d_s.keys():
            # TODO: hacky method for pgxuse => redo
            q_v = f_val
            try:
                if f_d_s[ pre ][ "remove_prefix" ] is True:
                    q_v = pre_code[1]
            except:
                pass
            q_list.append( { dbk: q_v } )
        else:
            q_list.append( { "id": f_val } )

    if len(q_list) > 1:
        query = { '$and': q_list }
    elif len(q_list) < 1:
        query = {}
    else:
        query = q_list[0]

    return query, error

################################################################################
################################################################################

if __name__ == '__main__':
    main()
