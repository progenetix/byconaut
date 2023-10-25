#!/usr/bin/env python3

import sys
from os import path

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from service_response_generation import *

"""podmd

* <https://progenetix.org/services/dbstats/>

podmd"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        dbstats()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def dbstats():

    initialize_bycon_service(byc)
    select_dataset_ids(byc)

    r = ByconautServiceResponse(byc)
    byc.update({
        "service_response": r.emptyResponse(),
        "error_response": r.errorResponse()
    })

    info_db = byc[ "config" ][ "housekeeping_db" ]
    coll = byc[ "config" ][ "beacon_info_coll" ]
    stats = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))[ info_db ][ coll ].find( { }, { "_id": 0 } ).sort( "date", -1 ).limit( 1 )

    results = [ ]
    for stat in stats:
        prdbug(byc, stat)
        # byc["service_response"]["info"].update({ "date": stat["date"] })
        for ds_id, ds_vs in stat["datasets"].items():
            if len(byc[ "dataset_ids" ]) > 0:
                if not ds_id in byc[ "dataset_ids" ]:
                    continue
            dbs = { "dataset_id": ds_id }
            dbs.update({"counts":ds_vs["counts"]})
            results.append( dbs )

    byc.update({"service_response": r.populatedResponse(results)})
    cgi_print_response( byc, 200 )

################################################################################
################################################################################

if __name__ == '__main__':
    main()
