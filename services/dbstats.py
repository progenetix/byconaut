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
    run_beacon_init_stack(byc)
    r = ByconautServiceResponse(byc)

    mdb_c = byc.get("db_config", {})
    db_host = mdb_c.get("host", "localhost")
    info_db = mdb_c.get("housekeeping_db")
    i_coll = mdb_c.get("beacon_info_coll")

    stats = MongoClient(host=db_host)[ info_db ][ i_coll ].find( { }, { "_id": 0 } ).sort( "date", -1 ).limit( 1 )

    results = [ ]
    for stat in stats:
        for ds_id, ds_vs in stat["datasets"].items():
            if len(byc[ "dataset_ids" ]) > 0:
                if not ds_id in byc[ "dataset_ids" ]:
                    continue
            dbs = { "dataset_id": ds_id }
            dbs.update({"counts":ds_vs["counts"]})
            results.append( dbs )

    print_json_response(r.populatedResponse(results), byc["env"])


################################################################################
################################################################################

if __name__ == '__main__':
    main()
