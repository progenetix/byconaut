import re
from os import environ
from pymongo import MongoClient, GEOSPHERE

################################################################################

def mongodb_update_indexes(ds_id, byc):

    dt_m = byc["datatable_mappings"]
    b_rt_s = byc["service_config"]["indexed_response_types"]
    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    data_db = mongo_client[ds_id]
    coll_names = data_db.list_collection_names()
    for r_t, r_d in b_rt_s.items():

        collname = r_d.get("collection", False)
        if collname not in coll_names:
            print(f"¡¡¡ Collection {collname} does not exist in {ds_id} !!!")
            continue

        i_coll = data_db[ collname ]
        io_params = dt_m["entities"][ r_t ]["parameters"]

        for p_k, p_v in io_params.items():
            i_t = p_v.get("indexed", False)
            if i_t is False:
                continue
            k = p_v["db_key"]
            print('Creating index "{}" in {} from {}'.format(k, collname, ds_id))
            m = i_coll.create_index(k)
            print(m)

        if "geoprov_lat" in io_params.keys():
            k = re.sub("properties.latitude", "geometry", io_params["geoprov_lat"]["db_key"])
            m = i_coll.create_index([(k, GEOSPHERE)])
            print(m)

    #<------------------------ special collections --------------------------->#

    special_colls = byc["service_config"].get("indexed_special_collections", {})
    __index_by_colldef(ds_id, special_colls)

    #<------------------------- special databases ---------------------------->#

    for s_db, coll_defs in byc["service_config"].get("indexed_special_dbs", {}).items():
        __index_by_colldef(s_db, coll_defs)

################################################################################
            
def __index_by_colldef(ds_id, coll_defs):

    mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    i_db = mongo_client[ds_id]
    coll_names = i_db.list_collection_names()

    for collname, io_params in coll_defs.items():
        if collname not in coll_names:
            continue

        i_coll = i_db[ collname ]

        for p_k, p_v in io_params.items():
            k = p_v["db_key"]
            print(f'Creating index "{k}" in {collname} from {ds_id}')
            try:
                m = i_coll.create_index(k)
                print(m)
            except Exception:
                print(f'¡¡¡ Index "{k}" in {collname} from {ds_id} has one with same id !!!')





