#!/usr/bin/env python3
import sys
from os import path

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from datatable_utils import export_datatable_download

"""
The service uses the standard bycon data retrieval pipeline with `biosample`
as entity type. Therefore, all standard Beacon query parameters work and also
the path is interpreted for an biosample `id` value if there is an entry at
`.../sampletable/{id}`

The table type can be changed with `tableType=individuals` (or `analyses`.

* http://progenetix.org/services/sampletable/pgxbs-kftvjv8w
* http://progenetix.org/services/sampletable/pgxbs-kftvjv8w?tableType=individuals&datasetIds=cellz
* http://progenetix.org/services/sampletable?datasetIds=cellz&filters=cellosaurus:CVCL_0030
* http://progenetix.org/services/sampletable?filters=pgx:icdom-81703

"""

################################################################################
################################################################################
################################################################################

def main():

    sampletable()

################################################################################

def sampletable():
    BYC.update({
        "request_entity_path_id": "biosamples",
        "request_entity_id": "biosample"
    })
    initialize_bycon_service()

    if not "table" in BYC_PARS.get("output", "___none___"):
        BYC_PARS.update({"output":"table"})

    if (table_type := BYC.get("response_entity_id", "___none___")) not in ["biosample", "individual", "analysis"]:
        table_type = "biosample"
        BYC.update({"response_entity_id": table_type})
    rsd = ByconResultSets().datasetsData()

    collated_results = []
    for ds_id, data in rsd.items():
        collated_results += data

    export_datatable_download(collated_results)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
