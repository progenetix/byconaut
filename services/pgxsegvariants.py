#!/usr/bin/env python3
import sys
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from export_file_generation import export_pgxseg_download

"""
The plot service uses the standard bycon data retrieval pipeline with `biosample`
as entity type. Therefore, all standard Beacon query parameters work and also
the path is interpreted for an biosample `id` value if there is an entry at
`.../pgxsegvariants/{id}`

* http://progenetix.org/services/pgxsegvariants/pgxbs-kftvjv8w

"""

################################################################################
################################################################################
################################################################################

def main():

    pgxsegvariants()

################################################################################

def pgxsegvariants():
    initialize_bycon_service()
    BYC.update({"response_entity_id": "genomicVariant"})
    rss = ByconResultSets().datasetsResults()
    # TODO: multi-dataset?
    ds_id = list(rss.keys())[0]
    export_pgxseg_download(rss, ds_id)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
