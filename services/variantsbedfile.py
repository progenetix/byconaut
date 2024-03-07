#!/usr/bin/env python3
import sys
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from export_file_generation import write_variants_bedfile

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
    variantsbedfile()

################################################################################

def variantsbedfile():
    initialize_bycon_service(byc, "g_variants")
    rss = ByconResultSets(byc).datasetsResults()
    ds_id = list(rss.keys())[0]
    ucsclink, bedfilelink = write_variants_bedfile(rss, ds_id, byc)
    # TODO: Error
    if "ucsc" in BYC_PARS.get("output", "bed"):
        print_uri_rewrite_response(ucsclink, bedfilelink)
    print_uri_rewrite_response(bedfilelink)


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
