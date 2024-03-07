#!/usr/bin/env python3
import sys
from os import path

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from export_file_generation import export_vcf_download

"""
The plot service uses the standard bycon data retrieval pipeline with `biosample`
as entity type. Therefore, all standard Beacon query parameters work and also
the path is interpreted for an biosample `id` value if there is an entry at
`.../pgxsegvariants/{id}`

* http://progenetix.org/services/vcfvariants/pgxbs-kftvjv8w

"""

################################################################################

def main():
    try:
        vcfvariants()
    except Exception:
        print_text_response(traceback.format_exc(), 302)


################################################################################

def vcfvariants():
    initialize_bycon_service(byc, "biosamples")

    if not "vcf" in BYC_PARS.get("output", "___none___"):
        BYC_PARS.update({"output":"vcf"})

    rss = ByconResultSets(byc).datasetsResults()

    # Note: only the first dataset will be exported ...
    ds_id = list(rss.keys())[0]
    export_vcf_download(rss, ds_id, byc)


################################################################################

if __name__ == '__main__':
    main()
