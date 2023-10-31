#!/usr/bin/env python3
import sys
from os import path, environ, pardir

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from export_file_generation import export_callsets_matrix
from interval_utils import generate_genome_bins

"""
The service uses the standard bycon data retrieval pipeline with `analysis`
as entity type. Therefore, all standard Beacon query parameters work and also
the path is interpreted for an biosample `id` value if there is an entry at
`.../analyses/{id}`
"""

################################################################################
################################################################################
################################################################################

def main():

    samplematrix()

################################################################################

def samplematrix():

    initialize_bycon_service(byc, "biosamples")
    run_beacon_init_stack(byc)
    generate_genome_bins(byc)

    if not "pgxmatrix" in byc.get("output", "___none___"):
        byc.update({"output":"pgxmatrix"})

    rss = ByconResultSets(byc).datasetsResults()

    # Note: only the first dataset will be exported ...
    ds_id = list(rss.keys())[0]
    export_callsets_matrix(rss, ds_id, byc)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
