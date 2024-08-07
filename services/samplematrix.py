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
    initialize_bycon_service()
    BYC.update({"response_entity_id": "biosample"})
    generate_genome_bins()
    if not "pgxmatrix" in BYC_PARS.get("output", "___none___"):
        BYC_PARS.update({"output":"pgxmatrix"})

    rss = ByconResultSets().datasetsResults()

    # Note: only the first dataset will be exported ...
    ds_id = list(rss.keys())[0]
    export_callsets_matrix(rss, ds_id)


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
