#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir

from bycon import *

"""
The plot service uses the standard bycon data retrieval pipeline with `biosample`
as entity type. Therefore, all standard Beacon query parameters work and also
the path is interpreted for an biosample `id` value if there is an entry at
`.../sampleplots/{id}`

The plot type can be set with `plotType=samplesplot` (or `histoplot` but that is
the fallback). Plot options are available as usual.

* http://progenetix.org/services/sampleplots/pgxbs-kftvjv8w
* http://progenetix.org/services/sampleplots/pgxbs-kftvjv8w?plotType=samplesplot&datasetIds=cellz
* http://progenetix.org/services/sampleplots?plotType=samplesplot&datasetIds=cellz&filters=cellosaurus:CVCL_0030
* http://progenetix.org/services/sampleplots?filters=pgx:icdom-81703

"""

################################################################################
################################################################################
################################################################################

def main():

    sampleplots()

################################################################################

def sampleplots():

    initialize_bycon_service(byc, "biosamples")
    run_beacon_init_stack(byc)
    generate_genomic_mappings(byc)

    plot_type = byc["form_data"].get("plot_type", "histoplot")

    if plot_type not in ["histoplot", "samplesplot"]:
        plot_type = "histoplot"
    byc.update({"output": plot_type})
    RSS = ByconResultSets(byc)
    RSS.plotSVGtoWeb(plot_type)

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
