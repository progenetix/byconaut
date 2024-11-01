---
title: Helper Applications
---

The `byconaut` repository provides a number of helper applications with different
types of functionalities, e.g.

* data I/O
* plotting (see [plotting](plotting.md))
* database maintenance
* data transformation

These applications are in some way used to populate or manage data resources for
`bycon` driven implementations of the Beacon protokol (_i.e._ genomic data resources).


## Plotting Apps

For more information see the dedicated [documentation page](plotting.md)).

## Data transformation & database maintenance

### `analysesStatusmapsRefresher`

This is one of the housekeeping scripts which has to be run after CNV data has
been added or modified in the database. It creates CNV status data for binned
genome intervals, used for histogram generation, sample clustering etc.,
as well as some other statistics (e.g. CNV coverage per chromosomal arms ...).

#### Arguments

* `-d`, `--datasetIds` ... to select the dataset (only one per run)
* `--filters` ... to (optionally) limit the processing to a subset of samples
  (e.g. after a limited update)

#### Use

* `bin/analysesStatusmapsRefresher.py -d progenetix`
* `bin/analysesStatusmapsRefresher.py -d progenetix --filters "pgx:icdom-81703"`
* `bin/analysesStatusmapsRefresher.py -d cellz --filters "cellosaurus:CVCL_0312"`

-------------------------------------------------------------------------------

## Utility apps

### `ISCNsegmenter`

This is a helper app to transform cytogenetic CGH annotations (rev ish) to the
canonical tab-delimited `.pgxseg` segment file format.

#### Use

* `bin/ISCNsegmenter.py -i imports/ccghtest.tab -o exports/cghtest-with-histo.pgxseg`

