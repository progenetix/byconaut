---
title: Housekeeping
---

Recurring "housekeeping" functions are provided by dedicated scripts with
eponymoys functionality located in the `housekeepers` directory (e.g. `deleteAnalyses.py`
is used for deleting records from teh `analyses` collection; `deleteBiosamplesWDS.py`
deletes analyses and their downtream records - _i.e._ the associated analyses and
the variants from those analyses). Additionally there is a separate `housekeeping.py`
app for executing a number of standard maintenance functions in sequential order.

Functions for importing and updating (for now) reside in the separate `importers` directory.

## General Options

Most housekeepers (and other) apps have some general options:

* `--testMode true` will run a test setting, e.g. for deletion apps only indicate
  the numbers to be deleted w/o actually remving records
    - most destructive apps will fall back to test mode by default and ask for confirmation
* `--limit 0` will perform the selected action on all records instad of a build-in
  default wherease e.g. `--limit 5` will just process a maximum of 5 records
* `--force true` will perform the selected action even if there have been warnings
  or errors written to the pre-processor log file; onme is usually prompted for this

## Creating Collations - `collationsCreator.py`

The `collationsCreator` script updates the dataset specific `collations` collections
which provide the aggregated data (sample numbers, hierarchy trees etc.) for all
individual codes belonging to one of the entities defined in the `filter_definitions`
in the `bycon` configuration. The (optional) hierarchy data is provided
in `rsrc/classificationTrees/__filterType__/numbered-hierarchies.tsv` as a list
of ordered branches in the format `code | label | depth | order`.

**TBD** The filter definition should be one of the configuration where users can
provide additions and overrides in the `byconaut/local` directory.

### Arguments

* `-d`, `--datasetIds` ... to select the dataset (only one per run)
* `--filters` ... to (optionally) limit the processing to a subset of samples
  (e.g. after a limited update)

### Use

* `bin/collationsCreator.py -d progenetix`
* `bin/collationsCreator.py -d examplez --collationTypes "PMID"`


## Pre-computing Binned CNV Frequencies - `frequencymapsCreator`

This app creates the frequency maps for the "collations" collection. Basically,
all samples matching any of the collation codes and representing CNV analyses
are selected and the frequencies of CNVs per genomic bin are aggregated. The
result contains teh gain and loss frquencies for all genomic intervals, for the
given entity.

### Arguments

* `-d`, `--datasetIds` ... to select the dataset (only one per run)
* `--collationTypes` ... to (optionally) limit the processing to a selected
  collation types (e.g. `NCIT`, `PMID`, `icdom` ...)

### Use

* `bin/frequencymapsCreator.py -d progenetix`
* `bin/frequencymapsCreator.py -d examplez --collationTypes "icdot"`

## Deleting Records

Records are deleted by providing a standard pgx-style tab-delimited metadata file
where only the corresponding `..._id` column is essential. As example, the 
`deleteIndividuals.py` app will take a table which includes a column `individual_id`
and use these values to delete the matching records.