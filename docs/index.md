# `byconaut`

!!! warning "Deprecation of `byconaut` dependency for `bycon` Installations"

    Since the **bycon v2.0 "Taito City"** release, the `byconaut` project has been
    reduced to non-standard functionality. Importantly, "beyond Beacon services",
    installation support, example data and data import functions have been migrated into
    the `bycon` project itself. The `byconaut` project now mainly serves as a playground
    for temporary utilities and scripts making use of `bycon` functions for additional
    tasks.

## Installation

`byconaut` depends on the `bycon` package which can be downloaded from its
[repository](http://github.com/progenetix/bycon/). Please see the repository
and the corresponding [documentation site](http://bycon.progenetix.org).

While there is also a `pip` installation possible over `pip3 install bycon`
this will _not_ include the local configuration files necessary e.g. for
processing the databases.

## Create your own databases

### Core Data

A basic setup for a Beacon compatible database - as supported by the `bycon` package -
consists of the core data collections mirroring the Beacon default data model:

* `variants`
* `analyses` (which covers parameters from both Beacon `analysis` and `run` entity schemas)
* `biosamples`
* `individuals`

Databases are implemented in an existing MongoDB setup using utility applications
contained in the `importers` directory by importing data from tab-delimited data
files. In principle, only 2 import files are needed for inserting and updating of records:
* a file for the non-variant metadata[^1] with specific header values, where as
  the absolute minimum id values for the different entities have to be provided
* a file for genomic variants, again with specific headers but also containing
  the upstream ids for the corresponding analysis, biosample and individual

#### Examples

##### Minimal metadata file

```
individual_id   biosample_id    analysis_id
BRCA-patient-001 brca-001  brca-001-cnv
BRCA-patient-001 brca-001  brca-001-snv
BRCA-patient-002 brca-002  brca-002-cnv
```
##### Variant file

```

```


## Further and optional procedures

1. Create database and variants collection
2. update the local `bycon` installation for your database information andlocal parameters
    * database name(s)
    * `filter_definitions` for parameter mapping
3. Create metadata collections - `analyses`, `biosamples` and `individuals`
4. Create `statusmaps` and CNV statistics for the analyses collection
    * only relevant for CNV database use cases
5. Create the `collations` collection which uses `filter_definitions` and the
   corresponding values to aggregate information for query matching, term expansion ...
6. Create `frequencymaps` for binned CNV data
    * relies on existence of `statusmaps` in `analyses` and `collations`
    * only needed for CNV data

## Data maintenance scripts

Please see the [helper apps documentation](applications/#data-transformation-database-maintenance).



[^1]: Metadata in biomedical genomics is "everything but the sequence variation"

