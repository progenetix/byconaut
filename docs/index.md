# `byconaut`

The `byconaut` package contains scripts for data processing for and based on the
`bycon` package. The main use cases are:

* generation of utility collections for the standard Progenetix data model
    - `collations`
    - `frequencymaps` provide binned CNV frequency values for samples belonging
      to a given collation code
* I/O & transformations for `bycon` generated files

## Installation

`byconaut` depends on the `bycon` package which can be downloaded from its
[repository](http://github.com/progenetix/bycon/). Please see the repository
and the corresponding [documentation site](http://bycon.progenetix.org).

While there is also a `pip` installation possible over `pip3 install bycon`
this will _not_ include the local configuration files necessary e.g. for
processing the databases.

## Database setup

### Option A: `examplez` from <rsrc/mongodump>

1. download <rsrc/mongodump/examplez.zip>
2. unpack somewhere & restore with (your paths etc.):
```
mongosh examplez --eval 'db.dropDatabase()'
mongorestore --db $database .../mongodump/examplez/
```
3. proceed w/ step 4 ... below

### Option B: Create your own databases

#### Core Data

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

Examples:

```
individual_id   biosample_id    analysis_id
pgxind-kftx25eh pgxbs-kftva59y  pgxcs-kftvldsu
```

#### Further and optional procedures

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

