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

### `analysesStatusmapsRefresher` (CNV)

The `analysesStatusmapsRefresher` script creates CNV status data for binned
genomic intervals, for each CNV callset (_i.e._ the CNV data of all corresponding
variants from the same experiment/sample).


#### Examples

* `bin/analysesStatusmapsRefresher.py -d examplez`

### `collationsCreator`

**`collations`** provide aggregate data for all samples etc. matching a given
classification, external reference or other entity code, including hierarchy
data for term expansion when matching the code. The hierarchy data is provided
in `rsrc/classificationTrees/__filterType__/numbered-hierarchies.tsv` as a list
of ordered branches in the format `code | label | depth | order`.

#### Examples

* `bin/collationsCreator.py -d examplez --collationTypes "icdom,icdot"`
* `bin/collationsCreator.py -d progenetix`

### `frequencymapsCreator` (CNV)

**`frequencymaps`** contain pre-computed frequencies for CNV data, aggregating
the binned statusmaps data from all analyses belonging to a given collation.

#### Examples

* `bin/frequencymapsCreator.py -d examplez`

## Utility apps

### `ISCNsegmenter`

#### Examples

* `bin/ISCNsegmenter.py -i imports/ccghtest.tab -o exports/cghtest-with-histo.pgxseg`
