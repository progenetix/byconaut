[![License: CC0-1.0](https://img.shields.io/badge/License-CC0%201.0-lightgrey.svg)](http://creativecommons.org/publicdomain/zero/1.0/)

# `byconaut`

The `byconaut` package contains scripts for data processing for and based on the
`bycon` package. The main use cases are:

* generation of utility collections for the standard Progenetix data model
    - `collations`
    - `frequencymaps` provide binned CNV frequency values for samples belonging
      to a given collation code
* I/O & transformations for `bycon` generated files

## Scripts

## collationsCreator

**Collations** provide aggregate data for all samples etc. matching a given
classification, external reference or other entity code, including hierarchy
data for term expansion when matching the code. The hierarchy data is provided
in `rsrc/classificationTrees/__filterType__/numbered-hierarchies.tsv` as a list
of ordered branches in the format `code | label | depth | order`.

#### Examples

* `bin/collationsCreator.py -d examplez --collationtypes "icdom,icdot"`
* `bin/collationsCreator.py -d progenetix`

### ISCNsegmenter

#### Examples

* `bin/ISCNsegmenter.py -i imports/ccghtest.tab -o exports/cghtest-with-histo.pgxseg`