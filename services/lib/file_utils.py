import csv, datetime, re, requests

from pathlib import Path
from os import environ, path
from pymongo import MongoClient
from copy import deepcopy
from random import sample as random_samples

from bycon import ByconVariant, prjsonnice, return_paginated_list

from datatable_utils import import_datatable_dict_line
from interval_utils import interval_cnv_arrays, interval_counts_from_callsets

################################################################################

def read_tsv_to_dictlist(filepath, max_count=0):
    dictlist = []
    with open(filepath, newline='') as csvfile:
    
        data = csv.DictReader(filter(lambda row: row.startswith('#') is False, csvfile), delimiter="\t", quotechar='"')
        fieldnames = list(data.fieldnames)

        for l in data:
            dictlist.append(dict(l))
            # prjsonnice(dict(l))

    if 0 < max_count < len(dictlist):
        dictlist = random_samples(dictlist, k=max_count)

    return dictlist, fieldnames


################################################################################

def read_www_tsv_to_dictlist(www, max_count=0):
    dictlist = []
    with requests.Session() as s:
        download = s.get(www)
        decoded_content = download.content.decode('utf-8')
        lines = list(decoded_content.splitlines())

        data = csv.DictReader(filter(lambda row: row.startswith('#') is False, lines), delimiter="\t", quotechar='"') # , quotechar='"'
        fieldnames = list(data.fieldnames)

        for l in data:
            dictlist.append(dict(l))

    if 0 < max_count < len(dictlist):
        dictlist = random_samples(dictlist, k=max_count)

    return dictlist, fieldnames


################################################################################

def callset_guess_probefile_path(callset, local_paths):
    if not local_paths:
        return False
    if not "server_callsets_dir_loc" in local_paths:
        return False
    if not "analysis_info" in callset:
        return False

    d = Path( path.join( *local_paths["server_callsets_dir_loc"]))
    n = local_paths.get("probefile_name", "___none___")

    if not d.is_dir():
        return False

    # TODO: not only geo cleaning?
    s_id = callset["analysis_info"].get("series_id", "___none___").replace("geo:", "")
    e_id = callset["analysis_info"].get("experiment_id", "___none___").replace("geo:", "")

    p_f = Path( path.join( d, s_id, e_id, n ) )

    if not p_f.is_file():
        return False

    return p_f

