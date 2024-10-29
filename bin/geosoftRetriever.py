#!/usr/bin/env python3

import re, json, requests, yaml
from copy import deepcopy
from progress.bar import Bar
import sys, datetime

from bycon import *
from byconServiceLibs import file_utils

loc_path = path.dirname( path.abspath(__file__) )
log_path = path.join( loc_path, pardir, "logs" )
"""

"""

################################################################################
################################################################################
################################################################################

def main():
    geosoft_retriever()

################################################################################

def geosoft_retriever():
    initialize_bycon_service()
    if len(BYC["BYC_DATASET_IDS"]) != 1:
        print("No single existing dataset was provided with -d ...")
        exit()
    ds_id = BYC["BYC_DATASET_IDS"][0]
    geo_url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?targ=self&view=brief&form=text&acc="
    log = []

    #----------------------- Read geo ids from database -----------------------#

    mongo_client = MongoClient(host=DB_MONGOHOST)    
    ana_coll = mongo_client[ ds_id ][ "analyses" ]
    geo_gsms = ana_coll.distinct("analysis_info.experiment_id", {"analysis_info.experiment_id":{"$regex":"geo"}})
    data_no = len(geo_gsms)

    bar = Bar("Retrieving ", max = data_no, suffix='%(percent)d%%'+" of "+str(data_no) ) if not BYC["TEST_MODE"] else False
    up_no = 0

    nm = re.compile( r'!Sample_title = (.+?)$' )

    # TODO: More extraction; currently just legacy ID retrieval

    for gsm in geo_gsms:
        bar.next()
        url = geo_url+gsm.replace("geo:", "")
        # print(f'\n{url}')
        if not (ana := ana_coll.find_one({"analysis_info.experiment_id": gsm})):
            log.append(f'{gsm}\tnot found again in analyses')
            continue
        # existing ones are skipped
        if len(ana["analysis_info"].get("experiment_title", "")) > 0:
            continue
        r =  requests.get(f'{url}')
        if r.ok:
            # print(r.content)
            fc = str(r.text)
            for line in fc.splitlines():
                line = str(line)
                # print(f'...{line}')
                if nm.match(line):
                    name = nm.match(line).group(1)
                    if len(name) > 0:
                        ana_coll.update_one({"_id": ana["_id"]}, {"$set":{"analysis_info.experiment_title": name}})
                        up_no +=1
                    else:
                        log.append(f'{gsm}\tno name extracted')

        else:
            log.append(f'{gsm}\terror')

    #----------------------------- Summary ------------------------------------#

    if not BYC["TEST_MODE"]:
        bar.finish()
        print(f'==> updated {up_no} analyses')

    file_utils.write_log(log, path.join( log_path, "geosoft_retriever_gsm" ))


################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
