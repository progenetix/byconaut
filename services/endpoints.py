#!/usr/bin/env python3
import re, json, yaml
from os import environ, pardir, path, scandir
import sys, datetime
from humps import camelize

from bycon import *

"""podmd
The service provides the schemas for the `BeaconMap` OpenAPI endpoints.
* <https://progenetix.org/services/endpoints/analyses>

podmd"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        endpoints()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def endpoints():

    initialize_bycon_service(byc)

    schema_name = rest_path_value("endpoints")
    if schema_name:
        p = path.join( pkg_path, "schemas", "models", "json", "progenetix-model", "endpoints.json")
    else:
        comps = schema_name.split('.')
        schema_name = comps.pop(0)
        p = path.join( pkg_path, "schemas", "models", "json", "progenetix-model", schema_name, "endpoints.json")

    root_def = RefDict(p)
    exclude_keys = [ "format", "examples" ]
    s = materialize(root_def, exclude_keys = exclude_keys)

    if s:

        print('Content-Type: application/json')
        print('status:200')
        print()
        print(json.dumps(camelize(s), indent=4, sort_keys=True, default=str)+"\n")
        exit()
    
################################################################################
################################################################################

if __name__ == '__main__':
    main()
