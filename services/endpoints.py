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
        print_text_response(traceback.format_exc(), 302)
    
################################################################################

def endpoints():
    initialize_bycon_service(byc, "endpoints")
    run_beacon_init_stack(byc)
    # TODO: This needs some error fallback, test for existing entities etc.
    schema_name = rest_path_value("endpoints")
    prdbug(f'Schema name: {schema_name}')
    if schema_name:
        p = path.join( pkg_path, "schemas", "models", "src", "progenetix-model", schema_name, "endpoints.yaml")
        prdbug(f'Endpoint path: {p}')
    else:
        p = path.join( pkg_path, "schemas", "models", "src", "progenetix-model", "endpoints.yaml")

    e_p = load_yaml_empty_fallback(p)
    print('Content-Type: application/json')
    print('status:200')
    print()
    print(json.dumps(e_p, indent=4, sort_keys=True, default=str)+"\n")
    exit()
    
################################################################################
################################################################################

if __name__ == '__main__':
    main()
