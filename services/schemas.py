#!/usr/bin/env python3
import sys
from os import path
from humps import camelize

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from service_helpers import *
from service_response_generation import *

"""podmd

* <https://progenetix.org/services/schemas/biosample>

podmd"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        schemas()
    except Exception:
        print_text_response(traceback.format_exc(), 302)
    
################################################################################

def schemas():
    initialize_bycon_service(byc, "schemas")
    r = ByconautServiceResponse(byc)

    if "id" in BYC_PARS:
        schema_name = BYC_PARS.get("id", None)
    else:
        schema_name = byc["request_entity_path_id_value"]

    if schema_name:
        comps = schema_name.split('.')
        schema_name = comps.pop(0)
        prdbug(schema_name)
        s = read_schema_file(byc, schema_name, "")
        if s:
            print('Content-Type: application/json')
            print('status:200')
            print()
            print(json.dumps(camelize(s), indent=4, sort_keys=True, default=str)+"\n")
            exit()

    BYC["ERRORS"].append("No correct schema id provided!")
    BeaconErrorResponse(byc).response(422)


################################################################################
################################################################################

if __name__ == '__main__':
    main()
