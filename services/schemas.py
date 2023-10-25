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
        print_text_response(traceback.format_exc(), byc["env"], 302)
    
################################################################################

def schemas():
   
    initialize_bycon_service(byc, "schemas")
    r = ByconautServiceResponse(byc)
    byc.update({
        "service_response": r.emptyResponse(),
        "error_response": r.errorResponse()
    })

    if "id" in byc["form_data"]:
        schema_name = byc["form_data"].get("id", None)
    else:
        schema_name = byc["request_entity_path_id_value"]

    if schema_name:

        comps = schema_name.split('.')
        schema_name = comps.pop(0)

        prdbug(byc, schema_name)

        s = read_schema_file(byc, schema_name, "")
        if s is not False:

            print('Content-Type: application/json')
            print('status:200')
            print()
            print(json.dumps(camelize(s), indent=4, sort_keys=True, default=str)+"\n")
            exit()
    
    response_add_error(byc, 422, "No correct schema id provided!")
    cgi_print_response( byc, 422 )

################################################################################
################################################################################

if __name__ == '__main__':
    main()
