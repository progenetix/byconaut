#!/usr/bin/env python3
import sys
from os import path

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), "lib" )
sys.path.append( services_lib_path )
from cytoband_utils import *
from service_helpers import *
from service_response_generation import *

"""
cytomapper.py --cytoBands 8q21 
cytomapper.py --chroBases 4:12000000-145000000
"""

################################################################################
################################################################################
################################################################################

def main():

    try:
        cytomapper()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)

################################################################################
################################################################################
################################################################################

def cytomapper():
    
    initialize_bycon_service(byc, sys._getframe().f_code.co_name)
    run_beacon_init_stack(byc)
    parse_cytoband_file(byc)

    results = __return_cytobands_results(byc)

    r = ByconautServiceResponse(byc)
    response = r.populatedResponse(results)

    if len( results ) < 1:
        e_m = "No matching cytobands!"
        e_r = BeaconErrorResponse(byc).error(e_m, 422)
        print_json_response(e_r, byc["env"])
    if "cyto_bands" in byc["varguments"]:
        response["meta"]["received_request_summary"].update({ "cytoBands": byc["varguments"]["cyto_bands"] })
    elif "chro_bases" in byc["varguments"]:
        response["meta"]["received_request_summary"].update({ "chroBases": byc["varguments"]["chro_bases"] })

    print_json_response(response, byc["env"])


################################################################################

def __return_cytobands_results(byc):

    a_d = byc.get("argument_definitions", {})
    c_b_d = byc.get("cytobands", [])
    chro_names = ChroNames(byc)

    cytoBands = [ ]
    if "cyto_bands" in byc["varguments"]:
        cytoBands, chro, start, end, error = bands_from_cytobands(byc["varguments"]["cyto_bands"], c_b_d, a_d)
    elif "chro_bases" in byc["varguments"]:
        cytoBands, chro, start, end = bands_from_chrobases(byc["varguments"]["chro_bases"], byc)

    if len( cytoBands ) < 1:
        return ()

    cb_label = cytobands_label( cytoBands )

    size = int(  end - start )
    chroBases = "{}:{}-{}".format(chro, start, end)
    sequence_id = chro_names.refseq(chro)

    if "text" in byc["output"]:
        open_text_streaming(byc["env"])
        print("{}\t{}".format(cb_label, chroBases))
        exit()

    # TODO: response objects from schema
    # r_s = byc["response_entity"]["beacon_schema"]["entity_type"]
    # cb_i = object_instance_from_schema_name(byc, r_s, "")
    
    results = [
        {
            "info": {
                "cytoBands": cb_label,
                "bandList": [x['chroband'] for x in cytoBands ],
                "chroBases": chroBases,
                "referenceName": chro,
                "size": size,
            },        
            "chromosome_location": {
                "type": "ChromosomeLocation",
                "species_id": "taxonomy:9606",
                "chr": chro,
                "interval": {
                    "start": cytoBands[0]["cytoband"],
                    "end": cytoBands[-1]["cytoband"],
                    "type": "CytobandInterval"
                }
            },
            "genomic_location": {
                "type": "SequenceLocation",
                "sequence_id": sequence_id,
                "interval": {
                    "start": {
                        "type": "Number",
                        "value": start
                    },
                    "end": {
                        "type": "Number",
                        "value": end
                    },
                    "type": "SequenceInterval"
                }
            }
        }
    ]

    return results


################################################################################
################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
