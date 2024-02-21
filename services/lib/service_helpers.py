import re
from humps import decamelize
from os import path
from pathlib import Path

from bycon import load_yaml_empty_fallback, BYC_PARS, ENV

################################################################################

def read_service_prefs(service, service_pref_path, byc):
    # snake_case paths; e.g. `intervalFrequencies` => `interval_frequencies.yaml`
    service = decamelize(service)
    f = Path( path.join( service_pref_path, service+".yaml" ) )
    if f.is_file():
        byc.update({"service_config": load_yaml_empty_fallback( f ) })


################################################################################

def set_selected_delivery_keys(method_keys):
    # the method keys can be overriden with "deliveryKeys"
    d_k = []
    delivery_method = BYC_PARS.get("method", "___none___")
    if "delivery_keys" in BYC_PARS:
        d_k = re.split(",", BYC_PARS.get("delivery_keys", []))
        if len(d_k) > 0:
            return d_k
    if not delivery_method:
        return d_k
    if not method_keys:
        return d_k
    d_k = method_keys.get(str(delivery_method), [])
    return d_k


################################################################################

def open_text_streaming(filename="data.pgxseg"):
    if not "local" in ENV:
        print('Content-Type: text/plain')
        if not "browser" in filename:
            print('Content-Disposition: attachment; filename="{}"'.format(filename))
        print('status: 200')
        print()


################################################################################

def close_text_streaming():
    print()
    exit()


################################################################################

def open_json_streaming(byc, filename="data.json"):
    meta = byc["service_response"].get("meta", {})

    if not "local" in ENV:
        print_json_download_header(filename)

    print('{"meta":', end='')
    print(json.dumps(camelize(meta), indent=None, sort_keys=True, default=str), end=",")
    print('"response":{', end='')
    for r_k, r_v in byc["service_response"].items():
        if "results" in r_k:
            continue
        if "meta" in r_k:
            continue
        print('"' + r_k + '":', end='')
        print(json.dumps(camelize(r_v), indent=None, sort_keys=True, default=str), end=",")
    print('"results":[', end="")


################################################################################

def print_json_download_header(filename):
    print('Content-Type: application/json')
    print(f'Content-Disposition: attachment; filename="{filename}"')
    print('status: 200')
    print()


################################################################################

def close_json_streaming():
    print(']}}')
    exit()
    


