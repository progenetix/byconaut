import re

################################################################################

def set_selected_delivery_keys(method_keys, form_data):
    # the method keys can be overriden with "deliveryKeys"

    d_k = []
    delivery_method = form_data.get("method", "___none___")

    if "delivery_keys" in form_data:
        d_k = re.split(",", form_data.get("delivery_keys", []))
        if len(d_k) > 0:
            return d_k

    if not delivery_method:
        return d_k
    if not method_keys:
        return d_k

    d_k = method_keys.get(str(delivery_method), [])

    return d_k


################################################################################

def response_add_error(byc, code=200, message=None):
    if not message:
        return
    if len(str(message)) < 1:
        return

    e = {"error_code": code, "error_message": message}
    byc["error_response"].update({"error": e})


################################################################################

def open_text_streaming(env="server", filename="data.pgxseg"):
    if not "local" in env:
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

    if not "local" in byc["env"]:
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
    


