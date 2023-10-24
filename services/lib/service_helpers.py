import re

################################################################################

def set_selected_delivery_keys(method, method_keys, form_data):
    # the method keys can be overriden with "deliveryKeys"

    d_k = []

    if "delivery_keys" in form_data:
        d_k = re.split(",", form_data.get("delivery_keys", []))
        if len(d_k) > 0:
            return d_k

    if not method:
        return d_k

    if not method_keys:
        return d_k

    d_k = method_keys.get(method, [])

    return d_k


################################################################################

def response_add_error(byc, code=200, message=False):
    if message is False:
        return
    if len(str(message)) < 1:
        return

    e = {"error_code": code, "error_message": message}
    byc["error_response"].update({"error": e})


