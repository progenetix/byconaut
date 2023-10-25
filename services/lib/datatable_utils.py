import csv, re, requests
# from attrdictionary import AttrDict
from random import sample as randomSamples

# bycon
from cgi_parsing import prdbug, prjsonnice
from bycon_helpers import assign_nested_value, get_nested_value

################################################################################

def export_datatable_download(results, byc):

    # TODO: separate table generation from HTTP response

    if not "table" in byc["output"]:
        return
    if not "datatable_mappings" in byc:
        return

    dt_m = byc["datatable_mappings"]
    r_t = byc.get("response_entity_id", "___none___")

    if not r_t in dt_m["definitions"]:
        return

    io_params = dt_m["definitions"][ r_t ]["parameters"]

    if not "local" in byc["env"]:
 
        print('Content-Type: text/tsv')
        if byc["download_mode"] is True:
            print('Content-Disposition: attachment; filename='+byc["response_entity_id"]+'.tsv')
        print('status: 200')
        print()

    if "idtable" in byc["output"]:
        io_params = {"id": {"db_key":"id", "type": "string" } }

    header = create_table_header(io_params)

    print("\t".join( header ))

    for pgxdoc in results:

        line = [ ]

        for par, par_defs in io_params.items():

            parameter_type = par_defs.get("type", "string")
            pres = par_defs.get("prefix_split", {})

            if len(pres.keys()) < 1:
                db_key = par_defs.get("db_key", "___undefined___")
                v = get_nested_value(pgxdoc, db_key)
                if isinstance(v, list):
                    line.append("::".join(map(str, (v))))
                else:
                    line.append(str(v))
            else:

                par_vals = pgxdoc.get(par, [])

                # TODO: this is based on the same order of prefixes as in the
                # header generation, w/o checking; should be keyed ...
                for pre in pres.keys():
                    p_id = ""
                    p_label = ""
                    for v in par_vals:
                        if v.get("id", "___none___").startswith(pre):
                            p_id = v.get("id")
                            p_label = v.get("label", "")
                            continue
                    line.append(p_id)
                    line.append(p_label)

        print("\t".join( line ))

    exit()

################################################################################

def import_datatable_dict_line(byc, parent, fieldnames, lineobj, primary_scope="biosample"):

    dt_m = byc["datatable_mappings"]

    if not primary_scope in dt_m["definitions"]:
        return

    io_params = dt_m["definitions"][ primary_scope ]["parameters"]
    def_params = create_table_header(io_params)

    pref_array_values = {}

    for f_n in fieldnames:

        if "#"in f_n:
            continue

        if f_n not in def_params:
            continue

        # this is for the split-by-prefix columns
        par = re.sub(r'___.*?$', '', f_n)
        par_defs = io_params.get(par, {})

        v = lineobj[f_n].strip()

        if len(v) < 1:
            if f_n in io_params.keys():
                v = io_params[f_n].get("default", "")

        if len(v) < 1:
            continue

        # this makes only sense for updating existing data; if there would be
        # no value, the parameter would just be excluded from the update object
        # if there was an empy value
        if "__delete__" in v.lower():
            v = ""

        parameter_type = par_defs.get("type", "string")
        if "num" in parameter_type:
            v = float(v)
        elif "integer" in parameter_type:
            v = int(v)

        if re.match(r"^(\w+[a-zA-Z0-9])_(id|label)___(\w+)$", f_n):
            p, key, pre = re.match(r"^(\w+)_(id|label)___(\w+)$", f_n).group(1,2,3)
            # TODO: this is a bit complicated - label and id per prefix ...
            if not p in pref_array_values.keys():
                pref_array_values.update({p:{pre:{}}})
            if not pre in pref_array_values[p].keys():
                pref_array_values[p].update({pre:{}})
            pref_array_values[p][pre].update({key:v})
            continue
        
        p_d = io_params.get(f_n)
        if not p_d:
            continue

        dotted_key = p_d.get("db_key")
        if not dotted_key:
            continue

        # assign_nested_attribute(parent, db_key, v)
        assign_nested_value(parent, dotted_key, v, p_d)

    for l_k, pre_item in pref_array_values.items():
        if not l_k in parent:
            parent.update({l_k:[]})
        for pre, p_v in pre_item.items():
            parent[l_k].append(p_v)

    return parent

################################################################################

def create_table_header(io_params):

    """podmd
    podmd"""

    header_labs = [ ]

    for par, par_defs in io_params.items():

        pres = par_defs.get("prefix_split", {})

        if len(pres.keys()) < 1:
            header_labs.append( par )
            continue

        for pre in pres.keys():
            header_labs.append( par+"_id"+"___"+pre )
            header_labs.append( par+"_label"+"___"+pre )

    return header_labs

################################################################################
