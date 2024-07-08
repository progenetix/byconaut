import csv, re, requests
# from attrdictionary import AttrDict
from random import sample as randomSamples

# bycon
from bycon import assign_nested_value, get_nested_value, prdbug, prdlhead, prjsonnice, BYC, BYC_PARS, ENV

################################################################################

def export_datatable_download(results):
    # TODO: separate table generation from HTTP response
    dt_m = BYC["datatable_mappings"]
    r_t = BYC.get("response_entity_id", "___none___")
    if not r_t in dt_m["definitions"]:
        return
    io_params = dt_m["definitions"][ r_t ]["parameters"]
    prdlhead(f'{r_t}.tsv')
    header = create_table_header(io_params)
    print("\t".join( header ))

    for pgxdoc in results:
        line = [ ]
        for par, par_defs in io_params.items():
            parameter_type = par_defs.get("type", "string")
            db_key = par_defs.get("db_key", "___undefined___")
            v = get_nested_value(pgxdoc, db_key)
            if isinstance(v, list):
                line.append("::".join(map(str, (v))))
            else:
                line.append(str(v))
        print("\t".join( line ))

    exit()


################################################################################

def import_datatable_dict_line(parent, fieldnames, lineobj, primary_scope="biosample"):
    dt_m = BYC["datatable_mappings"]
    if not primary_scope in dt_m["definitions"]:
        return
    io_params = dt_m["definitions"][ primary_scope ]["parameters"]
    def_params = create_table_header(io_params)
    for f_n in fieldnames:
        if "#"in f_n:
            continue
        if f_n not in def_params:
            continue
        # this is for the split-by-prefix columns
        par = re.sub(r'___.*?$', '', f_n)
        par_defs = io_params.get(par, {})
        v = lineobj[f_n].strip()
        if v == ".":
            v = ""
        if len(v) < 1:
            if f_n in io_params.keys():
                v = io_params[f_n].get("default", "")
        if len(v) < 1:
            continue
        # this makes only sense for updating existing data; if there would be
        # no value, the parameter would just be excluded from the update object
        # if there was an empy value
        if v.lower() in ("___delete___", "__delete__", "none", "___none___", "__none__", "-"):
            v = ""
        parameter_type = par_defs.get("type", "string")
        if "num" in parameter_type:
            v = float(v)
        elif "integer" in parameter_type:
            v = int(v)
        
        p_d = io_params.get(f_n)
        if not p_d:
            continue

        dotted_key = p_d.get("db_key")
        if not dotted_key:
            continue

        # assign_nested_attribute(parent, db_key, v)
        assign_nested_value(parent, dotted_key, v, p_d)

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
