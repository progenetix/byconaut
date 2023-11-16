import pymongo
from os import path, environ

from cgi_parsing import *
from bycon_helpers import get_nested_value, return_paginated_list
from variant_mapping import ByconVariant

services_lib_path = path.join( path.dirname( path.abspath(__file__) ) )
sys.path.append( services_lib_path )
from service_helpers import open_text_streaming, close_text_streaming

################################################################################

def stream_pgx_meta_header(ds_id, ds_results, byc):

    b_p = byc.get("pagination")

    mongo_client = pymongo.MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    bs_coll = mongo_client[ ds_id ][ "biosamples" ]

    open_text_streaming(byc["env"])

    for d in ["id", "assemblyId"]:
        print("#meta=>{}={}".format(d, byc["dataset_definitions"][ds_id][d]))
    # for k, n in s_r_rs["info"]["counts"].items():
    #     print("#meta=>{}={}".format(k, n))
    print(f'#meta=>pagination.skip={b_p["skip"]};pagination.limit={b_p["limit"]}')
            
    print_filters_meta_line(byc)

    for bs_id in ds_results["biosamples.id"][ "target_values" ]:
        bs = bs_coll.find_one( { "id": bs_id } )
        if not bs:
            continue
        h_line = pgxseg_biosample_meta_line(byc, bs, "histological_diagnosis_id")
        print(h_line)

    return


################################################################################

def pgxseg_biosample_meta_line(byc, biosample, group_id_key="histological_diagnosis_id"):

    dt_m = byc["datatable_mappings"]
    io_params = dt_m["definitions"][ "biosample" ]["parameters"]

    g_id_k = group_id_key
    g_lab_k = re.sub("_id", "_label", g_id_k)

    line = [ "#sample=>id={}".format(biosample.get("id", "¡¡¡NONE!!!")) ]

    for par, par_defs in io_params.items():

        in_pgxseg = par_defs.get("compact", False)
        if in_pgxseg is False:
            continue

        parameter_type = par_defs.get("type", "string")
        pres = par_defs.get("prefix_split", {})

        if len(pres.keys()) < 1:
            db_key = par_defs.get("db_key", "___undefined___")
            p_type =par_defs.get("type", "string")
            v = get_nested_value(biosample, db_key, p_type)
            h_v = ""
            if isinstance(v, list):
                h_v = "::".join(map(str, (v)))
            else:
                h_v = str(v)

            if len(h_v) > 0:
                if g_id_k == par:
                    line.append("group_id={}".format(h_v))
                if g_lab_k == par:
                    line.append("group_label={}".format(h_v))
                line.append("{}={}".format(par, h_v))
        else:

            par_vals = biosample.get(par, [])
            if not isinstance(par_vals, list):
                continue
            for pre, pre_defs in pres.items():
                in_pgxseg = pre_defs.get("compact", False)
                if in_pgxseg is False:
                    continue
                for v in par_vals:
                    if v.get("id", "___none___").startswith(pre):
                        line.append("{}_id___{}={}".format(par, pre, v.get("id")))
                        l = v.get("label", "")
                        if len(l) > 0:
                            line.append("{}_label___{}={}".format(par, pre, v.get("id")))
                        continue

    h_line = ";".join(line)

    return h_line

################################################################################    

def __pgxmatrix_interval_header(info_columns, byc):

    int_line = info_columns.copy()

    for iv in byc["genomic_intervals"]:
        int_line.append("{}:{}-{}:DUP".format(iv["reference_name"], iv["start"], iv["end"]))
    for iv in byc["genomic_intervals"]:
        int_line.append("{}:{}-{}:DEL".format(iv["reference_name"], iv["start"], iv["end"]))

    return int_line


################################################################################

def print_filters_meta_line(byc):

    filters = byc.get("filters", [])
    if len(filters) < 1:
        return

    f_vs = []
    for f in filters:
        f_vs.append(f.get("id", ""))
    print("#meta=>filters="+','.join(f_vs))

    return

################################################################################

def export_pgxseg_download(datasets_results, ds_id, byc):

    data_client = pymongo.MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    v_coll = data_client[ ds_id ][ "variants" ]
    ds_results = datasets_results.get(ds_id, {})
    # prdbug(byc, ds_results)
    if not "variants._id" in ds_results:
        # TODO: error message here
        return
    v__ids = ds_results["variants._id"].get("target_values", [])
    if test_truthy( byc["form_data"].get("paginate_results", True) ):
        v__ids = return_paginated_list(v__ids, byc.get("skip", 0), byc.get("limit", 0))

    stream_pgx_meta_header(ds_id, ds_results, byc)
    print_pgxseg_header_line()

    v_instances = []
    for v_id in v__ids:
        v_s = v_coll.find_one( { "_id": v_id }, { "_id": 0 } )
        v_instances.append(ByconVariant(byc).byconVariant(v_s))

    v_instances = list(sorted(v_instances, key=lambda x: (f'{x["reference_name"].replace("X", "XX").replace("Y", "YY").zfill(2)}', x['start'])))
    for v in v_instances:
        print_variant_pgxseg(v)

    close_text_streaming()

################################################################################

def print_variant_pgxseg(v_pgxseg):

    print( pgxseg_variant_line(v_pgxseg) )

################################################################################

def print_pgxseg_header_line():

    print( pgxseg_header_line() )

################################################################################

def pgxseg_header_line():

    return "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format("biosample_id", "reference_name", "start", "end", "log2", "variant_type", "reference_bases", "alternate_bases" )

################################################################################

def pgxseg_variant_line(v_pgxseg):

    for p in ("sequence", "reference_sequence"):
        if not v_pgxseg[p]:
            v_pgxseg.update({p: "."})

    log_v = "."
    if "info" in v_pgxseg:
        if v_pgxseg["info"]:
            log_v = v_pgxseg["info"].get("cnv_value", ".")

    v_l = (
        v_pgxseg.get("biosample_id"),
        v_pgxseg["reference_name"],
        v_pgxseg["start"],
        v_pgxseg["end"],
        log_v,
        v_pgxseg.get("variant_type", "."),
        v_pgxseg.get("reference_sequence"),
        v_pgxseg.get("sequence")
    )

    return "\t".join([str(x) for x in v_l])

################################################################################

def export_callsets_matrix(datasets_results, ds_id, byc):

    g_b = byc["interval_definitions"].get("genome_binning", "")
    i_no = len(byc["genomic_intervals"])

    m_format = "coverage"
    if "val" in byc["output"]:
        m_format = "values"

    ds_results = datasets_results[ds_id]
    p_r = byc["pagination"]

    if not "callsets._id" in ds_results:
        return

    cs_r = ds_results["callsets._id"]

    mongo_client = pymongo.MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    bs_coll = mongo_client[ ds_id ][ "biosamples" ]
    cs_coll = mongo_client[ ds_id ][ "callsets" ]

    open_text_streaming(byc["env"], "interval_callset_matrix.pgxmatrix")

    for d in ["id", "assemblyId"]:
        d_v = byc["dataset_definitions"][ds_id].get(d)
        if d_v:
            print(f'#meta=>{d}={d_v}')
    print_filters_meta_line(byc)
    print(f'#meta=>data_format=interval_{m_format}')

    info_columns = [ "analysis_id", "biosample_id", "group_id" ]
    h_line = __pgxmatrix_interval_header(info_columns, byc)
    info_col_no = len(info_columns)
    int_col_no = len(h_line) - len(info_columns)
    print(f'#meta=>genome_binning={g_b};interval_number={i_no}')
    print(f'#meta=>no_info_columns={info_col_no};no_interval_columns={int_col_no}')

    q_vals = cs_r["target_values"]
    r_no = len(q_vals)
    skip = byc["pagination"].get("skip", 0)
    limit = byc["pagination"].get("limit", 0)
    if r_no > p_r["limit"]:
        if test_truthy( byc["form_data"].get("paginate_results", True) ):
            q_vals = return_paginated_list(q_vals, skip, limit)
        print(f'#meta=>"WARNING: Only {len(q_vals)} analyses will be included due to pagination skip {skip} and limit {limit}."')

    bios_ids = set()
    cs_ids = {}
    cs_cursor = cs_coll.find({"_id": {"$in": q_vals } } )
    for cs in cs_cursor:
        bios = bs_coll.find_one( { "id": cs["biosample_id"] } )
        bios_ids.add(bios["id"])
        s_line = "#sample=>biosample_id={};analysis_id={}".format(bios["id"], cs["id"])
        h_d = bios["histological_diagnosis"]
        cs_ids.update({cs["id"]: h_d.get("id", "NA")})
        s_line = '{};group_id={};group_label={};NCIT::id={};NCIT::label={}'.format(s_line, h_d.get("id", "NA"), h_d.get("label", "NA"), h_d.get("id", "NA"), h_d.get("label", "NA"))
        print(s_line)

    print("#meta=>biosampleCount={};analysisCount={}".format(len(bios_ids), cs_r["target_count"]))
    print("\t".join(h_line))

    for cs_id, group_id in cs_ids.items():
        cs = cs_coll.find_one({"id":cs_id})
        if "values" in m_format:
            print("\t".join(
                [
                    cs_id,
                    cs.get("biosample_id", "NA"),
                    group_id,
                    *map(str, cs["cnv_statusmaps"]["max"]),
                    *map(str, cs["cnv_statusmaps"]["min"])
                ]
            ))
        else:
            print("\t".join(
                [
                    cs_id,
                    cs.get("biosample_id", "NA"),
                    group_id,
                    *map(str, cs["cnv_statusmaps"]["dup"]),
                    *map(str, cs["cnv_statusmaps"]["del"])
                ]
            ))

    close_text_streaming()

################################################################################

def export_pgxseg_frequencies(byc, results):

    if not "pgxseg" in byc["output"] and not "pgxfreq" in byc["output"]:
        return

    g_b = byc["interval_definitions"].get("genome_binning", "")
    i_no = len(byc["genomic_intervals"])

    open_text_streaming(byc["env"], "interval_frequencies.pgxfreq")

    print(f'#meta=>genome_binning={g_b};interval_number={i_no}')
    h_ks = ["reference_name", "start", "end", "gain_frequency", "loss_frequency", "no"]

    # should get error checking if made callable

    for f_set in results:
        m_line = []
        for k in ["group_id", "label", "dataset_id", "sample_count"]:
            m_line.append(k+"="+str(f_set[k]))
        print("#group=>"+';'.join(m_line))

    print("group_id\t"+"\t".join(h_ks))

    for f_set in results:
        for intv in f_set["interval_frequencies"]:
            v_line = [ ]
            v_line.append(f_set[ "group_id" ])
            for k in h_ks:
                v_line.append(str(intv[k]))
            print("\t".join(v_line))

    close_text_streaming()

################################################################################

def export_pgxmatrix_frequencies(byc, results):

    g_b = byc["interval_definitions"].get("genome_binning", "")
    i_no = len(byc["genomic_intervals"])

    open_text_streaming(byc["env"], "interval_frequencies.pgxmatrix")

    print(f'#meta=>genome_binning={g_b};interval_number={i_no}')

    # should get error checking if made callable
    for f_set in results:
        m_line = []
        for k in ["group_id", "label", "dataset_id", "sample_count"]:
            m_line.append(k+"="+str(f_set[k]))
        print("#group=>"+';'.join(m_line))
    # header

    h_line = [ "group_id" ]
    h_line = __pgxmatrix_interval_header(h_line, byc)
    print("\t".join(h_line))

    for f_set in results:
        f_line = [ f_set[ "group_id" ] ]
        for intv in f_set["interval_frequencies"]:
            f_line.append( str(intv["gain_frequency"]) )
        for intv in f_set["interval_frequencies"]:
            f_line.append( str(intv["loss_frequency"]) )

        print("\t".join(f_line))

    close_text_streaming()

################################################################################

def export_vcf_download(datasets_results, ds_id, byc):

    """
    """

    # TODO: VCF schema in some config file...
    open_text_streaming(byc["env"], f"{ds_id}_variants.vcf")
    print(
        """##fileformat=VCFv4.4
##reference=GRCh38
##ALT=<ID=DUP,Description="Duplication">
##ALT=<ID=DEL,Description="Deletion">
##INFO=<ID=END,Number=1,Type=Integer,Description="End position of the longest variant described in this record">
##INFO=<ID=SVLEN,Number=A,Type=Integer,Description="Length of structural variant">
##INFO=<ID=CN,Number=A,Type=Float,Description="Copy number of CNV/breakpoint">
##INFO=<ID=SVCLAIM,Number=A,Type=String,Description="Claim made by the structural variant call. Valid values are D, J, DJ for abundance, adjacency and both respectively">
##INFO=<ID=IMPRECISE,Number=0,Type=Flag,Description="Imprecise structural variation">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">"""
    )

    v_d = byc["variant_parameters"]
    v_o = {
        "#CHROM": ".",
        "POS": ".",
        "ID": ".",
        "REF": ".",
        "ALT": ".",
        "QUAL": ".",
        "FILTER": "PASS",
        "FORMAT": "",
        "INFO": ""
    }

    data_client = pymongo.MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
    v_coll = data_client[ ds_id ][ "variants" ]
    ds_results = datasets_results.get(ds_id, {})
    if not "variants._id" in ds_results:
        # TODO: error message here
        return
    v__ids = ds_results["variants._id"].get("target_values", [])
    if test_truthy( byc["form_data"].get("paginate_results", True) ):
        v__ids = return_paginated_list(v__ids, byc.get("skip", 0), byc.get("limit", 0))

    v_instances = []
    for v_id in v__ids:
        v = v_coll.find_one( { "_id": v_id }, { "_id": 0 } )
        v_instances.append(ByconVariant(byc).byconVariant(v))

    v_instances = list(sorted(v_instances, key=lambda x: (f'{x["reference_name"].replace("X", "XX").replace("Y", "YY").zfill(2)}', x['start'])))

    variant_ids = []
    for v in v_instances:
        v_iid = v.get("variant_internal_id", "__none__")
        if v_iid not in variant_ids:
            variant_ids.append(v_iid)

    biosample_ids = []
    for v in v_instances:
        biosample_ids.append(v.get("biosample_id", "__none__"))
    # no duplicates here since each has its column
    biosample_ids = list(set(biosample_ids))

    for bsid in biosample_ids:
        v_o.update({bsid: "."})

    print("\t".join(v_o.keys()))

    bv = ByconVariant(byc)
    for d in variant_ids:

        prdbug(byc, d)

        d_vs = [var for var in v_instances if var.get('variant_internal_id', "__none__") == d]
        vcf_v = bv.vcfVariant(d_vs[0])
        
        for bsid in biosample_ids:
            vcf_v.update({bsid: "."})

        for d_v in d_vs:
            b_i = d_v.get("biosample_id", "__none__")
            vcf_v.update({b_i: "0/1"})

        r_l = map(str, list(vcf_v.values()))
        print("\t".join(r_l))

    close_text_streaming()

