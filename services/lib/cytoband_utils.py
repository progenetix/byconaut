import csv, datetime, re, time, base36, yaml
from os import environ, path, pardir

from bycon import generate_id, ByconVariant

################################################################################
################################################################################
################################################################################

def parse_cytoband_file(byc):
    """podmd
 
    podmd"""
 
    cb_file = path.join( byc["genome_rsrc_path"], "cytoBandIdeo.txt")
    cb_re = re.compile( byc["interval_definitions"][ "cytobands" ][ "pattern" ] )
    cb_keys = [ "chro", "start", "end", "cytoband", "staining" ]

    cytobands = [ ]
    cytolimits = { }
    genome_size = 0

    i = 0

    c_bands = [ ]
    with open(cb_file) as cb_f:                                                                                          
        for c_band in csv.DictReader(filter(lambda row: row.startswith('#') is False, cb_f), fieldnames=cb_keys, delimiter='\t'):
            c_bands.append(c_band)

    #--------------------------------------------------------------------------#

    # !!! making sure the chromosomes are sorted !!!
    for chro in byc["interval_definitions"]["chromosomes"]:
        chro = str(chro)
        c_m = f'chr{chro}'
        chrobands = [ ]
        for cb in c_bands:
            if cb["chro"] == c_m:
                cb["i"] = i
                cb["chro"] = cb["chro"].replace("chr", "")
                cb["chroband"] = cb["chro"]+cb[ "cytoband" ]
                cytobands.append(dict(cb))
                chrobands.append(dict(cb))
                i += 1
        cytolimits.update({
            chro: {
                "chro": [ int(cytobands[0]["start"]), int(cytobands[-1]["end"]) ],
                "size": int(cytobands[-1]["end"]) - int(cytobands[0]["start"]),
                "p": arm_base_range(chro, "p", cytobands),
                "q": arm_base_range(chro, "q", cytobands)
            }
        })
        genome_size += int(chrobands[-1]["end"])

    #--------------------------------------------------------------------------#

    byc.update( {
        "cytobands": cytobands,
        "cytolimits": cytolimits,
        "genome_size": genome_size
    } )


################################################################################

def bands_from_cytobands(chr_bands, byc):

    cb_pat = re.compile( byc["argument_definitions"]["cyto_bands"]["pattern"] )
    error = ""

    end_re = re.compile(r"^([pq]\d.*?)\.?\d$")
    arm_re = re.compile(r"^([pq]).*?$")
    p_re = re.compile(r"^p.*?$")
    q_re = re.compile(r"^q.*?$")

    # chr_bands = "4pcenqcen"

    if "p10" in chr_bands:
        chr_bands = re.sub("p10", "pcen", chr_bands)
    if "q10" in chr_bands:
        chr_bands = re.sub("q10", "qcen", chr_bands)

    # print("|||-"+chr_bands+"-|||")

    chro, cb_start, cb_end = cb_pat.match(chr_bands).group(1,2,3)

    cytobands = list(filter(lambda d: d[ "chro" ] == chro, byc["cytobands"].copy()))
    if len(cytobands) < 10:
        return([], "", "", "", "error")

    if cb_start is None and cb_end is None:
        return cytobands, chro, int( cytobands[0]["start"] ), int( cytobands[-1]["end"] ), error

    p_bands = list(filter(lambda d: p_re.match(d[ "cytoband" ]), cytobands))
    q_bands = list(filter(lambda d: q_re.match(d[ "cytoband" ]), cytobands))

    # if there was no end, the start band is queried again until its last match
    if cb_end is None:
        cb_end = cb_start

    if "qter" in cb_start:
        cb_start = cytobands[-1]["cytoband"]
    if "qter" in cb_end:
        cb_end = cytobands[-1]["cytoband"]
    if "pter" in cb_start:
        cb_start = cytobands[0]["cytoband"]
    if "pter" in cb_end:
        cb_end = cytobands[0]["cytoband"]
    if "pcen" in cb_start:
        cb_start = p_bands[-1]["cytoband"]
    if "pcen" in cb_end:
        cb_end = p_bands[-1]["cytoband"]
    if "qcen" in cb_start:
        cb_start = q_bands[0]["cytoband"]
    if "qcen" in cb_end:
        cb_end = q_bands[0]["cytoband"]

    if "p" in cb_end:
        if "q" in cb_start:
            cb_start, cb_end = cb_end, cb_start

    # using a numeric comparison to sort bands for p higher to lower
    cb_re = re.compile(r'^([pq])((\d)(?:\d(?:\.\d\d?\d?)?)?)$', re.IGNORECASE)
    if cb_re.match(cb_start) and cb_re.match(cb_end):
        fb1 = float( cb_re.match(cb_start).group(2) )
        fb2 = float( cb_re.match(cb_end).group(2) )
        arm1 = cb_re.match(cb_start).group(1)
        arm2 = cb_re.match(cb_end).group(1)
        mb1 = int( cb_re.match(cb_start).group(3) )
        mb2 = int( cb_re.match(cb_end).group(3) )
        if arm1 == arm2:
            if "p" in arm1:
                if not mb1 > mb2:
                    if fb2 > fb1:
                        cb_start, cb_end = cb_end, cb_start
            elif "q" in arm1:
                if not mb2 > mb1:
                    if fb2 < fb1:
                        cb_start, cb_end = cb_end, cb_start

    # print("\n", chro, cb_start, cb_end, chr_bands)

    # TODO: this is ugly - someho whad problems w/ recursion version :-/
    start_bands = match_bands(cb_start, cytobands)
    if len(start_bands) < 1:
        if end_re.match(cb_start):
            band = end_re.match(cb_start).group(1)
            start_bands = match_bands(band, cytobands)
            if len(start_bands) < 1:
                if arm_re.match(cb_start):
                    band = arm_re.match(cb_start).group(1)
                    start_bands = match_bands(band, cytobands)

    end_bands = match_bands(cb_end, cytobands)
    if len(end_bands) < 1:
        if end_re.match(cb_end):
            band = end_re.match(cb_end).group(1)
            end_bands = match_bands(band, cytobands)
            if len(end_bands) < 1:
                if arm_re.match(cb_end):
                    band = arm_re.match(cb_end).group(1)
                    end_bands = match_bands(band, cytobands)

    cb_from = start_bands[0]["i"]
    cb_to = end_bands[-1]["i"] + 1

    matched = byc["cytobands"][cb_from:cb_to]
 
    return matched, chro, int( matched[0]["start"] ), int( matched[-1]["end"]), error


################################################################################

def match_bands(band, cytobands):

    cb_re = re.compile(rf"^{band}", re.IGNORECASE)
    m_b_s = list( filter(lambda d:cb_re.match(d["cytoband"]), cytobands) )
    return m_b_s


################################################################################

def arm_base_range(chro, arm, cytobands):

    if arm not in ["p","q", "P", "Q"]:
        return 0, 1

    arm_re = re.compile(rf"^{arm}", re.IGNORECASE)
    bands = list(filter(lambda d: d[ "chro" ] == chro, cytobands))
    bands = list(filter(lambda d: arm_re.match(d[ "cytoband" ]), bands))

    return [ int(bands[0]["start"]), int(bands[-1]["end"]) ]


################################################################################

def cytobands_label( cytobands ):

    """
    Receives: (potentially filtered) list of cytoband objects
    Returns: the concatenated first and last cytoband label
    Examples:
      - p12.2q22.2
      - q13.1
    """

    cb_label = ""

    if len(cytobands) > 0:
        cb_label = cytobands[0].get("cytoband", "")
        if len( cytobands ) > 1:
            cb_label = cb_label+cytobands[-1].get("cytoband", "")

    return cb_label


################################################################################

def variants_from_revish(bs_id, cs_id, technique, iscn, byc):

    v_s, v_e = deparse_ISCN_to_variants(iscn, byc)
    variants = []

    # the id here is a placeholder since we now use a stringified version of the
    # MongoDB ObjectId w/ `pgxvar-` prepend
    for v in v_s:

        v.update({
            "id": generate_id("pgxvar"),
            "biosample_id": bs_id,
            "callset_id": cs_id,
            "updated": datetime.datetime.now().isoformat()
        })

        variants.append(ByconVariant(byc).byconVariant(v))

    return variants, v_e


################################################################################

def deparse_ISCN_to_variants(iscn, byc):

    a_d = byc.get("argument_definitions", {})
    g_a = byc.get("genome_aliases", {})
    i_d = byc["interval_definitions"]
    v_t_defs = byc.get("variant_type_definitions")

    iscn = "".join(iscn.split())
    variants = []
    cb_pat = re.compile( a_d["cyto_bands"]["pattern"] )
    errors = []

    for cnv_t, cnv_defs in v_t_defs.items():

        revish = cnv_defs.get("revish_label")
        if not revish:
            continue

        iscn_re = re.compile(rf"^.*?{revish}\(([\w.,]+)\).*?$", re.IGNORECASE)

        if iscn_re.match(iscn):

            m = iscn_re.match(iscn).group(1)

            for i_v in re.split(",", m):
                
                if not cb_pat.match(i_v):
                    continue

                cytoBands, chro, start, end, error = bands_from_cytobands(i_v, byc)
                if len(error) > 0:
                    errors.append(error)
                    continue

                v_l = end - start
                t = cnv_defs.get("DUPDEL", "CNV")

                cytostring = "{}({})".format(cnv_t, i_v).lower()

                if "amp" in revish and v_l > i_d.get("cnv_amp_max_size", 3000000):
                    revish = "hldup"

                v_s = {}
               
                v = ({
                    "variant_state": cnv_defs.get("variant_state"),
                    "location": {
                        "sequence_id": g_a["refseq_aliases"].get(chro),
                        "chromosome": chro,
                        "start": start,
                        "end": end
                    },
                    "info": {
                        "ISCN": cytostring,
                        "var_length": v_l,
                        "cnv_value": cnv_defs.get("cnv_dummy_value"),
                        "note": "from text annotation; CNV dummy value"
                    }
                })

                variants.append(v)

    return variants, " :: ".join(errors)


################################################################################

def cytobands_label_from_positions(byc, chro, start, end):

    cytobands, chro, start, end = cytobands_list_from_positions(byc, chro, start, end)
    cbl = cytobands_label( cytobands )

    return cbl


################################################################################

def bands_from_chrobases(chro_bases, byc):

    cb_pat = re.compile( byc["argument_definitions"]["chro_bases"]["pattern"] )
    if not cb_pat.match(chro_bases):
        return [], "NA", 0, 0
    chro, cb_start, cb_end = cb_pat.match(chro_bases).group(2,3,5)

    return cytobands_list_from_positions(byc, chro, cb_start, cb_end)


################################################################################

def cytobands_list_from_positions(byc, chro, start=None, end=None):

    if start:
        start = int(start)
        if not end:
            end = start + 1
        end = int(end)

    cytobands = list(filter(lambda d: d[ "chro" ] == chro, byc["cytobands"]))
    if start == None:
        start = 0
    if end == None:
        end = int( cytoBands[-1]["end"] )

    if isinstance(start, int):
        cytobands = list(filter(lambda d: int(d[ "end" ]) > start, cytobands))

    if isinstance(end, int):
        cytobands = list(filter(lambda d: int(d[ "start" ]) < end, cytobands))
    else:
        print(end)

    return cytobands, chro, start, end

