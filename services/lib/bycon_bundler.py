import csv, datetime, re, sys

from os import environ, path
from pymongo import MongoClient
from copy import deepcopy

from datatable_utils import import_datatable_dict_line
from interval_utils import interval_cnv_arrays, interval_counts_from_callsets
from variant_mapping import ByconVariant
from bycon_helpers import return_paginated_list


services_lib_path = path.join( path.dirname( path.abspath(__file__) ) )
sys.path.append( services_lib_path )
from file_utils import *

################################################################################
################################################################################
################################################################################

class ByconBundler:

    """
    # The `ByconBundler` class

    This class bundles documents from the main entities which have a complete
    intersection - e.g. for a set of variants their callsets, biosamples and
    individuals. The bundling does _not_ have to be complete; e.g. bundles may
    be based on only some matched variants (not all variants of the referenced
    callsets); and bundles may have empty lists for some entities.
    """

    def __init__(self, byc):

        self.byc = byc
        self.errors = []
        self.filepath = None
        self.datasets_results = None
        self.header = []
        self.data = []
        self.fieldnames = []
        self.callsetVariantsBundles = []
        self.intervalFrequenciesBundles = []
        pagination = byc.get("pagination", {"skip": 0, "limit": 0})
        self.limit = pagination.get("limit", 0)
        self.skip = pagination.get("skip", 0)

        self.bundle = {
            "variants": [],
            "callsets": [],
            "biosamples": [],
            "individuals": [],
            "ds_id": None,
            "info": {
                "errors": []
            }
        }

        self.keyedBundle = {
            "variants_by_callset_id": {},
            "callsets_by_id": {},
            "individuals_by_id": {},
            "biosamples_by_id": {},
            "ds_id": None,
            "info": {
                "errors": []
            }
        }

    #--------------------------------------------------------------------------#
    #----------------------------- public -------------------------------------#
    #--------------------------------------------------------------------------#

    def read_pgx_file(self, filepath):

        self.filepath = filepath

        h_lines = []

        with open(self.filepath) as f:
            for line in f:
                line = line.strip()
                if line.startswith("#"):
                    h_lines.append(line)

        d_lines, fieldnames = read_tsv_to_dictlist(self.filepath, max_count=0)

        self.header = h_lines
        self.data = d_lines
        self.fieldnames = fieldnames

        return self
        

    #--------------------------------------------------------------------------#

    def read_probedata_file(self, filepath):

        self.filepath = filepath
        self.probedata = []

        p_lines, fieldnames = read_tsv_to_dictlist(self.filepath, max_count=0)

        p_o = {
            "probe_id": False,
            "reference_name": False,
            "start": False,
            "value": False
        }

        p_f_d = {
            "probe_id": {"type": "string", "key": fieldnames[0]},
            "reference_name": {"type": "string", "key": fieldnames[1]},
            "start": {"type": "integer", "key": fieldnames[2]},
            "value": {"type": "number", "key": fieldnames[3]}
        }

        for l in p_lines:
            p = deepcopy(p_o)
            for pk, pv in p_f_d.items():
                l_k = pv["key"]
                p.update({ pk: l.get(l_k) })
                if "int" in pv["type"]:
                    p.update({ pk: int(p[pk]) })
                elif "num" in pv["type"]:
                    p.update({ pk: float(p[pk]) })
            self.probedata.append(p)

        return self.probedata

    #--------------------------------------------------------------------------#

    def pgxseg_to_keyed_bundle(self, filepath):
        self.read_pgx_file(filepath)

        if not "biosample_id" in self.fieldnames:
            self.errors.append("¡¡¡ The `biosample_id` parameter is required for variant assignment !!!")
            return

        self.__deparse_pgxseg_samples_header()
        self.__keyed_bundle_add_variants_from_lines()

        return self.keyedBundle

    #--------------------------------------------------------------------------#

    def pgxseg_to_bundle(self, filepath):

        self.pgxseg_to_keyed_bundle(filepath)
        self.__flatten_keyed_bundle()

        return self.bundle


    #--------------------------------------------------------------------------#

    def callsets_variants_bundles(self):

        # TODO: This is similar to a keyed bundle component ...

        bb = self.bundle

        c_p_l = []
        for p_o in bb.get("callsets", []):
            cs_id = p_o.get("id")
            p_o.update({
                "ds_id": bb.get("ds_id", ""),
                "variants":[]
            })
            for v in bb["variants"]:
                if v.get("callset_id", "") == cs_id:
                    p_o["variants"].append(ByconVariant(self.byc).byconVariant(v))

            c_p_l.append(p_o)
            
        self.callsetVariantsBundles = c_p_l

        return self.callsetVariantsBundles


    #--------------------------------------------------------------------------#

    def resultsets_callset_bundles(self, datasets_results={}):
        self.datasets_results = datasets_results
        self.__callsets_bundle_from_result_set()
        self.__callsets_add_database_variants()
        return { "callsets_variants_bundles": self.callsetVariantsBundles }


    #--------------------------------------------------------------------------#

    def resultsets_frequencies_bundles(self, datasets_results=[]):
        self.datasets_results = datasets_results
        self.__callsets_bundle_from_result_set()
        self.intervalFrequenciesBundles.append(self.__callsetBundleCreateIset())
        return {"interval_frequencies_bundles": self.intervalFrequenciesBundles}


    #--------------------------------------------------------------------------#

    def callsets_frequencies_bundles(self):
            
        self.intervalFrequenciesBundles.append(self.__callsetBundleCreateIset())
        return self.intervalFrequenciesBundles


    #--------------------------------------------------------------------------#
    #----------------------------- private ------------------------------------#
    #--------------------------------------------------------------------------#

    def __deparse_pgxseg_samples_header(self):

        b_k_b = self.keyedBundle
        h_l = self.header

        for l in h_l:
            if not l.startswith("#sample=>"):
                continue       
            l = re.sub("#sample=>", "", l)
            bios_d = {}
            for p_v in l.split(";"):
                k, v = p_v.split("=")
                v = re.sub(r'^[\'\"]', '', v)
                v = re.sub(r'[\'\"]$', '', v)
                bios_d.update({k:v})
            fieldnames = list(bios_d.keys())
            bs_id = bios_d.get("biosample_id")
            if bs_id is None:
                continue

            bios = {"id": bs_id} 
            bios = import_datatable_dict_line(self.byc, bios, fieldnames, bios_d, "biosample")
            cs_id = bios.get("callset_id", re.sub("pgxbs", "pgxcs", bs_id) )
            ind_id = bios.get("individual_id", re.sub("pgxbs", "pgxind", bs_id) )
            ind = {"id": ind_id} 
            cs = {"id": cs_id, "biosample_id": bs_id, "individual_id": ind_id} 

            bios.update({"individual_id": ind_id})

            # b_k_b["callsets_by_id"].update({ cs_id: import_datatable_dict_line(self.byc, cs, fieldnames, bios_d, "analysis") })
            # b_k_b["individuals_by_id"].update({ ind_id: import_datatable_dict_line(self.byc, ind, fieldnames, bios_d, "individual") })
            b_k_b["callsets_by_id"].update({ cs_id: cs })
            b_k_b["individuals_by_id"].update({ ind_id: ind })
            b_k_b["biosamples_by_id"].update({ bs_id: bios })
            b_k_b["variants_by_callset_id"].update({ cs_id: [] })

        self.keyedBundle = b_k_b


    #--------------------------------------------------------------------------#

    def __callsets_bundle_from_result_set(self):

        for ds_id, ds_res in self.datasets_results.items():
            if not "callsets._id" in ds_res:
                continue

            mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
            cs_coll = mongo_client[ds_id]["callsets"]
            cs_r = ds_res["callsets._id"]
            cs__ids = cs_r["target_values"]
            r_no = len(cs__ids)
            if r_no < 1:
                continue
            cs__ids = return_paginated_list(cs__ids, self.skip, self.limit)

            for cs__id in cs__ids:
                cs = cs_coll.find_one({"_id": cs__id })
                cs_id = cs.get("id", "NA")

                cnv_chro_stats = cs.get("cnv_chro_stats", False)
                cnv_statusmaps = cs.get("cnv_statusmaps", False)

                if cnv_chro_stats is False or cnv_statusmaps is False:
                    continue

                p_o = {
                    "dataset_id": ds_id,
                    "callset_id": cs_id,
                    "biosample_id": cs.get("biosample_id", "NA"),
                    "cnv_chro_stats": cs.get("cnv_chro_stats"),
                    "cnv_statusmaps": cs.get("cnv_statusmaps"),
                    "probefile": callset_guess_probefile_path(cs, self.byc),
                    "variants": []
                }

                # TODO: add optional probe read in

                self.bundle["callsets"].append(p_o)

        return

    #--------------------------------------------------------------------------#

    def __callsets_add_database_variants(self):


        bb = self.bundle
        c_p_l = []

        mongo_client = MongoClient(host=environ.get("BYCON_MONGO_HOST", "localhost"))
        for p_o in bb.get("callsets", []):
            ds_id = p_o.get("dataset_id", "___none___")
            var_coll = mongo_client[ds_id]["variants"]
            cs_id = p_o.get("callset_id", "___none___")
            v_q = {"callset_id": cs_id}
            for v in var_coll.find(v_q):
               p_o["variants"].append(ByconVariant(self.byc).byconVariant(v))

            c_p_l.append(p_o)

        self.callsetVariantsBundles = c_p_l
        return


    #--------------------------------------------------------------------------#

    def __keyed_bundle_add_variants_from_lines(self):

        fieldnames = self.fieldnames
        varlines = self.data

        b_k_b = self.keyedBundle

        inds_ided = b_k_b.get("individuals_by_id", {})
        bios_ided = b_k_b.get("biosamples_by_id", {})
        cs_ided = b_k_b.get("callsets_by_id", {})

        vars_ided = b_k_b.get("variants_by_callset_id", {})

        for v in varlines:

            bs_id = v.get("biosample_id", "___none___")

            # If the biosample exists in metadata all the other items will exist by id
            if not bs_id in bios_ided:
                cs_id = re.sub(r'^(pgxbs-)?', "pgxcs-", bs_id)
                ind_id = re.sub(r'^(pgxbs-)?', "pgxind-", bs_id)
                cs_ided.update( {cs_id: {"id": cs_id, "biosample_id": bs_id, "individual_id": ind_id } } )
                vars_ided.update( {cs_id: [] } )
                bios_ided.update( {bs_id: {"id": bs_id, "individual_id": ind_id } } )
                inds_ided.update( {ind_id: {"id": ind_id } } )
            else:
                for cs_i, cs_v in cs_ided.items():
                    if cs_v.get("biosample_id", "___nothing___") == bs_id:
                        cs_id = cs_i
                        continue
            
            bios = bios_ided.get(bs_id)
            cs = cs_ided.get(cs_id)
            ind_id = bios.get("individual_id", "___nothing___")
            ind = inds_ided.get(ind_id)

            update_v = {
                "individual_id": ind_id,
                "biosample_id": bs_id,
                "callset_id": cs_id,
            }

            update_v = import_datatable_dict_line(self.byc, update_v, fieldnames, v, "genomicVariant")
            update_v = ByconVariant(self.byc).pgxVariant(update_v)
            update_v.update({
                "updated": datetime.datetime.now().isoformat()
            })

            vars_ided[cs_id].append(update_v)

        for cs_id, cs_vars in vars_ided.items():
            maps, cs_cnv_stats, cs_chro_stats = interval_cnv_arrays(cs_vars, self.byc)           
            cs_ided[cs_id].update({"cnv_statusmaps": maps})
            cs_ided[cs_id].update({"cnv_stats": cs_cnv_stats})
            cs_ided[cs_id].update({"cnv_chro_stats": cs_chro_stats})
            cs_ided[cs_id].update({"updated": datetime.datetime.now().isoformat()})

        self.keyedBundle.update({
            "individuals_by_id": inds_ided,
            "biosamples_by_id": bios_ided,
            "callsets_by_id": cs_ided,
            "variants_by_callset_id": vars_ided
        })

    #--------------------------------------------------------------------------#

    def __flatten_keyed_bundle(self):

        b_k_b = self.keyedBundle

        bios_k = b_k_b.get("biosamples_by_id", {})
        ind_k = b_k_b.get("individuals_by_id", {})
        cs_k = b_k_b.get("callsets_by_id", {})
        v_cs_k = b_k_b.get("variants_by_callset_id", {})

        self.bundle.update({
            "biosamples": list( bios_k.values() ),
            "individuals": list( ind_k.values() ),
            "callsets": list( cs_k.values() ),
            "variants": [elem for sublist in ( v_cs_k.values() ) for elem in sublist]
        })

    #--------------------------------------------------------------------------#

    def __callsetBundleCreateIset(self, label=""):

        intervals, cnv_cs_count = interval_counts_from_callsets(self.bundle["callsets"], self.byc)

        ds_id = self.bundle.get("ds_id", "")
        iset = {
            "dataset_id": ds_id,
            "group_id": ds_id,
            "label": label,
            "sample_count": cnv_cs_count,
            "interval_frequencies": []
        }

        for intv_i, intv in enumerate(intervals):
            iset["interval_frequencies"].append(intv.copy())

        return iset

################################################################################
