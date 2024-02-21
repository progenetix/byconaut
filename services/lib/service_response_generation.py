from deepmerge import always_merger
from os import environ

from bycon_helpers import mongo_result_list, mongo_test_mode_query, return_paginated_list
from cgi_parsing import prdbug
from config import BYC, BYC_PARS
from export_file_generation import *
from query_execution import execute_bycon_queries
from query_generation import ByconQuery
from response_remapping import *
from schema_parsing import object_instance_from_schema_name

from service_helpers import set_selected_delivery_keys

################################################################################

class ByconautServiceResponse:

    def __init__(self, byc: dict, response_schema="byconautServiceResponse"):
        self.byc = byc
        self.beacon_defaults = byc.get("beacon_defaults", {})
        self.services_defaults = byc.get("services_defaults", {})
        self.entity_defaults = self.beacon_defaults.get("entity_defaults", {"info":{}})
        self.service_config = self.byc.get("service_config", {})
        self.response_schema = response_schema
        self.requested_granularity = BYC_PARS.get("requested_granularity", "record")
        # TBD for authentication? 
        self.returned_granularity = self.requested_granularity
        self.beacon_schema = self.byc["response_entity"].get("beacon_schema", "___none___")
        self.data_response = object_instance_from_schema_name(byc, response_schema, "")
        self.error_response = object_instance_from_schema_name(byc, "beaconErrorResponse", "")
        self.__meta_add_received_request_summary_parameters()
        self.__meta_add_parameters()

        return
    

    # -------------------------------------------------------------------------#
    # ----------------------------- public ------------------------------------#
    # -------------------------------------------------------------------------#

    def collationsResponse(self):
        if not "byconautServiceResponse" in self.response_schema:
            return

        colls = ByconCollations(self.byc).populatedCollations()
        self.data_response["response"].update({"results": colls})
        self.__service_response_update_summaries()
        self.__serviceResponse_force_granularities()
        return self.data_response


    # -------------------------------------------------------------------------#

    def emptyResponse(self):
        if not "byconautServiceResponse" in self.response_schema:
            return
        return self.data_response


    # -------------------------------------------------------------------------#

    def populatedResponse(self, results=[]):
        if not "byconautServiceResponse" in self.response_schema:
            return
        self.data_response["response"].update({"results": results})
        self.__service_response_update_summaries()
        self.__serviceResponse_force_granularities()
        return self.data_response


    # -------------------------------------------------------------------------#

    def errorResponse(self):
        return self.error_response


    # -------------------------------------------------------------------------#
    # ----------------------------- private -----------------------------------#
    # -------------------------------------------------------------------------#

    def __meta_add_parameters(self):
        r_m = self.data_response.get("meta", {})
        if BYC["TEST_MODE"] is True:
            r_m.update({"test_mode": BYC["TEST_MODE"]})
        if "returned_schemas" in r_m:
            r_m.update({"returned_schemas":[self.beacon_schema]})

        info = self.entity_defaults["info"].get("content", {"api_version": "___none___"})
        for p in ["api_version", "beacon_id"]:
            if p in info.keys():
                r_m.update({p: info.get(p, "___none___")})

        # TODO: this is hacky; need a separate setting of the returned granularity
        # since the server may decide so...
        if "returned_granularity" in r_m:
            r_m.update({"returned_granularity": self.returned_granularity})

        service_meta = self.service_config.get("meta", {})
        for rrs_k, rrs_v in service_meta.items():
            r_m.update({rrs_k: rrs_v})

        return


    # -------------------------------------------------------------------------#

    def __meta_add_received_request_summary_parameters(self):
        r_m = self.data_response.get("meta", {})
        if not "received_request_summary" in r_m:
            return

        r_r_s = r_m["received_request_summary"]
        r_r_s.update({
            "requested_schemas": [self.beacon_schema]
        })
        if BYC["TEST_MODE"] is True:
            r_r_s.update({"test_mode": BYC["TEST_MODE"]})

        for name in ["dataset_ids"]:
            value = self.byc.get(name)
            if not value:
                continue
            r_r_s.update({name: value})

        vargs = self.byc.get("varguments", [])
        # TODO: a bit hacky; len == 1 woulld be the default assemblyId ...
        if len(vargs) > 1:
            r_r_s.update({"request_parameters":{"g_variant":vargs}})

        fs = self.byc.get("filters", [])
        fs_p = []
        if len(fs) > 0:
            for f in fs:
                fs_p.append(f.get("id"))
            r_r_s.update({"filters":fs_p})
        else:
            r_r_s.pop("filters", None)

        for p in ["requested_granularity"]:
            if p in BYC_PARS and p in r_r_s:
                r_r_s.update({p: BYC_PARS.get(p)})

        for q in ["collation_types"]:
            if q in BYC_PARS:
                r_r_s.update({"request_parameters": always_merger.merge( r_r_s.get("request_parameters", {}), { "collation_types": BYC_PARS.get(q) })})

        info = self.entity_defaults["info"].get("content", {"api_version": "___none___"})
        for p in ["api_version"]:
            if p in info.keys():
                r_r_s.update({p: info.get(p, "___none___")})

        return


   # -------------------------------------------------------------------------#

    def __service_response_update_summaries(self):
        if not "response" in self.data_response:
            return
        c_r = self.data_response["response"].get("results", [])
        t_count = len(c_r)

        t_exists = True if t_count > 0 else False

        self.data_response.update({
            "response_summary": {
                "num_total_results": t_count,
                "exists": t_exists
            }
        })

        return

    # -------------------------------------------------------------------------#

    def __serviceResponse_force_granularities(self):
        if not "record" in self.returned_granularity:
            self.data_response["response"].pop("results", None)
        if "boolean" in self.returned_granularity:
            self.data_response["response_summary"].pop("num_total_results", None)
            self.data_response.pop("response", None)


################################################################################
################################################################################
################################################################################

class ByconCollations:
    def __init__(self, byc: dict):
        self.byc = byc
        self.delivery_method = BYC_PARS.get("method", "___none___")
        self.output = BYC_PARS.get("output", "___none___")
        self.dataset_ids = byc.get("dataset_ids", [])
        self.beacon_defaults = byc.get("beacon_defaults", {})
        self.service_config = byc.get("service_config", {})
        self.entity_defaults = self.beacon_defaults.get("entity_defaults", {"info":{}})
        self.filter_definitions = byc.get("filter_definitions", {})
        self.form_data = byc.get("form_data", {})
        self.filters = byc.get("filters", [])
        self.response_entity_id = byc.get("response_entity_id", "filteringTerm")
        self.path_id_value = byc.get("request_entity_path_id_value", False)
        self.filter_collation_types = set()
        self.collations = []

        return

    # -------------------------------------------------------------------------#
    # ----------------------------- public ------------------------------------#
    # -------------------------------------------------------------------------#

    def populatedCollations(self):
        self.__return_collations()
        return self.collations

    # -------------------------------------------------------------------------#
    # ----------------------------- private -----------------------------------#
    # -------------------------------------------------------------------------#

    def __return_collations(self):
        f_coll = "collations"
        d_k = set_selected_delivery_keys(self.service_config.get("method_keys"))

        c_id = self.form_data.get("id", "")
        # TODO: This should be derived from some entity definitions
        # TODO: whole query generation in separate function ...
        query = {}

        if BYC["TEST_MODE"] is True:
            t_m_c = self.form_data.get("test_mode_count", 5)
            query = mongo_test_mode_query(self.dataset_ids[0], f_coll, t_m_c)
        elif len(c_id) > 0:
            query = { "id": c_id }
        else:

            q_list = []
            ft_fs = []
            for f in self.filters:
                ft_fs.append('(' + f.get("id", "___none___") + ')')
            if len(ft_fs) > 0:
                f_s = '|'.join(ft_fs)
                f_re = re.compile(r'^' + '|'.join(ft_fs))
            else:
                f_re = None

            if f_re is not None:
                q_list.append({"id": { "$regex": f_re}})

            q_types = self.form_data.get("collation_types", [])
            if len(q_types) > 0:
                q_list.append({"collation_type": {"$in": q_types }})

            if len(q_list) == 1:
                query = q_list[0]
            elif len(q_list) > 1:
                query = {"$and": q_list}
        
        # TODO
        # if not query:
        #     warning = 'No limit (filters, collationTypes, id) on collation listing -> abortin...'

        s_s = { }

        for ds_id in self.dataset_ids:
            fields = {"_id": 0}
            f_s = mongo_result_list(ds_id, f_coll, query, fields)
            for f in f_s:
                if "codematches" in str(self.delivery_method):
                    if int(f.get("code_matches", 0)) < 1:
                        continue

                i_d = f.get("id", "NA")
                if i_d not in s_s:
                    s_s[ i_d ] = { }

                if len(d_k) < 1:
                    d_k = list(f.keys())
                    
                for k in d_k:
                    if k in self.service_config.get("integer_keys", []):
                        s_s[ i_d ].update({k: s_s[ i_d ].get(k, 0) + f.get(k, 0)})
                    elif k == "name":
                        s_s[ i_d ][ "type" ] = f.get(k)
                    else:
                        s_s[ i_d ][ k ] = f.get(k)
        
        for k, v in s_s.items():
            self.collations.append(v)

        return


################################################################################
################################################################################
################################################################################

