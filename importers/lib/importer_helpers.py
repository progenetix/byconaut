import datetime
from os import path
from progress.bar import Bar

# bycon
from config import *
from bycon_helpers import prjsonnice, prdbug

loc_path = path.dirname( path.abspath(__file__) )
services_lib_path = path.join( loc_path, pardir, pardir, "services", "lib" )
sys.path.append( services_lib_path )
from bycon_bundler import ByconBundler
from datatable_utils import import_datatable_dict_line
from file_utils import write_log
from service_helpers import ask_limit_reset

class ByconautImporter():
    def __init__(self):
        self.log = []
        self.entity = None
        self.dataset_id = BYC["BYC_DATASET_IDS"][0]
        self.input_file = None
        self.import_collname = None
        self.import_entity = None
        self.import_id = None
        self.import_upstream = ["individuals", "biosamples", "analyses"]
        self.delete_downstream = []
        self.mongo_client = MongoClient(host=DB_MONGOHOST)
        self.ind_coll = mongo_client[ self.dataset_id ]["individuals"]
        self.bios_coll = mongo_client[ self.dataset_id ]["biosamples"]
        self.ana_coll = mongo_client[ self.dataset_id ]["analyses"]

        self.__initialize_importer()


    #--------------------------------------------------------------------------#
    #----------------------------- public -------------------------------------#
    #--------------------------------------------------------------------------#

    def get_dataset_id(self):
        return self.dataset_id


    #--------------------------------------------------------------------------#

    def get_input_file(self):
        return self.input_file


    #--------------------------------------------------------------------------#

    def import_individuals(self):
        self.__prepare_individuals()
        self.__insert_database_records_from_file()


    #--------------------------------------------------------------------------#

    def update_individuals(self):
        self.__prepare_individuals()
        self.__update_database_records_from_file()


    #--------------------------------------------------------------------------#

    def import_biosamples(self):
        self.__prepare_biosamples()
        self.__insert_database_records_from_file()


    #--------------------------------------------------------------------------#

    def update_biosamples(self):
        self.__prepare_biosamples()
        self.__update_database_records_from_file()


    #--------------------------------------------------------------------------#

    def import_analyses(self):
        self.__prepare_analyses()
        self.__insert_database_records_from_file()


    #--------------------------------------------------------------------------#

    def update_analyses(self):
        self.__prepare_analyses()
        self.__update_database_records_from_file()


    #--------------------------------------------------------------------------#

    def delete_individuals(self):
        self.__prepare_individuals()
        self.__delete_database_records()


    #--------------------------------------------------------------------------#

    def delete_individuals_and_downstream(self):
        self.__prepare_individuals()
        self.delete_downstream = ["biosamples", "analyses", "variants"]
        self.__delete_database_records()


    #--------------------------------------------------------------------------#

    def delete_biosamples(self):
        self.__prepare_biosamples()
        self.__delete_database_records()


    #--------------------------------------------------------------------------#

    def delete_biosamples_and_downstream(self):
        self.__prepare_biosamples()
        self.delete_downstream = ["analyses", "variants"]
        self.__delete_database_records()


    #--------------------------------------------------------------------------#

    def delete_analyses(self):
        self.__prepare_analyses()
        self.__delete_database_records()


    #--------------------------------------------------------------------------#

    def delete_analyses_and_downstream(self):
        self.__prepare_analyses()
        self.delete_downstream = ["variants"]
        self.__delete_database_records()


    #--------------------------------------------------------------------------#
    #----------------------------- Private ------------------------------------#
    #--------------------------------------------------------------------------#

    def __initialize_importer(self):
        BYC.update({"BYC_DATASET_IDS": BYC_PARS.get("dataset_ids", [])})
        if len(BYC["BYC_DATASET_IDS"]) != 1:
            print("No single existing dataset was provided with -d ...")
            exit()
        self.dataset_id = BYC["BYC_DATASET_IDS"][0]
        self.input_file = BYC_PARS.get("inputfile")
        if not self.input_file:
            print("No input file file specified (-i, --inputfile) => quitting ...")
            exit()
        if not BYC["TEST_MODE"]:
            tmi = input("Do you want to run in TEST MODE (i.e. no database insertions/updates)?\n(Y|n): ")
            if not "n" in tmi.lower():
                BYC.update({"TEST_MODE": True})
        if BYC["TEST_MODE"]:
                print("... running in TEST MODE")

        return


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __prepare_individuals(self):
        # TODO: have those parameters read from bycon globals
        self.import_collname = "individuals"
        self.import_entity = "individual"
        self.import_id = "individual_id"
        self.import_upstream = []
        self.__check_dataset()
        self.__read_data_file()


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __prepare_biosamples(self):
        # TODO: have those parameters read from bycon globals
        self.import_collname = "biosamples"
        self.import_entity = "biosample"
        self.import_id = "biosample_id"
        self.import_upstream = ["individuals"]
        self.__check_dataset()
        self.__read_data_file()


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __prepare_analyses(self):
        # TODO: have those parameters read from bycon globals
        self.import_collname = "analyses"
        self.import_entity = "analysis"
        self.import_id = "analysis_id"
        self.import_upstream = ["individuals", "biosamples"]
        self.__check_dataset()
        self.__read_data_file()


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __check_dataset(self):
        # done after assignment of import_collname
        if self.dataset_id not in BYC.get("DATABASE_NAMES",[]):
            print(f'Dataset {self.dataset_id} does not exist. You have to create it first.')
            if "individuals" in str(self.import_collname):
                proceed = input(f'Please type the name of the dataset and hit Enter: ')
                if not str(self.dataset_id) in proceed:
                    exit()
            else:
                exit()


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __read_data_file(self):
        iid = self.import_id

        bb = ByconBundler()
        self.data_in = bb.read_pgx_file(self.input_file)
        print(f'=> The input file contains {len(self.data_in.data)} items')

        self.import_docs = []

        import_ids = []
        l_no = 0
        for new_doc in self.data_in.data:
            l_no += 1
            if not (import_id_v := new_doc.get(iid)):
                self.log.append(f'¡¡¡ no {iid} value in entry {l_no} => skipping !!!')
                continue
            if import_id_v in import_ids:
                self.log.append(f'¡¡¡ duplicated {iid} value in entry {l_no} => skipping !!!')
                continue

            import_ids.append(import_id_v)
            self.import_docs.append(dict(new_doc))


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __delete_database_records(self):
        ds_id = self.dataset_id
        icn = self.import_collname
        dcs = self.delete_downstream
        iid = self.import_id
        fn = self.data_in.fieldnames

        del_coll = self.mongo_client[ds_id][icn]

        #----------------------- Checking database content --------------------#

        del_ids = []
        for test_doc in self.import_docs:
            del_id_v = test_doc[iid]
            if not del_coll.find_one({"id": del_id_v}):
                self.log.append(f'id {del_id_v} does not exist in {ds_id}.{icn} => maybe deleted already ...')
            del_ids.append(del_id_v)

        self.__parse_log()

        #---------------------------- Delete Stage ----------------------------#

        del_nos = {
            icn: 0
        }
        bar = Bar("Deleting ", max = len(del_ids), suffix='%(percent)d%%'+f' of {str(len(del_ids))} {icn}' ) if not BYC["TEST_MODE"] else False
        for del_id in del_ids:
            d_c = del_coll.count_documents({"id": del_id})
            del_nos[icn] += d_c
            if not BYC["TEST_MODE"]:
                del_coll.delete_many({"id": del_id})
                bar.next()
        if not BYC["TEST_MODE"]:
            bar.finish()

        for c in dcs:
            bar = Bar(f'Deleting {c} for ', max = len(del_ids), suffix='%(percent)d%%'+f' of {str(len(del_ids))} {icn}' ) if not BYC["TEST_MODE"] else False
            del_coll = self.mongo_client[ds_id][c]
            del_nos.update({c: 0})
            for del_id in del_ids:
                d_c = del_coll.count_documents({iid: del_id})
                del_nos[c] += d_c
                if not BYC["TEST_MODE"]:
                    del_coll.delete_many({iid: del_id})
                    bar.next()
            if not BYC["TEST_MODE"]:
                bar.finish()

        if not BYC["TEST_MODE"]:
            for k, v in del_nos.items():
                print(f'==> deleted {v} {k}')
        else:
            for k, v in del_nos.items():
                print(f'==> would have deleted {v} {k}')
               

    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __update_database_records_from_file(self):
        ds_id = self.dataset_id
        icn = self.import_collname 
        ien = self.import_entity
        iid = self.import_id
        fn = self.data_in.fieldnames
        
        import_coll = self.mongo_client[ ds_id ][icn]

        #----------------------- Checking database content --------------------# 

        checked_docs = []
        for test_doc in self.import_docs:
            import_id_v = test_doc[iid]
            self.__check_upstream_ids(test_doc)
            if not import_coll.find_one({"id": import_id_v}):
                self.log.append(f'id {import_id_v} does not exist in {ds_id}.{icn} => you might need to run an importer ...')
                continue
            checked_docs.append(test_doc)

        self.__parse_log()

        #---------------------- Updating checked records-----------------------# 

        bar = Bar("Processing ", max = len(checked_docs), suffix='%(percent)d%%'+" of "+str(len(checked_docs)) ) if not BYC["TEST_MODE"] else False

        #---------------------------- Import Stage ----------------------------# 

        i_no = 0
        for new_doc in checked_docs:
            o_id = new_doc[iid]
            update_i = import_coll.find_one({"id": o_id})
            update_i = import_datatable_dict_line(update_i, fn, new_doc, ien)
            update_i.update({"updated": datetime.datetime.now().isoformat()})

            if not BYC["TEST_MODE"]:
                import_coll.update_one({"id": o_id}, {"$set": update_i})
                bar.next()
            else:
                prjsonnice(update_i)
            i_no += 1

        #---------------------------- Summary ---------------------------------#

        if not BYC["TEST_MODE"]:
            bar.finish()
            print(f'==> updated {i_no} {ien}')
        else:
            print(f'==> tested {i_no} {ien}')


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __insert_database_records_from_file(self):
        ds_id = self.dataset_id
        icn = self.import_collname 
        ien = self.import_entity
        iid = self.import_id
        fn = self.data_in.fieldnames

        import_coll = self.mongo_client[ ds_id ][icn]

        #----------------------- Checking database content --------------------# 

        checked_docs = []
        for test_doc in self.import_docs:
            import_id_v = test_doc[iid]
            self.__check_upstream_ids(test_doc)
            if import_coll.find_one({"id": import_id_v}):
                self.log.append(f'existing id {import_id_v} in {ds_id}.{icn} => please check or remove')
                continue
            checked_docs.append(test_doc)

        self.__parse_log()

        #---------------------------- Import Stage ----------------------------# 

        i_no = 0
        for new_doc in checked_docs:
            update_i = {"id": new_doc[iid]}
            update_i = import_datatable_dict_line(update_i, fn, new_doc, ien)
            update_i.update({"updated": datetime.datetime.now().isoformat()})

            if not BYC["TEST_MODE"]:
                import_coll.insert_one(update_i)
                i_no += 1
            else:
                prjsonnice(update_i)

        #-------------------------------- Summary -----------------------------#

        print(f'=> {i_no} records were inserted into {ds_id}.{icn}')

    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __check_upstream_ids(self, new_doc):
        ind_id = new_doc.get("individual_id", "___none___")
        bios_id = new_doc.get("biosample_id", "___none___")
        ana_id = new_doc.get("analysis_id", "___none___")
        if "individuals" in self.import_upstream:
            if not self.ind_coll.find_one({"id": ind_id}):
                self.log.append(f'individual {ind_id} for {ien} {import_id_v} should exist before {ien} import')
        if "biosamples" in self.import_upstream:
            if not self.bios_coll.find_one({"id": bios_id}):
                self.log.append(f'biosample {bios_id} for {ien} {import_id_v} should exist before {ien} import')
        if "analyses" in self.import_upstream:
            if not self.ana_coll.find_one({"id": ana_id}):
                self.log.append(f'analysis {ana_id} for {ien} {import_id_v} should exist before {ien} import')


    #--------------------------------------------------------------------------#
    #--------------------------------------------------------------------------#

    def __parse_log(self):
        if len(self.log) < 1:
            return

        write_log(self.log, self.input_file)
        if (force := BYC_PARS.get("force")):
            print(f'¡¡¡ {len(self.log)} errors => still proceeding since"--force {force}" in effect')
        else:
            print(f'¡¡¡ {len(self.log)} errors => quitting w/o data changes\n... override with "--force true"')
            exit()

