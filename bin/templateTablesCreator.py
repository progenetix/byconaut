#!/usr/bin/env python3

from os import path, pardir

from bycon import *

dir_path = path.dirname( path.relpath(__file__) )
pkg_path = path.join( dir_path, pardir )

services_lib_path = path.join( pkg_path, "services", "lib" )
sys.path.append( services_lib_path )

"""
This script uses the `datatable_definitions.yaml` from `bycon` tpo generate import
tables for the different entities (and a general `metadata_template.tsv` for all
non-variant parameters) in the local `rsrc/templates/` directory.
"""

################################################################################
################################################################################
################################################################################

def main():

    templates_creator()

################################################################################

def templates_creator():
    initialize_bycon_service(byc, "templates_creator")
    dt_m = byc["datatable_mappings"].get("definitions", {})
    rsrc_p = path.join(pkg_path, "rsrc", "templates")

    all_cols = []

    for t_t, t_d in dt_m.items():
        entity_cols = []
        for p_n, p_d in t_d["parameters"].items():
            p_t = p_d.get("type", "string")
            # print(f'{t_t}: {p_n} ({p_t})')
            prefs = p_d.get("prefix_split")
            if prefs:
                for p in prefs:
                    for t in ("id", "label"):
                        h = f'{p_n}_{t}___{p}'
                        entity_cols.append(h)
                        if "variant" not in t_t.lower() and h not in all_cols:
                            all_cols.append(h)
            else:
                entity_cols.append(p_n)
                if "variant" not in t_t.lower() and p_n not in all_cols:
                    all_cols.append(p_n)

        f_p = path.join(rsrc_p, t_t+"_template.tsv")
        f = open(f_p, "w")
        f.write("\t".join(entity_cols)+"\n")
        f.close()
        print(f'===> Wrote {f_p}')

    f_p = path.join(rsrc_p, "metadata_template.tsv")
    f = open(f_p, "w")
    f.write("\t".join(all_cols)+"\n")
    f.close()
    print(f'===> Wrote {f_p}')

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
