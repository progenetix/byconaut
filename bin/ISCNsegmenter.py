#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir
import sys, datetime

from bycon import *

services_lib_path = path.join( path.dirname( path.abspath(__file__) ), pardir, "services", "lib" )
sys.path.append( services_lib_path )
from file_utils import read_tsv_to_dictlist

"""
bin/ISCNsegmenter.py -i imports/ccghtest.tab -o exports/cghvars.tsv
bin/ISCNsegmenter.py -i imports/progenetix-from-filemaker-ISCN-samples-cCGH.tsv -o exports/progenetix-from-filemaker-ISCN-samples-cCGH-icdom-grouping.pgxseg -g icdo_topography_id
"""

################################################################################
################################################################################
################################################################################

def main():

	iscn_segmenter()

################################################################################

def iscn_segmenter():

	initialize_bycon_service(byc)
	set_processing_modes(byc)
	parse_variants(byc)
	generate_genomic_mappings(byc)

	group_parameter = "histological_diagnosis_id"

	if byc["args"].groupBy:
		group_parameter = byc["args"].groupBy

	technique = "cCGH"
	iscn_field = "iscn_ccgh"
	platform_id = "EFO:0010937"
	platform_label = "comparative genomic hybridization (CGH)"
	
	if not byc["args"].inputfile:
		print("No input file file specified (-i, --inputfile) => quitting ...")
		exit()
	inputfile = byc["args"].inputfile

	if not byc["args"].outputfile:
		outputfile = path.splitext(inputfile)[0]
		outputfile += "_processed"
		print("""¡¡¡ No output file has been specified (-o, --outputfile) !!!
Output will be written to {}""".format(outputfile) )
	else:
		outputfile = path.splitext(byc["args"].outputfile)[0]

	if byc["test_mode"] is True:
		outputfile += "_test"

	outputfile += ".pgxseg"

	iscn_samples, fieldnames = read_tsv_to_dictlist(inputfile, int(byc["args"].limit))

	if not iscn_field in fieldnames:
		print('The samplefile header does not contain the "{}" column => quitting'.format(iscn_field))
		exit()
	if not group_parameter in fieldnames:
		print('The samplefile header does not contain the provided "{}" `group_by` parameter\n    => continuing but be ¡¡¡WARNED!!!'.format(group_parameter))

	iscn_no = len(iscn_samples)
	s_w_v_no = 0
	print("=> The samplefile contains {} samples".format(iscn_no))

	pgxseg = open(outputfile, "w")
	pgxseg.write( "#meta=>biosample_count={}\n".format(iscn_no) )

	for c, s in enumerate(iscn_samples):

		n = str(c+1)
		update_bs ={
			"id": s.get("biosample_id", "sample-"+n),
			"callset_id": s.get("callset_id", "exp-"+n),
			"individual_id": s.get("individual_id", "ind-"+n),
		}
		update_bs = import_datatable_dict_line(byc, update_bs, fieldnames, s, "biosample")
		h_line = pgxseg_biosample_meta_line(byc, update_bs, group_parameter)
		pgxseg.write( "{}\n".format(h_line) )

	pgxseg.write( "{}\n".format(pgxseg_header_line()) )

	for c, s in enumerate(iscn_samples):

		n = str(c+1)
		bs_id = s.get("biosample_id", "sample-"+n)
		cs_id = s.get("callset_id", "exp-"+n)

		variants, v_e = variants_from_revish(bs_id, cs_id, technique, s[iscn_field], byc)

		if len(variants) > 0:
			s_w_v_no += 1

			v_instances = list(sorted(variants, key=lambda x: (f'{x["reference_name"].replace("X", "XX").replace("Y", "YY").zfill(2)}', x['start'])))
		
			for v in v_instances:
				pgxseg.write(pgxseg_variant_line(v)+"\n")

	print("=> {} samples had variants".format(s_w_v_no))
	print("Wrote to {}".format(outputfile))

	exit()

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
	main()
