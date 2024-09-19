#!/usr/bin/env python3

import re, json, yaml
from os import path, environ, pardir
import sys, datetime

from bycon import *
from bycon.services import cytoband_utils, datatable_utils, export_file_generation, interval_utils

"""
bin/ISCNsegmenter.py -i imports/ccghtest.tab -o exports/cghvars.tsv
bin/ISCNsegmenter.py -i imports/progenetix-from-filemaker-ISCN-samples-cCGH.tsv -o exports/progenetix-from-filemaker-ISCN-samples-cCGH-icdom-grouping.pgxseg -g icdo_topography_id
"""

################################################################################
################################################################################
################################################################################

def main():
	initialize_bycon_service()
	interval_utils.generate_genome_bins()

	group_parameter = BYC_PARS.get("groupBy", "histological_diagnosis_id")
	input_file = BYC_PARS.get("inputfile")
	output_file = BYC_PARS.get("outputfile")

	technique = "cCGH"
	iscn_field = "iscn_ccgh"
	platform_id = "EFO:0010937"
	platform_label = "comparative genomic hybridization (CGH)"
	
	if not input_file:
		print("No input file file specified (-i, --inputfile) => quitting ...")
		exit()

	if not output_file:
		output_file = path.splitext(input_file)[0]
		output_file += "_processed"
		print("""¡¡¡ No output file has been specified (-o, --outputfile) !!!
Output will be written to {}""".format(output_file) )
	else:
		output_file = path.splitext(output_file)[0]

	if BYC["TEST_MODE"] is True:
		output_file += "_test"

	output_file += ".pgxseg"

	iscn_samples, fieldnames = file_utils.read_tsv_to_dictlist(input_file, int(BYC_PARS.get("limit", 0)))

	if not iscn_field in fieldnames:
		print('The samplefile header does not contain the "{}" column => quitting'.format(iscn_field))
		exit()
	if not group_parameter in fieldnames:
		print('The samplefile header does not contain the provided "{}" `group_by` parameter\n    => continuing but be ¡¡¡WARNED!!!'.format(group_parameter))

	data_no = len(iscn_samples)
	s_w_v_no = 0
	print(f'=> The input file contains {data_no} items')

	pgxseg = open(output_file, "w")
	pgxseg.write( "#meta=>biosample_count={}\n".format(iscn_no) )

	for c, s in enumerate(iscn_samples):

		n = str(c+1)
		update_bs ={
			"id": s.get("biosample_id", "sample-"+n),
			"analysis_id": s.get("analysis_id", "exp-"+n),
			"individual_id": s.get("individual_id", "ind-"+n),
		}
		update_bs = datatable_utils.import_datatable_dict_line(update_bs, fieldnames, s, "biosample")
		h_line = export_file_generation.pgxseg_biosample_meta_line(update_bs, group_parameter)
		pgxseg.write( "{}\n".format(h_line) )

	pgxseg.write( "{}\n".format(export_file_generation.pgxseg_header_line()) )

	for c, s in enumerate(iscn_samples):

		n = str(c+1)
		bs_id = s.get("biosample_id", "sample-"+n)
		cs_id = s.get("analysis_id", "exp-"+n)

		variants, v_e = cytoband_utils.variants_from_revish(bs_id, cs_id, technique, s[iscn_field])

		if len(variants) > 0:
			s_w_v_no += 1

			v_instances = list(sorted(variants, key=lambda x: (f'{x["reference_name"].replace("X", "XX").replace("Y", "YY").zfill(2)}', x['start'])))
		
			for v in v_instances:
				pgxseg.write(export_file_generation.pgxseg_variant_line(v)+"\n")

	print(f'=> {s_w_v_no} samples had variants')
	print(f'Wrote to {output_file}')

	exit()

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
	main()
