#!/usr/bin/env python3

import sys
import cgi, re
from os import environ, path
from uuid import uuid4

from bycon import *

dir_path = path.dirname( path.abspath(__file__) )

################################################################################
################################################################################
################################################################################

def main():

    try:
        uploader()
    except Exception:
        print_text_response(traceback.format_exc(), byc["env"], 302)

################################################################################
################################################################################
################################################################################

def uploader():

    initialize_bycon_service(byc, "uploader")
    file_id = str(uuid4())
    form = cgi.FieldStorage()

    response = {
        "error": {},
        "rel_path": "{}/{}".format(byc["local_paths"].get("server_tmp_dir_web", "/tmp"), file_id),
        "loc_path": path.join( *byc["local_paths"][ "server_tmp_dir_loc" ], file_id ),
        "file_id": file_id,
        "plot_link": '/services/samplePlots/?fileId='+file_id,
        "host": "http://"+str(environ.get('HTTP_HOST'))
    }

    if not "upload_file" in form:
        response.update({"error": "ERROR: No `upload_file` parameter in POST..." })
        print_json_response(response)

    file_item = form['upload_file']
    file_name = path.basename(file_item.filename)
    file_type = file_name.split('.')[-1]
    data = file_item.file.read()

    response.update({
        "file_name": file_name,
        "file_type": file_type
    })

    with open(response["loc_path"], 'wb') as f:
        f.write(data)

    print_json_response(response)

################################################################################
################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    main()
