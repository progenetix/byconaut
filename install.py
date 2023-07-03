#!/usr/bin/env python3

# version: 2023-06-22

import sys, re, ruamel.yaml
from os import getlogin, path, system

dir_path = path.dirname( path.abspath(__file__) )

from bycon import *

"""
The install script copies the relevant services files to the webserver directory
specified in the `install.yaml` file and sets the file permissions
accordingly. It requires admin permissions ({sudo_cmd}).
"""

################################################################################
################################################################################
################################################################################

def main(no_sudo):

    install_services(no_sudo)

################################################################################
################################################################################
################################################################################

def install_services(no_sudo):
    if no_sudo:
        sudo_cmd = ""
    else:
        sudo_cmd = "sudo"

    yaml = ruamel.yaml.YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)

    i_f = path.join( dir_path, "install.yaml" )
    try:
        with open( i_f ) as y_c:
            install = yaml.load( y_c )
    except Exception as e:
        print(e)
        exit()

    # WARNING: This assumes that the values are sensible...
    for p in ["system_user", "system_group", "bycon_install_dir", "bycon_instance_pars"]:
        p_v = install.get(p, None)
        if p_v is None:
            print("¡¡¡ No `{}` value defined in {} !!!".format(p, i_f))
            exit()

    s_u = install["system_user"]
    s_g = install["system_group"]

    for p in ["server_tmp_dir_loc", "server_tmp_dir_web"]:
        p_v = install["bycon_instance_pars"].get(p, None)
        if p_v is None:
            print("¡¡¡ No `bycon_instance_pars.{}` value defined in {} !!!".format(p, i_f))
            exit()

    b_s_d_p = path.join( *install["bycon_source_dir"] )
    b_s_d_p = re.sub("__USERNAME__", getlogin(), b_s_d_p)
    b_i_d_p = path.join( *install["bycon_install_dir"] )
    w_t_d_p = path.join( *install["bycon_instance_pars"]["server_tmp_dir_loc"] )

    for s_p in [b_i_d_p, w_t_d_p]:
        if not path.isdir(s_p):
            print("¡¡¡ {} does not exist - please check & create !!!".format(s_p))
            exit()
    
    system(f'{sudo_cmd} rsync -avh {dir_path}/services/ {b_i_d_p}/services/')
    system(f'{sudo_cmd} rsync -avh {b_s_d_p}/local/ {dir_path}/local/')
    system(f'{sudo_cmd} rsync -avh {dir_path}/local/ {b_i_d_p}/services/local/')

    system(f'{sudo_cmd} chown -R {s_u}:{s_g} {b_i_d_p}')
    system(f'{sudo_cmd} chmod 775 {b_i_d_p}/services/*.py')

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == "--no-sudo":
        no_sudo = True
    else:
        no_sudo = False

    main(no_sudo)
