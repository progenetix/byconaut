#!/usr/bin/env python3

# version: 2023-11-23
import yaml
import sys, re, getpass
from os import path, system

dir_path = path.dirname( path.abspath(__file__) )

from bycon import *

"""
The install script copies the relevant services files to the webserver directory
specified in the `install.yaml` file and sets the file permissions
accordingly. By default, it requires admin permissions (sudo). If you want to run
it without sudo, invoke it with `--no-sudo`. The current use will need to be able
to write into the target directories. 
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

    i_f = path.join( dir_path, "install.yaml" )
    try:
        with open( i_f ) as y_c:
            install = yaml.load( y_c , Loader=yaml.FullLoader)
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
    # in case this is a standard path w/ a username ...
    b_s_d_p = re.sub("__USERNAME__", getpass.getuser(), b_s_d_p)

    b_i_d_p = path.join( *install["bycon_install_dir"] )
    w_t_d_p = path.join( *install["bycon_instance_pars"]["server_tmp_dir_loc"] )
    s_s_d = path.join(dir_path, "services", "")
    s_i_d = path.join(b_i_d_p, "services", "")
    s_c_d = path.join(dir_path, "local", "")

    for s_p in [b_i_d_p, w_t_d_p]:
        if not path.isdir(s_p):
            print("¡¡¡ {} does not exist - please check & create !!!".format(s_p))
            exit()
    
    # pulling local definition from the bycon dir into the local definitions
    # source; this might be commented to avoid clashes
    # ¡¡¡ Do not use `--delete` here to keep configs which do not exist in `bycon`
    system(f'{sudo_cmd} rsync -avh {path.join(b_s_d_p, "local", "")} {s_c_d}')

    # adding the local configs to the directories with the exacutables
    for bin_dir in ["bin", "services"]:
        system(f'{sudo_cmd} rsync -avh {s_c_d} {path.join(dir_path, bin_dir, "local", "")}')

    # copying the services dir to the server cgi directory
    system(f'{sudo_cmd} rsync -avh {s_s_d} {s_i_d}')
    system(f'{sudo_cmd} chown -R {s_u}:{s_g} {s_i_d}')
    system(f'{sudo_cmd} chmod 775 {s_i_d}*.py')

    # making sure the tmp dir has the right permissions (existence checked above)
    system(f'{sudo_cmd} chmod -R 1777 {w_t_d_p}')

################################################################################
################################################################################
################################################################################

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == "--no-sudo":
        no_sudo = True
    else:
        no_sudo = False

    main(no_sudo)
