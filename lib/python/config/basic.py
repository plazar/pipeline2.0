import os.path

institution = 'McGill'
pipeline = "PRESTO"
survey = "PALFA2.0"
pipelinedir = "/homes/borgii/plazar/research/PALFA/pipeline2.0_clean/pipeline2.0"
psrfits_utilsdir = "/usr/local/src/psrfits_utils_git/psrfits_utils"
delete_rawdata = True
coords_table = os.path.join(pipelinedir, "lib", "PALFA_coords_table.txt")
log_dir = os.path.join(pipelinedir, "logs")

import basic_check
basic_check.basic.populate_configs(locals())
basic_check.basic.check_sanity()

