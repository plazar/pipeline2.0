import os.path

# All of the following 3 are simple strings with no
# pre-defined options
institution = 'McGill'
pipeline = "PRESTO"
survey = "PALFA2.0"

# This is the root directory of the source for the pipeline as pulled
# from github
pipelinedir = "/homes/borgii/plazar/research/PALFA/pipeline2.0_clean/pipeline2.0"

# This is the root directory of the source for the psrfits_utils code
# as pulled from github
psrfits_utilsdir = "/usr/local/src/psrfits_utils_git/psrfits_utils"

# A boolean value that determines if raw data is deleted when results
# for a job are successfully uploaded to the common DB, or if the
# maximum number of attempts for a job is reached.
delete_rawdata = True

# Should not need to change this unless you rearrange the pipeline filesystem
coords_table = os.path.join(pipelinedir, "lib", "PALFA_coords_table.txt")
log_dir = os.path.join(pipelinedir, "logs")
qsublog_dir = os.path.join(log_dir, "qsublog")

import basic_check
basic_check.basic.populate_configs(locals())
basic_check.basic.check_sanity()

