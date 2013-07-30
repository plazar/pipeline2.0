import os.path
import config.basic
################################################################
# Configurations for processing
################################################################
# This is where all of the output files are stored
# Must be writable and have lots of free space...
base_results_directory = "/data/data7/PALFA/test_new_pipeline_clean/"
# The following is the name of the scratch working directory
# basename on the individual processing nodes
base_working_directory = "/exports/scratch/PALFA/"
# The following is the path where the temporary working directory 
# should be created. This could be /dev/shm, or simply another 
# directory on the worker node.
base_tmp_dir = "/dev/shm/"

# set to True to use subdirectory with name $PBS_JOBID in
# base_working_directory and base_tmp_dir (dir has to exist)
use_pbs_subdir = False

# Should not need to change the names of the zaplists...
zaplistdir = os.path.join(config.basic.pipelinedir, "lib", "zaplists")
default_wapp_zaplist = os.path.join(zaplistdir, "PALFA_commondb_wapp.zaplist")
default_mock_zaplist = os.path.join(zaplistdir, "PALFA_commondb_Mock.zaplist")

# Set following to False to only process data that has dynamic zaplists
use_default_zaplists = True 

# The following don't currently get used. They are placeholders.
num_cores = 1 # The number of cores to use/request for each job
use_hyperthreading = False # Whether or not to use HyperThreading

import processing_check
processing_check.processing.populate_configs(locals())
processing_check.processing.check_sanity()
