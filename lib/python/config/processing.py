import os.path

import config.basic
################################################################
# Configurations for processing
################################################################
base_working_directory = "/exports/scratch/PALFA/"
zaplistdir = os.path.join(config.basic.pipelinedir, "lib", "zaplists")
default_zaplist = os.path.join(zaplistdir, "PALFA.zaplist")

import processing_check
processing_check.processing.populate_configs(locals())
processing_check.processing.check_sanity()
