import os.path
import config.basic
################################################################
# Configurations for processing
################################################################
# The following is the name of the scratch working directory
# basename on the individual processing nodes
base_working_directory = "/exports/scratch/PALFA/"

# Should not need to change the names of the zaplists...
zaplistdir = os.path.join(config.basic.pipelinedir, "lib", "zaplists")
default_zaplist = os.path.join(zaplistdir, "PALFA.zaplist")

import processing_check
processing_check.processing.populate_configs(locals())
processing_check.processing.check_sanity()
