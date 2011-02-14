################################################################
# Configurations for processing
################################################################
base_working_directory = "/exports/scratch/PALFA/"
default_zaplist = "/homes/borgii/plazar/research/PALFA/pipeline2.0/lib/zaplists/PALFA.zaplist"
zaplistdir = "/homes/borgii/plazar/research/PALFA/pipeline2.0/lib/zaplists/"

import processing_check
processing_check.processing.populate_configs(locals())
processing_check.processing.check_sanity()
