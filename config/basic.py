institution = 'McGill'
pipeline = "PRESTO"
survey = "PALFA2.0"
pipelinedir = "/homes/borgii/plazar/research/PALFA/pipeline2.0_clean/pipeline2.0"

import basic_check
basic_check.basic.populate_configs(locals())
basic_check.basic.check_sanity()

