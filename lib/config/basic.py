institution = 'McGill'
pipeline = "PRESTO"
survey = "PALFA2.0"
pipelinedir = "/home/snip3/dev/pythonapps/pipeline2.0"
delete_rawdata = True
coords_table = "/home/snip3/dev/pythonapps/pipeline2.0/PALFA_coords_table.txt"

import basic_check
basic_check.basic.populate_configs(locals())
basic_check.basic.check_sanity()

