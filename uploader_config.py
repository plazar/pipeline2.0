################################################################
# Result Uploader Configuration
#
# 
################################################################
uploader_result_dir_overide = True
uploader_result_dir = "/data/data7/PALFA/test_new_pipeline/"
uploader_version_num = 'PRESTO:56b00442679f3c3edc36cbe322b2022eca53e459;PIPELINE:8512aac773bc1414f1dddd93397ca4aa5bb693a2'

import sanity_check
sanity = sanity_check.SanityCheck()
sanity.run(__name__)