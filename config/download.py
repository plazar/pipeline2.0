################################################################
# Downloader Configuration
################################################################
api_service_url = "http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx?WSDL"
api_username = "mcgill"
api_password = "palfa@Mc61!!"
temp = "/data/alfa/test_pipeline_clean/"
space_to_use = 228748364800
numdownloads = 2
numrestores = 2
numretries = 3

import download_check
download_check.download.populate_configs(locals())
download_check.download.check_sanity()
