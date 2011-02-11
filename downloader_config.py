################################################################
# Downloader Configuration
#
# Downlaoder uses 'rawdata_directory' to move downlaoded files to
################################################################

downloader_api_service_url = "http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx?WSDL"
downloader_api_username = "mcgill"
downloader_api_password = "palfa@Mc61!!"
#downloader_temp = "/data/alfa/test_pipeline" # When set to empty string will download to directory of the script
downloader_temp = "/data/alfa/test_pipeline_clean/" # When set to empty string will download to directory of the script
downloader_space_to_use = 228748364800#214748364800 #Size to use in bytes; Use 'None' to use all available space
downloader_numofdownloads = 2
downloader_numofrestores = 2
downloader_numofretries = 3

import sanity_check
sanity = sanity_check.SanityCheck()
sanity.run(__name__)