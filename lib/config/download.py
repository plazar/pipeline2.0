################################################################
# Downloader Configuration
################################################################
api_service_url = "http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx?WSDL"
api_username = "mcgill"
api_password = "palfa@Mc61!!"
ftp_host = "arecibo.tc.cornell.edu"
ftp_port = 31001
ftp_username = "palfadata"
ftp_password = "NAIC305m"
temp = "/data/alfa/pipeline_test_downloads"
space_to_use = 228748364800
numdownloads = 2
numrestores = 2
numretries = 3
log_file_path = "downloader.log"

import download_check
download_check.download.populate_configs(locals())
download_check.download.check_sanity()