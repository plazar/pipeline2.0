import os.path
import config.basic
################################################################
# Downloader Configuration
################################################################
api_service_url = "http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx?WSDL"
api_username = "username"
api_password = "password"
ftp_host = "arecibo.tc.cornell.edu"
ftp_port = 31001
ftp_username = "username"
ftp_password = "password"
temp = "/data/alfa/test_pipeline_clean/"
space_to_use = 228748364800
numdownloads = 2
numrestores = 2
numretries = 3
log_file_path = os.path.join(config.basic.pipelinedir, "logs", "downloader.log")
request_timeout = 1 # Integer number of hours before an un-restored request 
                    # is marked as failed

import download_check
download_check.download.populate_configs(locals())
download_check.download.check_sanity()
