import os.path
import config.basic
################################################################
# Downloader Configuration
################################################################
# The url is from Adam and is unlikely to need changing
api_service_url = "http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx"
# These are from Adam and are different from the database ones...
api_username = "username"
api_password = "password"
# You should not need to change the port or host for FTP
ftp_host = "arecibo.tc.cornell.edu"
ftp_port = 31001
# These are from Adam and are different from the database ones...
ftp_username = "username"
ftp_password = "password"

# This is the directory where the raw data is downloaded to
# it needs read/write perms for whoever is running the downloaded
datadir = "/data/alfa/test_pipeline_clean/"
space_to_use = 60 * 2**30    # max size of downloaded data in bytes
min_free_space = 10 * 2**30  # Minimum amount of disk space on the file system
                             # that must be kept free (bytes)

numdownloads = 2  # max number of files to download at once
numrestores = 2   # max number of active (i.e. open) restore requests at Cornell
numretries = 3    # max number of times to try to download before failing

# The following is the name of the logfile.  Should not need changing.
log_file_path = os.path.join(config.basic.pipelinedir, "logs", "downloader.log")
request_timeout = 1 # Integer number of hours before an un-restored request 
                    # is marked as failed

# The following control the data type and format requested from Cornell
request_numbits = 4 # or 16 (which we will likely never serve)
request_datatype = 'mock' # or 'wapp'

import download_check
download_check.download.populate_configs(locals())
download_check.download.check_sanity()
