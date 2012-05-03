import StringIO
import os
import pipeline_utils
import config.basic
################################################################
# Result Uploader Configuration
################################################################
def version_num():
    """Compute version number and return it as a string.
    """
    prestohash = pipeline_utils.execute("git rev-parse HEAD", \
                            dir=os.getenv('PRESTO'))[0].strip()
    pipelinehash = pipeline_utils.execute("git rev-parse HEAD", \
                            dir=config.basic.pipelinedir)[0].strip()
    psrfits_utilshash = pipeline_utils.execute("git rev-parse HEAD", \
                            dir=config.basic.psrfits_utilsdir)[0].strip()
    vernum = 'PRESTO:%s;PIPELINE:%s;PSRFITS_UTILS:%s' % \
                            (prestohash, pipelinehash, psrfits_utilshash)
    return vernum

# Directory on the FTP server to upload PFDs and singlepulse files (do not change unless asked by Adam)
pfd_ftp_dir = 'pfd'
sp_ftp_dir = 'singlePulse'

import upload_check
upload_check.upload.populate_configs(locals())
upload_check.upload.check_sanity()
