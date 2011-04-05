import subprocess
import os
import config.basic
################################################################
# Result Uploader Configuration
################################################################
def version_num():
    """Compute version number and return it as a string.
    """
    prestohash = subprocess.Popen("git rev-parse HEAD", \
                            cwd=os.getenv('PRESTO'), \
                            stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    pipelinehash = subprocess.Popen("git rev-parse HEAD", \
                            cwd=config.basic.pipelinedir, \
                            stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    psrfits_utilshash = subprocess.Popen("git rev-parse HEAD", \
                            cwd=config.basic.psrfits_utilsdir, \
                            stdout=subprocess.PIPE, shell=True).stdout.read().strip()
    vernum = 'PRESTO:%s;PIPELINE:%s;PSRFITS_UTILS:%s' % \
                            (prestohash, pipelinehash, psrfits_utilshash)
    return vernum

import upload_check
upload_check.upload.populate_configs(locals())
upload_check.upload.check_sanity()
