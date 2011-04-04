import subprocess
import os
import config.basic
################################################################
# Result Uploader Configuration
################################################################
prestohash = subprocess.Popen("git rev-parse HEAD", \
                        cwd=os.getenv('PRESTO'), \
                        stdout=subprocess.PIPE, shell=True).stdout.read().strip()
pipelinehash = subprocess.Popen("git rev-parse HEAD", \
                        cwd=config.basic.pipelinedir, \
                        stdout=subprocess.PIPE, shell=True).stdout.read().strip()
psrfits_utilshash = subprocess.Popen("git rev-parse HEAD", \
                        cwd=config.basic.psrfits_utilsdir, \
                        stdout=subprocess.PIPE, shell=True).stdout.read().strip()
version_num = 'PRESTO:%s;PIPELINE:%s;PSRFITS_UTILS:%s' % \
                        (prestohash, pipelinehash, psrfits_utilshash)

import upload_check
upload_check.upload.populate_configs(locals())
upload_check.upload.check_sanity()
