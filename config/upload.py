import subprocess
import os
import config.basic
################################################################
# Result Uploader Configuration
################################################################
prestohash = subprocess.Popen("git rev-parse HEAD", cwd=os.getenv('PRESTO'), \
                        stdout=subprocess.PIPE, shell=True).stdout.read().strip()
pipelinehash = subprocess.Popen("git rev-parse HEAD", cwd=config.basic.pipelinedir, \
                        stdout=subprocess.PIPE, shell=True).stdout.read().strip()
version_num = 'PRESTO:%s;PIPELINE:%s' % (prestohash, pipelinehash)

import upload_check
upload_check.upload.populate_configs(locals())
upload_check.upload.check_sanity()
