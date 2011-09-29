import stringio
import os
import pipeline_utils
import config.basic
################################################################
# Result Uploader Configuration
################################################################
def version_num():
    """Compute version number and return it as a string.
    """
    strio = StringIO.StringIO()
    pipeline_utils.execute("git rev-parse HEAD", \
                            dir=os.getenv('PRESTO'), \
                            stdout=strio, shell=True)
    prestohash = strio.getvalue().strip()
    strio.close()
    strio = StringIO.StringIO()
    pipeline_utils.execute("git rev-parse HEAD", \
                            dir=config.basic.pipelinedir, \
                            stdout=strio, shell=True)
    pipelinehash = strio.getvalue().strip()
    strio.close()
    strio = StringIO.StringIO()
    pipeline_utils.execute("git rev-parse HEAD", \
                            dir=config.basic.psrfits_utilsdir, \
                            stdout=strio, shell=True)
    psrfits_utilshash = strio.getvalue().strip()
    strio.close()
    vernum = 'PRESTO:%s;PIPELINE:%s;PSRFITS_UTILS:%s' % \
                            (prestohash, pipelinehash, psrfits_utilshash)
    return vernum

import upload_check
upload_check.upload.populate_configs(locals())
upload_check.upload.check_sanity()
