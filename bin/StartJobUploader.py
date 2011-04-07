import time
import sys
import traceback

import mailer
import JobUploader
import config.background
import config.email

while True:
    try:
        JobUploader.run()
    except Exception, e:
        if config.email.send_on_crash:
            msg  = '*** Uploader has crashed! ***\n\n'
            msg += 'Fatal error occured while running job uploader: %s\n\n' % str(e)
            msg += ''.join(traceback.format_exception(*sys.exc_info()))
            notification = mailer.ErrorMailer(msg).send()
        sys.stderr.write("Fatal error occurred!\n")
        raise
    time.sleep(config.background.sleep)       

