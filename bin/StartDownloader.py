#!/usr/bin/env python

import time
import sys
import traceback
import optparse

import mailer
import Downloader
import pipeline_utils
import config.background
import config.email

def main():
    delay = 0.5 # First iteration will set delay=1 or multiply by 2
    while True:
        try:
            Downloader.status()
            if Downloader.run():
                # files were successfully downloaded
                delay=1
            else:
                # No files successfully download this iteration
                # Increase sleep time
                delay = min((delay*2, 32))
        except Exception, e:
            if config.email.send_on_crash:
                msg  = '*** Downloader has crashed! ***\n\n'
                msg += 'Fatal error occured while running downloader: %s\n\n' % str(e)
                msg += ''.join(traceback.format_exception(*sys.exc_info()))
                notification = mailer.ErrorMailer(msg, subject="Downloader crash!")
                notification.send()
            sys.stderr.write("Fatal error occurred!\n")
            raise
        print "Will sleep for %d seconds" % (config.background.sleep*delay)
        time.sleep(config.background.sleep*delay)


if __name__=='__main__':
    parser = pipeline_utils.PipelineOptions(usage="%prog [OPTIONS]", \
                                   description="Start the job pooler.")
    options, args = parser.parse_args()

    try:
        main()
    except KeyboardInterrupt:
        print "Exiting..."
