#!/usr/bin/env python
"""
Background script for running pulsar search jobs.

Patrick Lazarus, June 7th, 2010
"""
import sys
import time
import traceback
import optparse

import job
import mailer
import pipeline_utils
import config.background
import config.email


def main():
    while True:
        #rotation function changes/updates the states and submits jobs
        #that were created
        try:
            job.status()
            job.rotate()
        except Exception, e:
            if config.email.send_on_crash:
                msg = '*** Job pooler has crashed! ***\n\n'
                msg += 'Fatal error occured while running job pool: %s\n\n' % str(e)
                msg += ''.join(traceback.format_exception(*sys.exc_info()))
                notification = mailer.ErrorMailer(msg, subject="Job Pooler crash!")
                notification.send()
            sys.stderr.write("Fatal error occurred!\n")
            raise
        time.sleep(config.background.sleep)       

if __name__=='__main__':
    parser = optparse.OptionParser(usage="%prog [OPTIONS]", \
                                   description="Start the job pooler.")
    parser.add_option('-d', '--debug', dest='debug', action='store_true', \
                        help="Set the pipeline to print debugging info.", \
                        default=False)
    options, args = parser.parse_args()
    pipeline_utils.DEBUG = options.debug

    try:
        main()
    except KeyboardInterrupt:
        print "Exiting..."
