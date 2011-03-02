#!/usr/bin/env python
"""
Background script for running pulsar search jobs.

Patrick Lazarus, June 7th, 2010
"""
import sys
import time
import traceback

import job
import mailer
import config.background

def main():
    while True:
        #rotation function changes/updates the states and submits jobs
        #that were created
        try:
            job.status()
            job.rotate()
        except Exception, e:
            traceback_string = ''.join(traceback.format_exception(*sys.exc_info()))
            msg = 'Fatal error occured while running job pool: %s\n\n' % str(e)
            msg += traceback_string
            notification = mailer.ErrorMailer(msg).send()
            sys.stderr.write("Fatal error occurred!\n")
            raise
        time.sleep(config.background.sleep)       

main()
