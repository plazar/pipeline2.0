#!/usr/bin/env python
"""
Background script for running pulsar search jobs.

Patrick Lazarus, June 7th, 2010
"""
import re
import os
import sys
import time
import socket
import shutil
import os.path
import traceback
import subprocess
from mailer import ErrorMailer

import config
import dev
import job

def main():
    global datafile_demand

    try:
        #initialize new JobPool object to manage search jobs in QSUB
        jobpool = job.JobPool()
    except Exception, e:
        try:
            traceback_string = ''.join(traceback.format_exception(*sys.exc_info()))
            msg = 'Could not initialize JobPool.\nFatal occured: %s' % str(e)
            msg += '\n\nTraceback:\n' + traceback_string
            notification = ErrorMailer(msg)
            notification.send()
            sys.stderr.write("Fatal error occurred! Could not initialize JobPool\n")
            sys.stderr.write(traceback_string)
            sys.exit(1)
        except Exception:
            raise

    while True:
        #rotation function changes/updates the states and submits jobs
        #that were created
        try:
            jobpool.rotate()
            jobpool.status()
        except Exception, e:
            try:
                traceback_string = ''.join(traceback.format_exception(*sys.exc_info()))
                msg = 'Could not initialize JobPool.\nFatal occured: %s' % str(e)
                msg += '\n\nTraceback:\n' + traceback_string
                notification = ErrorMailer(msg)
                notification.send()
                sys.stderr.write("Fatal error occurred!\n")
                sys.stderr.write(traceback_string)
                sys.exit(1)
            except Exception:
                raise
        time.sleep(config.bgs_sleep)       

main()
