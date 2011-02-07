#!/usr/bin/env python
"""
Background script for running pulsar search jobs.

Patrick Lazarus, June 7th, 2010
"""
import re
import os
import os.path
import subprocess
import time
import socket
import shutil
import job
import traceback
from mailer import ErrorMailer
import config
import dev
from mailer import ErrorMailer 

def main():
    global datafile_demand
    
    try:
        #initialize new JobPool object to manage search jobs in QSUB
        jobpool = job.JobPool()
    except Exception, e:
	print "Fatal occured: "+ str(e)
        try:
            notification = ErrorMailer('Could not initialize JobPool.\nFatal occured: %s' % str(e))
            notification.send()
            exit("Could not initialize JobPool.")
        except Exception,e:
            pass
        

    while True:
        #rotation function changes/updates the states and submits jobs
        #that were created
        try:
            jobpool.rotate()
            jobpool.status()
        except Exception, e:
            try:
                notification = ErrorMailer('Fatal occured: %s' % str(e))
                notification.send()
                exit('Fatal occured: %s' % str(e))
            except Exception,e:
                pass
        time.sleep(config.bgs_sleep)       

main()
