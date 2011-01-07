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

#import PBSQuery

import config
import dev
from mailer import ErrorMailer 

def main():
    global datafile_demand

   #dev.generate_dummy_fits()
   # datafiles = get_datafiles()

    
    try:
        #initialize new JobPool object to manage search jobs in QSUB
        jobpool = job.JobPool()
        jobpool.recover_from_qsub()
        while True:
            #rotation function changes/updates the states and submits jobs
            #that were created
            try:
                jobpool.fetch_new_jobs()
                jobpool.status()
                jobpool.rotate()
            except Exception, e:
                print "Error occured: %s" % str(e)
            time.sleep(60)
            
    except Exception, e:
	    print "Fatal occured: "+ str(e)
        #mailer = ErrorMailer("The following error has occured:\n"+str(e))
        #mailer.send()
        
    
        


main()
