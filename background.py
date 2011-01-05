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
        jobpool = job.JobPool()
        jobpool.start()
        for j in jobpool.jobs:
            print j.jobname
            print j.get_log_status()
   
        jobpool.status()
    
        while True:
            jobpool.rotate()
            time.sleep(60)
            
    except Exception, e:
	print "Error occured: "+ str(e)
        #mailer = ErrorMailer("The following error has occured:\n"+str(e))
        #mailer.send()
        
    
        


main()
