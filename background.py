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

#import PBSQuery

import config
import dev
from mailer import ErrorMailer 

def main():
    global datafile_demand

   #dev.generate_dummy_fits()
   # datafiles = get_datafiles()

    
    try:
        from job import *
        import job
        jobpool = JobPool()
        jobpool.start()
        for job in jobpool.jobs:
            print job.jobname
            print job.get_log_status()
        
        jobpool.status()
    
        while True:
            jobpool.rotate()
            time.sleep(1)
            
    except Exception as e:
        mailer = ErrorMailer("The following error has occured:\n"+str(e))
        mailer.send()
        
    
        


main()
