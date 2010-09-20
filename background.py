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
from job import *
import job
import config
import dev

def main():
    global datafile_demand

   #dev.generate_dummy_fits()
   # datafiles = get_datafiles()

    jobpool = JobPool()

    for job in jobpool.jobs:
        print job.jobname
        print job.get_log_status()
        
    jobpool.status()

    while True:
        jobpool.rotate()
        time.sleep(1)
        


main()
