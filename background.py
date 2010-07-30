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

import PBSQuery

import job
import config

def get_datafiles():
    """Return a list of data files found in:
            config.rawdata_directory and its subdirectories
        matching the regular expression pattern:
            config.rawdata_re_pattern
    """
    datafiles = []
    for (dirpath, dirnames, filenames) in os.walk(config.rawdata_directory):
        for fn in filenames:
            if re.match(config.rawdata_re_pattern, fn) is not None: 
                datafiles.append(os.path.join(dirpath, fn))
    return datafiles


def get_demand(searchjobs):
    """Return a dictionary where the keys are the datafile names
        and the values are the number of jobs that require that
        particular file.
        
        This info will ensure we don't delete data files that are
        being used by multiple jobs before _all_ the jobs are
        finished.
    """
    datafile_demand = {}
    for j in searchjobs:
        status = j.get_status().lower()
        if (status in ['submitted to queue', 'processing in progress', \
                        'processing successful', 'new job']) or \
                        ((status == 'processing failed') and \
                        (j.count_status() < config.max_attempts)):
            # Data files are still in demand
            for d in j.datafiles:
                if d in datafile_demand.keys():
                    datafile_demand[d] += 1
                else:
                    datafile_demand[d] = 1
    return datafile_demand


def get_queue_status():
    """Connect to the PBS queue and return the number of
        survey jobs running and the number of jobs queued.
        
        Returns a 2-tuple: (numrunning, numqueued).
    """
    batch = PBSQuery.PBSQuery()
    alljobs = batch.getjobs()
    numrunning = 0
    numqueued = 0
    for j in alljobs.keys():
        if alljobs[j]['Job_Name'].startswith(config.job_basename):
            if 'Q' in alljobs[j]['job_state']:
                numqueued += 1
            elif 'R' in alljobs[j]['job_state']:
                numrunning += 1
    return (numrunning, numqueued)


def is_in_demand(j):
    """Check if the datafiles used for PulsarSearchJob j are
        required for any other jobs. If so, return True, 
        otherwise return False.
    """
    global datafile_demand
    in_demand = False
    for datafile in j.datafiles:
        if datafile_demand[datafile] > 0:
            in_demand = True
            break
    return in_demand


def submit_job(j):
    """Submit PulsarSearchJob j to the queue. Update j's log.
    """
    pipe = subprocess.Popen('qsub -V -v DATA_FILE="%s" -l %s -N %s' % \
                        (','.join(j.datafiles), config.resource_list, \
                                config.job_basename), \
                        shell=True, stdout=subprocess.PIPE)
    jobid = pipe.communicate()[0]
    pipe.close()
    j.log.addentry(job.LogEntry(status="Submitted to queue", host=socket.gethostname(), \
                                    info="Job ID: %s" % jobid.strip()))


def upload_results(j):
    """Upload results from PulsarSearchJob j to the database.
        Update j's log.
    """
    raise NotImplementedError("upload_job() isn't implemented.")


def delete_job(j):
    """Delete datafiles for PulsarSearchJob j. Update j's log.
        Archive j's log.
    """
    if config.delete_rawdata:
        if not is_in_demand(j):
            j.log.addentry(job.LogEntry(status="Deleted", host=socket.gethostname()))
            # Delete data files
            for d in j.datafiles:
                os.remove(d)
            # Archive log file
            shutil.move(j.logfilenm, config.log_archive)
        

def main():
    global datafile_demand

    while True:
        datafiles = get_datafiles()
        jobpool = JobPool()
        searchjobs = JobPool.jobs_from_datafiles(datafiles)
        datafile_demand = get_demand(searchjobs)
       
        numrunning, numqueued = get_queue_status()
        cansubmit = (numqueued == 0) # Can submit a job if none are queued
        for j in searchjobs:
            status = j.get_status.lower()
            if (status == "submitted to queue") or \
                    (status == "processing in progress"):
                pass
            elif (status == "processing failed"):
                numfails = j.count_status("processing failed")
                if numfails < max_attempts:
                    if cansubmit:
                        submit_job(j)
                        cansubmit = False
                else:
                    delete_job(j)
            elif (status == "processing successful"):
                upload_results(j)
            elif (status == "new job"):
                if cansubmit:
                    submit_job(j)
                    cansubmit = False
            elif (status == "upload successful"):
                delete_job(j)
            else:
                raise ValueError("Unrecognized status: %s" % status)
        time.sleep(config.sleep_time)