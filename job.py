#!/usr/bin/env python
"""
A few objects for keeping track of pulsar search jobs.

Patrick Lazarus, June 5th, 2010
"""
import os
import re
import os.path
import config
import datetime
import PBSQuery
import socket
import subprocess

import socket
import shutil

import pprint


class JobPool:
    def __init__(self):
        self.jobs = []
        self.datafiles = []
        self.demand_file_list = {}
        print "Loading datafile(s)..."
        self.get_datafiles()
        print "Creating Jobs from datafile(s)..."
        self.create_jobs_from_datafiles()
        print "Created "+str(len(self.jobs))+" job(s)"

    def create_jobs_from_datafiles(self):
        """Given a list of datafiles, group them into jobs.
            For each job return a PulsarSearchJob object.
        """
        # For PALFA2.0 each observation is contained within a single file.
        
        for datafile in (self.datafiles):
            p_searchjob = PulsarSearchJob([datafile])
            if  isinstance(p_searchjob, PulsarSearchJob):
                self.jobs.append(p_searchjob)

    def delete_job(self, job):
        """Delete datafiles for PulsarSearchJob j. Update j's log.
            Archive j's log.
        """
        if config.delete_rawdata:
            if not is_in_demand(j):
                job.log.addentry(job.LogEntry(status="Deleted", host=socket.gethostname()))
                # Delete data files
                for d in job.datafiles:
                    os.remove(d)
                # Archive log file
                shutil.move(job.logfilenm, config.log_archive)
                self.jobs.delete(job)


    def get_datafiles(self):
        """Return a list of data files found in:
                config.rawdata_directory and its subdirectories
            matching the regular expression pattern:
                config.rawdata_re_pattern
       """
        for (dirpath, dirnames, filenames) in os.walk(config.rawdata_directory):
            for fn in filenames:
                if re.match(config.rawdata_re_pattern, fn) is not None:
                    self.datafiles.append(os.path.join(dirpath, fn))
        if self.datafiles:
            return True
        else:
            return False



    def status(self):
        print "Jobs in the Pool: "+ str(len(self.jobs))
        #print "Jobs Running: "+

    def upload_results(self,job):
        """Upload results from PulsarSearchJob j to the database.
            Update j's log.
        """
        raise NotImplementedError("upload_job() isn't implemented.")

    def rotate(self):
        print "Rotating through: "+ str(len(self.jobs)) +" jobs."
        numrunning, numqueued = self.get_queue_status()
        print "Jobs Running: "+ str(numrunning)
        print "Jobs Queued: "+ str(numqueued)
        cansubmit = (numqueued == 0) # Can submit a job if none are queued
        self.qsub_update_status()

        for job in self.jobs:
            jobname = str(job.jobname)
            status, job.jobid = job.get_log_status()
            print "Name: "+ jobname
            print "PBS Name: "+ str(job.jobid)
            print "Status: "+ status
            print "Q-Status: "+ str(job.status)
            status, job.jobid = job.get_log_status()
            self.qsub_update_status()

            if  job.status == PulsarSearchJob.NEW_JOB:
                self.submit_job(job)
            elif job.status > PulsarSearchJob.NEW_JOB:
                pass
            elif job.status == PulsarSearchJob.TERMINATED:
                if self.restart_job(job):
                    print "Resubmitting a job: "+ job.jobid
                    self.submit_job(job)

#            if (status == "submitted to queue") or \
#                    (status == "processing in progress"):
#                pass
#            elif (status == "processing failed"):
#                numfails = job.count_status("processing failed")
#                if numfails < max_attempts:
#                    if cansubmit:
#                        self.submit_job(job)
#                        cansubmit = False
#                else:
#                    self.delete_job(job)
#            elif (status == "processing successful"):
#                self.upload_results(job)
#            elif (status == "new job"):
#                if cansubmit:
#                    self.submit_job(job)
#                    cansubmit = False
#            elif (status == "upload successful"):
#                self.delete_job(job)
#            else:
#                raise ValueError("Unrecognized status: %s" % status)
#            print "Status: "+ status
#            print str(self.qsub_status(job))
            break


    def submit_job(self, job):
        """Submit PulsarSearchJob j to the queue. Update j's log.
        """
        print "Submitting a job"
        pprint.pprint(job.datafiles)
        print 'qsub -V -v DATAFILES="%s" -l %s -N %s search.py' % \
                            (','.join(job.datafiles), config.resource_list, \
                                    config.job_basename)
        pipe = subprocess.Popen('qsub -V -v DATAFILES="%s" -l %s -N %s -e %s search.py' % \
                            (','.join(job.datafiles), config.resource_list, \
                                    config.job_basename,'qsublog'), \
                            shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE)
        jobid = pipe.communicate()[0]
        job.jobid = jobid.rstrip()
        pipe.stdin.close()
        job.status = PulsarSearchJob.SUBMITED
        job.log.addentry(LogEntry(qsubid=job.jobid, status="Submitted to queue", host=socket.gethostname(), \
                                        info="Job ID: %s" % jobid.strip()))

    def update_demand_file_list(self):
        """Return a dictionary where the keys are the datafile names
            and the values are the number of jobs that require that
            particular file.

            This info will ensure we don't delete data files that are
            being used by multiple jobs before _all_ the jobs are
            finished.
        """
        self.demand_file_list = {}
        for job in self.jobs:
            status, jobid = job.get_log_status()
            if (status in ['submitted to queue', 'processing in progress', \
                            'processing successful', 'new job']) or \
                            ((status == 'processing failed') and \
                            (job.count_status() < config.max_attempts)):
                # Data files are still in demand
                for d in job.datafiles:
                    if d in self.demand_file_list.keys():
                        self.demand_file_list[d] += 1
                    else:
                        self.demand_file_list[d] = 1
         

    def get_queue_status(self):
        """Connect to the PBS queue and return the number of
            survey jobs running and the number of jobs queued.

            Returns a 2-tuple: (numrunning, numqueued).
        """
        batch = PBSQuery.PBSQuery()
        alljobs = batch.getjobs()
        numrunning = 0
        numqueued = 0
        for j in alljobs.keys():
            #pprint.pprint(alljobs[j]['Job_Name'])
            if alljobs[j]['Job_Name'][0].startswith(config.job_basename):
                if 'Q' in alljobs[j]['job_state']:
                    numqueued += 1
                elif 'R' in alljobs[j]['job_state']:
                    numrunning += 1
        return (numrunning, numqueued)

    def is_in_demand(self,job):
        """Check if the datafiles used for PulsarSearchJob j are
            required for any other jobs. If so, return True,
            otherwise return False.
        """
        self.update_demand_file_list() #update demanded file list
        in_demand = False
        for datafile in job.datafiles:
            if self.demand_file_list[datafile] > 0:
                in_demand = True
                break
        return in_demand

    #def qsub_status(self, job):
    def qsub_job_error(self, job):
        if os.path.exists(os.path.join("qsublog",config.job_basename+".e"+job.jobid.split(".")[0])):
            if os.path.getsize(os.path.join("qsublog",config.job_basename+".e"+job.jobid.split(".")[0])) > 0:
                job.log.addentry(LogEntry(qsubid=job.jobid, status="Processing failed", host=socket.gethostname(), \
                                        info="Job ID: %s" % job.jobid.strip()))
                return True
        else:
            return False



    """
    Updates JobPool Jobs using from qsub queue and qsub error logs.
    
    """
    def qsub_update_status(self):
        
        batch = PBSQuery.PBSQuery()
        for job in self.jobs:
            if job.jobid in batch.getjobs():
                if 'R' in batch.getjobs()['job.jobid']['job_state']:
                    job.status = PulsarSearchJob.SUBMITED_RUNNING
                else:
                    job.status = PulsarSearchJob.SUBMITED_QUEUED
            else:
                if job.status > PulsarSearchJob.NEW_JOB:
                    job.status = PulsarSearchJob.TERMINATED

    def restart_job(self, job):
        log_status, job.jobid = job.get_log_status()
        cansubmit = True
        numfails = job.count_status("processing failed")
        if (numfails < config.max_attempts):
            cansubmit = False
 
        if (self.qsub_job_error(job) and cansubmit):
            return True
        else:
            return False
        pass

















class PulsarSearchJob:
    """A single pulsar search job object.
    """
    TERMINATED = 0
    NEW_JOB = 1
    SUBMITED = 2
    SUBMITED_QUEUED = 3
    SUBMITED_RUNNING = 4
    
    def __init__(self, datafiles):
        """PulsarSearchJob creator.
            'datafiles' is a list of data files required for the job.
        """
        self.datafiles = datafiles
        self.jobname = self.get_jobname()
        self.jobid = None
        #self.logfilenm = self.jobname + ".log"
        self.logfilenm = os.path.join(config.log_dir,os.path.basename(self.jobname) + ".log")
        self.log = JobLog(self.logfilenm, self)
        self.status = self.NEW_JOB

    def get_status(self):
        """Get and return the status of the most recent log entry.
        """
        return self.log.logentries[-1].status.lower() , self.log.logentries[-1].qsubid

    def count_status(self, status):
        """Count and return the number of times the job has reported
            'status' in its log.
        """
        count = 0
        for entry in self.log.logentries:
            if entry.status.lower() == status.lower():
                count += 1
        return count

    def get_jobname(self):
        """Based on data files determine the job's name and return it.
        """
        datafile0 = self.datafiles[0]
        if datafile0.endswith(".fits"):
            jobname = datafile0[:-5]
        else:
            raise ValueError("First data file is not a FITS file!" \
                             "\n(%s)" % datafile0)
        return jobname



def get_jobname(datafiles):
    """Based on data files determine the job's name and return it.
    """
    datafile0 = datafiles[0]
    if datafile0.endswith(".fits"):
        jobname = datafile0[:-5]
    else:
        raise ValueError("First data file is not a FITS file!" \
                         "\n(%s)" % datafile0)
    return jobname


class JobLog:
    """A object for reading/writing logfiles for search jobs.
    """
    def __init__(self, logfn, job):
        self.logfn = logfn
        self.job = job # PulsarSearchJob object that this log belongs to
        self.logfmt_re = re.compile("^(?P<date>.*) -- (?P<qsubid>.*) -- (?P<status>.*) -- " \
                                    "(?P<host>.*) -- (?P<info>.*)$")
        if os.path.exists(self.logfn):
            # Read the logfile
            self.logentries = self.read()
        else:
            # Create the log file
            entry = LogEntry(qsubid = job.jobid,status="New job", host=socket.gethostname(), \
                             info="Datafiles: %s" % self.job.datafiles)
            self.addentry(entry)
            self.logentries = [entry]
        self.lastupdate = os.path.getmtime(self.logfn)

    def parse_logline(self, logline):
        """Parse a line from a log and return a LogEntry object.
        """
        m = self.logfmt_re.match(logline)
        return LogEntry( ** m.groupdict())

    def read(self):
        """Open the log file, parse it and return a list
            of entries.
            
            Notes: '#' defines a comment.
                   Each entry should have the following format:
                   'datetime' -- 'qsubid' -- 'status' -- 'hostname' -- 'additional info'
        """
        logfile = open(self.logfn)
        lines = [line.partition("#")[0] for line in logfile.readlines()]
        logfile.close()
        lines = [line for line in lines if line.strip()] # remove empty lines

        # Check that all lines have the correct format:
        for line in lines:
            if self.logfmt_re.match(line) is None:
                raise ValueError("Log file line doesn't have correct format" \
                                 "\n(%s)!" % line)
        logentries = [self.parse_logline(line) for line in lines]
        return logentries

    def update(self):
        """Check if log has been modified since it was last read.
            If so, read the log file.
        """
        mtime = os.path.getmtime(self.logfn)
        if self.lastupdate < mtime:
            # Log has been modified recently
            self.logentries = self.read_log()
            self.lastupdate = mtime
        else:
            # Everything is up to date. Do nothing.
            pass

    def addentry(self, entry):
        """Open the log file and add 'entry', a LogEntry object.
        """
        logfile = open(self.logfn, 'a')
        logfile.write(str(entry) + "\n")
        logfile.close()


class LogEntry:
    """An object for describing entries in a JobLog object.
    """
    def __init__(self, qsubid, status, host, info="", date=datetime.datetime.now().isoformat(' ')):
        self.status = status
        self.qsubid = qsubid
        self.host = host
        self.info = info
        self.date = date

    def __str__(self):
        return "%s -- %s -- %s -- %s -- %s" % (self.date, self.qsubid, self.status, self.host, \
                                         self.info)

"""
Mapping of status to action:

Submitted to queue -> Do nothing
Processing in progress -> Do nothing
Processing successful -> Upload/tidy results, delete file, archive log
Processing failed -> if attempts<thresh: resubmit, if attempts>=thresh: delete file, archive log
"""
