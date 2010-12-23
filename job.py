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
#import PBSQuery
import socket
import subprocess

import socket
import shutil

import pprint
import dev
from formats import psrfits

import sqlite3

from mailer import ErrorMailer

class JobPool:
    def __init__(self):
        self.jobs = []
        self.datafiles = []
        self.demand_file_list = {}
        self.cycles = 0
        self.merged_dict = {}
        

    def start(self):
        print "Loading datafile(s)..."
       
        print "Creating Jobs from datafile(s)..."
        self.fetch_new_jobs()
        print "Created "+str(len(self.jobs))+" job(s)"
        
    def create_jobs_from_datafiles(self,files_in = None):
        """Given a list of datafiles, group them into jobs.
            For each job return a PulsarSearchJob object.
        """

        #group files for preproccessing (merging)
        files_in = self.group_files(files_in)
        #merge files before submitting a job
        files_in = self.merge_files(files_in)

        # For PALFA2.0 each observation is contained within a single file.
        if not files_in:
            return
        for datafile in (files_in):
            p_searchjob = PulsarSearchJob([datafile])
            if  isinstance(p_searchjob, PulsarSearchJob):
                self.datafiles.append(datafile)
                self.jobs.append(p_searchjob)
        
    def group_files(self, files_in):
        """Given a list of datafiles, group files that need to be merged before
            submitting to QSUB.
            Return a list of datafiles, files that are grouped
            are list of a list.
        """
        files_out = []
        processed = []
        for file in files_in[:]:
            if not file in processed:
            #4bit-p2030.20100810.B2020+28.b0s0g0.00100.fits
                match = re.match("4bit-.*\.b0s\dg0\.\d{5}\.fits", file)
                if match: #if it is a 4bit files start looking for associated files
                    processed.append(file)
                    new_group = [file]
                    for next_file in files_in[:]:
                        if not next_file in processed:
                            new_match = re.match(file[0:len(file)-17]\
                            .replace("+", "\+") + 'b0s\dg0' + \
                            file[len(file)-11:len(file)], next_file)
                            if new_match:
                                processed.append(next_file)
                                new_group.append(next_file)
                    files_out.append(new_group)
                else:
                    files_out.append(file)
        return files_out

    def merge_files(self, files_in):
        """Given a list of datafiles run a mergin command on datafiles that
            are grouped into list of a list.
            Return list of datafiles replacing the grouped file with a
            single merged file.
        """
        files_out = []
        for item in files_in:
            if isinstance(item, list):
                merge_pipe = subprocess.Popen('merger %s' % (" ".join(item)), \
                            shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE)
                merger_response = merge_pipe.communicate()

                #get response if merging was successfull
                if not merger_response[1]:
                    #merging was successfull - add merged file to files list for return
                    #assume merger_response[0] is the produced file name of merged
                    self.merged_dict[merger_response[0]] = item
                    files_out.append(merger_response[0])
                    
                else:
                    #mailer can send an error to supervisor
                    mailing_message = "<h1>Merger Error.</h1> \n <p>The following error occured:</p>\n %s" % \
                    (merger_response)
                    mailing_message += "<br>\nFiles that could not be merged:"
                    for fn in item:
                        mailing_message += "<br>\n"+ fn
                    mailer = ErrorMailer(mailing_message)
                    mailer.send()
                    
                #add pre-merged files to datafiles, so they will not get picked up again on rotation
                self.datafiles += item
            else:
                #single file - do not need to merge it
                files_out.append(item)
        return files_out

    def delete_job(self, job):
        """Delete datafiles for PulsarSearchJob j. Update j's log.
            Archive j's log.
        """
        job.log.addentry(LogEntry(qsubid=job.jobid,status="Deleted", host=socket.gethostname(),info="Job was deleted"))
        if config.delete_rawdata:
            if not self.is_in_demand(job):                
                # Delete data files
                for d in job.datafiles:
                    print "Deleting datafile: " + str(d)
                    os.remove(d)
                # Archive log file
                if os.path.exists(os.path.join(config.log_archive,os.path.basename(job.logfilenm))):
                    os.remove(os.path.join(config.log_archive,os.path.basename(job.logfilenm)))
                shutil.move(job.logfilenm, config.log_archive)
        

        if job in self.jobs:
            self.jobs.remove(job)

        if job.jobname+".fits" in self.datafiles:
            self.datafiles.remove(job.jobname+".fits")

    def get_datafiles_from_db(self):
        didnt_get_files = True
        tmp_datafiles = []
        while didnt_get_files:
            try:
                db_conn = sqlite3.connect("sqlite3db");
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                fin_file_query = "SELECT * FROM restore_downloads WHERE status LIKE 'Finished:%'"
                db_cur.execute(fin_file_query)
                row = db_cur.fetchone()
                while row:
                    print row['filename'] +" "+ row['status']
                    tmp_datafiles.append(os.path.join(config.rawdata_directory,row['filename']))
                    row = db_cur.fetchone()                
                didnt_get_files = False
                return tmp_datafiles
            except Exception,e:
                print "Database error: "+ str(e)+" Retrying in 1 sec"
                    
                    

    def get_datafiles(self):
        """Return a list of data files found in:
                config.rawdata_directory and its subdirectories
            matching the regular expression pattern:
                config.rawdata_re_pattern
       """
        tmp_datafiles = []
        for (dirpath, dirnames, filenames) in os.walk(config.rawdata_directory):
            for fn in filenames:
                if re.match(config.rawdata_re_pattern, fn) is not None:
                    tmp_datafiles.append(os.path.join(dirpath, fn))
		    print "Adding file:" + os.path.join(dirpath, fn)
        return tmp_datafiles



    def status(self):
        print "Jobs in the Pool: "+ str(len(self.jobs))
        #print "Jobs Running: "+

    def upload_results(self,job):
        """Upload results from PulsarSearchJob j to the database.
            Update j's log.
        """
        raise NotImplementedError("upload_results() isn't implemented.")

    def rotate(self):
        print "Rotating through: "+ str(len(self.jobs)) +" jobs."
        numrunning, numqueued = self.get_queue_status()
        print "Jobs Running: "+ str(numrunning)
        print "Jobs Queued: "+ str(numqueued)
        cansubmit = (numqueued == 0) # Can submit a job if none are queued
        self.qsub_update_status()

        
        for job in self.jobs[:]:
            jobname = str(job.jobname)
            status, job.jobid = job.get_log_status()
            self.qsub_update_status()

            if  job.status == PulsarSearchJob.NEW_JOB:
                if self.restart_job(job):
                    print "Submitting a job"
                    self.submit_job(job)
                else:
                    print "Forbidden to restart this job - deleting"
                    self.delete_job(job)
            elif job.status > PulsarSearchJob.NEW_JOB:
                pass
            elif job.status == PulsarSearchJob.TERMINATED:
                if self.qsub_job_error(job):
                    if self.restart_job(job):
                        print "Resubmitting a job: "+ job.jobid
                        self.submit_job(job)
                    else:
                        print "Removing the job: Multiple fails: "+job.jobname
                        self.delete_job(job)
                else:
                    #if job terminated with no errors - upload the results
                    self.upload_results(job)


        str_status = ['TERMINATED','NEW_JOB','SUBMITED','SUBMITED_QUEUED','SUBMITED_RUNNING']
        print "\t\t\tName\t\t\t\tJob ID\t\t\tLog-Status\t\t\tQ-Status"
        for job in self.jobs:
            print "%s\t%s\t%s\t%s" % (job.jobname,job.jobid,status,str_status[job.status])

        if self.cycles == 10:
            print "================================ Adding files"
            #dev.add_files()
        self.cycles += 1

        self.fetch_new_jobs()

    def submit_job(self, job):
        """Submit PulsarSearchJob j to the queue. Update j's log.
        """
#        print 'qsub -V -v DATAFILES="%s" -l %s -N %s search.py' % \
#                            (','.join(job.datafiles), config.resource_list, \
#                                    config.job_basename)
        pipe = subprocess.Popen('qsub -V -v DATAFILES="%s" -l %s -N %s -e %s search.py' % \
                            (','.join(job.datafiles), config.resource_list, \
                                    config.job_basename,'qsublog'), \
                            shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE)

        jobid = pipe.communicate()[0]
        job.jobid = jobid.rstrip()
       
        pipe.stdin.close()

#        job.jobid = dev.get_fake_job_id()
#        dev.write_fake_qsub_error(os.path.join("qsublog",config.job_basename+".e"+job.jobid.split(".")[0]))

        job.status = PulsarSearchJob.SUBMITED
        job.log.addentry(LogEntry(qsubid=job.jobid, status="Submitted to queue", host=socket.gethostname(), \
                                        info="Job ID: %s" % job.jobid.strip()))

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
                            (job.count_status(status) < config.max_attempts)):
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
        return (0,0)
    
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
            if datafile in self.demand_file_list:
                if self.demand_file_list[datafile] > 0:
                    in_demand = True
                    break
        return in_demand

    #def qsub_status(self, job):
    def qsub_job_error(self, job):
        """Check if qsub job terminated with an error.
            Return True if the job terminated with the error, False otherwise.
        """
        if os.path.exists(os.path.join("qsublog",config.job_basename+".e"+job.jobid.split(".")[0])):
            if os.path.getsize(os.path.join("qsublog",config.job_basename+".e"+job.jobid.split(".")[0])) > 0:
                job.log.addentry(LogEntry(qsubid=job.jobid, status="Processing failed", host=socket.gethostname(), \
                                        info="Job ID: %s" % job.jobid.strip()))
                return True
        else:
            return False
    
    def qsub_update_status(self):
        """Updates JobPool Jobs using from qsub queue and qsub error logs.
        """
        for job in self.jobs:
#            job.status = PulsarSearchJob.TERMINATED
            batch = PBSQuery.PBSQuery().getjobs()
            if job.jobid in batch:
                if 'R' in batch[job.jobid]['job_state']:
                    job.status = PulsarSearchJob.SUBMITED_RUNNING
                else:
                    job.status = PulsarSearchJob.SUBMITED_QUEUED
            else:
                if job.status > PulsarSearchJob.NEW_JOB:
                    job.status = PulsarSearchJob.TERMINATED

    def restart_job(self, job):
        log_status, job.jobid = job.get_log_status()
        self.qsub_job_error(job)

        cansubmit = True
        numfails = job.count_status("processing failed")
        deleted = job.count_status("deleted")
        if (numfails > config.max_attempts):
            cansubmit = False


        if deleted > 0:
            deleted = True

        if (cansubmit and not deleted):
            return True
        else:
            return False
        pass

    def fetch_new_jobs(self):
#        print "=====================================  Fetching new jobs"
        files_to_x_check = self.get_datafiles()
#        print "Files found: "+ str(len(files_to_x_check))
        for file in self.datafiles:
            if file in files_to_x_check:
                files_to_x_check.remove(file)
#        print "Files kept: "+str(len(files_to_x_check))

        for file in files_to_x_check:
            tmp_job = PulsarSearchJob([file])
            if not self.restart_job(tmp_job):
#                print "Removing file: "+ file
                self.delete_job(tmp_job)
                files_to_x_check.remove(file)
#            else:
#                print "Will not remove file because i can restart the job: "+ file
#        print "Files left to add to queue: "+ str(len(files_to_x_check))
        self.create_jobs_from_datafiles(files_to_x_check)
#        print "===================================== END END END  Fetching new jobs"


class PulsarSearchJob:
    """A single pulsar search job object.
    """
    TERMINATED = 0
    NEW_JOB = 1
    SUBMITED = 2
    SUBMITED_QUEUED = 3
    SUBMITED_RUNNING = 4
    UPLOAD_STARTED = 5
    UPLOAD_FAILED = 6
    UPLOAD_COMPLETED = 7
    FINISHED = 8
    
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
        self.presto_output_dir = self.prep_out_dir()
        self.status = self.NEW_JOB

    def prep_out_dir(self,in_datafiles=None):
        if not in_datafiles:
            in_datafiles = self.datafiles
        if isinstance(in_datafiles, list):
            filename=in_datafiles[0]
        elif isinstance(in_datafiles, string):
            filename=in_datafiles
        else:
            raise Exception('Could not get input filename(s)')

        if not os.path.isfile(filename):
            raise Exception('File with the given path doesn\'t exists.')
        elif filename[len(filename)-5:] != ".fits":
            raise Exception('Unrecognized input file extension.')


        """for the file: p2030.20100810.B2020+28.b0s0g0.00100.fits
            rawdata basename: p2030.20100810.B2020+28.b0s0g0.00100
            beam number: 0 (from b0)
            processing date: (not from file name)
            mjd: (Read from the file header using the psrfits module.)
        """
        parsed=psrfits.SpectraInfo(filename)
        imjd, fmjd=DATEOBS_to_MJD(parsed.date_obs)
        mjdtmp="%.14f" % fmjd
        MJD="%5d.%14s" % (imjd, mjdtmp[2:])
        basename=os.path.basename(filename)
        rawdata_basename=basename[len(basename)-5:]
        
        try:
            beam_num = int(basename[len(basename)-16:len(basename)-15])
        except ValueError:
            raise Exception('Could not determine raw file\'s beam number.')
        
        proc_date=datetime.datetime.now().strftime('%y%m%d')
        
        presto_out_dir = config.base_working_directory +"/"+ MJD +"/"+ rawdata_basename +"/"+ str(beam_num)
        
        try:
            os.makedirs(presto_out_dir)
        except OSError:
            if not os.path.exists(presto_out_dir):
                raise "Could not create directory: %s" % presto_out_dir
        
        return presto_out_dir

        
    def get_log_status(self):
        """Get and return the status of the most recent log entry.
        """
        self.log = JobLog(self.logfilenm, self)
#        print "=========LOG entry"
#        print self.log.logentries[-1].status.lower() , self.log.logentries[-1].qsubid
#        print "=========LOG entry"        
        return self.log.logentries[-1].status.lower() , self.log.logentries[-1].qsubid

    def count_status(self, status):
        """Count and return the number of times the job has reported
            'status' in its log.
        """
        self.log = JobLog(self.logfilenm, self)
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
