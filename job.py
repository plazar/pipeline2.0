#!/usr/bin/env python
"""
A few objects for keeping track of pulsar search jobs.

Patrick Lazarus, June 5th, 2010
"""
import os
import re
import os.path
import datetime
import sqlite3
import socket
import subprocess
import shutil
import pprint

import logging
import datafile
from QsubManager import Qsub
from PipelineQueueManager import PipelineQueueManager
QueueManagerClass = Qsub

import config
import dev
import time

from mailer import ErrorMailer


"""
from QTestManager import QTest
from PipelineQueueManager import PipelineQueueManager
QueueManagerClass = QTest
"""

from OutStream import OutStream as OutStream

jobpool_cout = OutStream("JobPool","background.log",config.bgs_screen_output)
job_cout = OutStream("Job","background.log",config.bgs_screen_output)

from mailer import ErrorMailer

class JobPool:
    def __init__(self):
        self.jobs = []
        self.datafiles = []
        self.demand_file_list = {}
        self.merged_dict = {}

    def shutdown(self):
        result = self.query("SELECT exit_jobpool FROM pipeline")
        if int(result['exit_jobpool']) == 1:
            return True
        else:
            return False
    
    #Creates PulsarSearchJob(s) from datafiles added to the list -> self.datafiles    
    def create_jobs_from_datafiles(self,files_in = None):
        """Given a list of datafiles, group them into jobs.
            For each job return a PulsarSearchJob object.
        """
        if not files_in:
            return

        #group files for preproccessing (merging)
        files_in = self.group_files(files_in)
        #merge files before submitting a job
        files_in = self.merge_files(files_in)

        # For PALFA2.0 each observation is contained within a single file.
        for datafile in (files_in):
            try:
                out_str =  "DEBUG: [datafile] when creating PulsarSearchJob", [datafile]
                jobpool_cout.outs(out_str, OutStream.DEBUG)
                p_searchjob = PulsarSearchJob([datafile])
                if  isinstance(p_searchjob, PulsarSearchJob):
                    self.datafiles.append(datafile)
                    self.jobs.append(p_searchjob)
            except Exception,e:
                jobpool_cout.outs("Error occured while creating a SearchJob: "+str(e))
        
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

    #Removes a job from JobPool
    def delete_job(self, job):
        """Delete datafiles for PulsarSearchJob j. Update j's log.
            Archive j's log.
            remove from jobs and datafiles
        """
        job.log.addentry(LogEntry(qsubid=job.jobid,status="Deleted", host=socket.gethostname(),info="Job was deleted"))
        if config.delete_rawdata:
            if not self.is_in_demand(job):                
                # Delete data files
                for d in job.datafiles:
                    jobpool_cout.outs("Deleting datafile: " + str(d))
                    os.remove(d)
                # Archive log file
                if os.path.exists(os.path.join(config.log_archive,os.path.basename(job.logfilenm))):
                    os.remove(os.path.join(config.log_archive,os.path.basename(job.logfilenm)))
                shutil.move(job.logfilenm, config.log_archive)

        if job in self.jobs:
            self.jobs.remove(job)

        if job.jobname+".fits" in self.datafiles:
            self.datafiles.remove(job.jobname+".fits")

    #Removes a job from JobPool
    def complete_job(self, job):
        """Delete datafiles for PulsarSearchJob j. Update j's log.
            Archive j's log.
            remove from jobs and datafiles
        """
        job.log.addentry(LogEntry(qsubid=job.jobid,status="Processed", host=socket.gethostname(),info="Job was processed"))
        self.delete_job(job)
    
    #Returns a list of files that Downloader marked Finished:* in the qlite3db
    def get_datafiles_from_db(self):
        didnt_get_files = True
        tmp_datafiles = []
        while didnt_get_files:
            try:
                db_conn = sqlite3.connect(config.bgs_db_file_path);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                fin_file_query = "SELECT * FROM downloads WHERE status LIKE 'downloaded'"
                db_cur.execute(fin_file_query)
                row = db_cur.fetchone()
                while row:
                    #print row['filename'] +" "+ row['status']
                    tmp_datafiles.append(os.path.join(config.rawdata_directory,row['filename']))
                    row = db_cur.fetchone()                
                didnt_get_files = False
		for file in tmp_datafiles:
                        jobpool_cout.outs(file)
                return tmp_datafiles
            except Exception,e:
                jobpool_cout.outs("Database error: %s. Retrying in 1 sec" % str(e), OutStream.ERROR)
    
    def created_jobs_for_files_DB(self):
        files_with_no_jobs = self.query("SELECT * from downloads as d1 where d1.id not in (SELECT downloads.id FROM jobs, job_files, downloads WHERE jobs.id = job_files.job_id AND job_files.file_id = downloads.id) and d1.status = 'downloaded'")
        for file_with_no_job in files_with_no_jobs:
            self.create_job_entry(file_with_no_job)
     
    def create_job_entry(self,file_with_no_job):
        job_id = self.query("INSERT INTO jobs (status,created_at,updated_at) VALUES ('%s','%s','%s')"\
                                % ('new',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        self.query("INSERT INTO job_files (job_id,file_id,created_at,updated_at) VALUES (%u,%u,'%s','%s')"\
                                            % (job_id,file_with_no_job['id'],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        
            
        
    def update_db_file_processed(self, job):
        db_conn = sqlite3.connect(config.bgs_db_file_path);
        db_conn.row_factory = sqlite3.Row
        db_cur = db_conn.cursor();
        fin_file_query = "UPDATE restore_downloads SET status = 'Processed' WHERE filename = '%s'" % (os.path.basename(job.datafiles[0]))
        db_cur.execute(fin_file_query)
        db_conn.commit()
        db_conn.close()            

    def get_datafiles(self):
        """Return a list of data files found in:
                config.rawdata_directory and its subdirectories
            matching the regular expression pattern:
                config.rawdata_re_pattern
            Now using get_datafiles_from_db
       """
        tmp_datafiles = []
        for (dirpath, dirnames, filenames) in os.walk(config.rawdata_directory):
            for fn in filenames:
                if re.match(config.rawdata_re_pattern, fn) is not None:
                    tmp_datafiles.append(os.path.join(dirpath, fn))
                    jobpool_cout.outs("Adding file:" + os.path.join(dirpath, fn))
        return tmdebug.outp_datafiles

    def status(self,log=True):
        running_jobs = self.query("SELECT * FROM jobs WHERE status='submitted'")
        processed_jobs = self.query("SELECT * FROM jobs WHERE status='processed'")
        new_jobs = self.query("SELECT * FROM jobs WHERE status='new'")
        waiting_resubmit_jobs = self.query("SELECT * FROM jobs WHERE status='failed'")
        failed_jobs = self.query("SELECT * FROM jobs WHERE status='terminal_failure'")
        
        status_str= "\n\n================= Job Pool Status ==============\n"
        status_str+="Num. of jobs       running: %u\n" % len(running_jobs)
        status_str+="Num. of jobs     processed: %u\n" % len(processed_jobs)
        status_str+="Num. of jobs       waiting: %u\n" % len(new_jobs)
        status_str+="Num. of jobs waiting retry: %u\n" % len(waiting_resubmit_jobs)
        status_str+="Num. of jobs        failed: %u\n" % len(failed_jobs)
        if log:
            jobpool_cout.outs(status_str)
        else:
            print status_str

    def upload_results(self,job):
        """Upload results from PulsarSearchJob j to the database.
            Update j's log.
        """
        raise NotImplementedError("upload_results() isn't implemented.")

    #Progresess and reports status of the JobPool and Jobs that are being run
    #or created for submitting to QSUB
    def rotate(self):
        '''For each job;
            if the job is new and we allow to submit(queued plus running jobs)
            the job will get submitted to qsub;
            otherwise the jobs is already submitted or it is terminated.
            If the job is submited and not terminated, then it means that it is
            run or queued in QSUB, so no action should be taken;
            if the job is terminated then we see if errors were reported by QSUB,
            if so check if we could start it again; if not the job is deleted
            due to multiple fails;
            If the job has terminated without errors then the processing is
            assumed to be completed successfuly and upload of the results is called upon the job
        '''
        self.created_jobs_for_files_DB()
        self.update_jobs_status_from_queue()
        self.resubmit_failed_jobs()
        
    def update_jobs_status_from_queue(self):
        #collect all non processed jobs from db linking to downloaded files
        jobs = self.query("SELECT * FROM jobs,job_files,downloads WHERE jobs.status NOT LIKE 'processed' AND jobs.status NOT LIKE 'failed' AND jobs.status NOT LIKE 'terminal_failure' AND jobs.id=job_files.job_id AND job_files.file_id=downloads.id")
        for job in jobs:
            #check if Queue is processing a file for this job
            in_queue,queueidreported = QueueManagerClass.is_processing_file(job['filename'])
            if not in_queue:
                #if it is not processing, collect the last job submit 
                last_job_submit = self.query("SELECT * FROM job_submits WHERE job_id=%u ORDER by id DESC LIMIT 1" % int(job['id']))
                if len(last_job_submit) > 0:
                    #if there was a submit check if the job terminated with an error
                    if QueueManagerClass.error(last_job_submit[0]['queue_id']):
                        #if the job terminated with an error, update it's status to failed
                        if self.get_submits_count_by_job_id(job['id']) < config.max_attempts:
                            self.query("UPDATE jobs SET status='failed', updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job['id'])))
                            if config.email_on_failures:
                                self.mail_job_failure(job['id'],last_job_submit[0]['queue_id'])
                        else:
                            self.query("UPDATE jobs SET status='terminal_failure', updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job['id'])))
                            if config.delete_rawdata:
                                self.delete_jobs_files_by_job_id(job['id'])
                            if config.email_on_terminal_failures:
                                self.mail_job_failure(job['id'],last_job_submit[0]['queue_id'],terminal=True)
                        
                        #also update the last attempt
                        self.query("UPDATE job_submits SET status='failed', details='%s',updated_at='%s' WHERE id=%u" % ("Job terminated with an Error.",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(last_job_submit[0]['id'])))
                    else:
                        #if the job terminated without an error, update it's status to processed
                        self.query("UPDATE jobs SET status='processed', updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job['id'])))
                        #also update the last attempt
                        self.query("UPDATE job_submits SET status='finished',details='%s',updated_at='%s' WHERE id=%u" % ("Job terminated with an Error.",datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(last_job_submit[0]['id'])))
                elif len(last_job_submit) == 0:
                    #the job was never submited, so we submit it
                    running, queued = self.get_queue_status()
                    if (running + queued) < config.max_jobs_running:
                        self.submit(job)
            else:
                #if queue is processing a file for this job update job's status
                self.query("UPDATE jobs SET status='submitted',updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job['id'])))
    
    def delete_jobs_files_by_job_id(self,job_id):
        files = self.query("SELECT * FROM job_files,downloads where job_files.job_id=%u AND job_files.file_id=downloads.id" % (job_id))
        for file_row in files:
            if os.path.exists(file_row['filename']):
                if os.path.isfile(file_row['filename']):
                    try:
                        os.remove(file_row['filename'])
                    except:
                        pass
    
    def mail_job_failure(self,job_id,queue_id,terminal=False):
        stdout_log, stderr_log = QueueManagerClass.getLogs(queue_id)
        if terminal:
            email_content = "Terminal Job Failure. Job will not be retried. File(s) used by this job were deleted.\n JobId: %s\n Last Attempt queue_id: %s\n" % (job_id, queue_id)
        else:
            email_content = "Job Failure\n\nJobId: %s\n Last Attempt queue_id: %s\n" % (job_id, queue_id)
            
        email_content += "\nJob's Datafile(s):\n %s\n" % ("\n".join(self.get_jobs_files_by_job_id(job_id)))

        email_content += "\nStandard Output Log:\n===================start==================\ %s \n====================end===================\
        \n\nStandard Error Log:\n===================start================== %s \n====================end===================\n" % (stdout_log, stderr_log)

        try:
            mailer = ErrorMailer(email_content)
            mailer.send()
        except Exception, e:
            pass
                
    def get_submits_count_by_job_id(self,job_id):
        job_submits = self.query("SELECT * FROM job_submits WHERE job_id=%u" % int(job_id))
        return len(job_submits)
        
    
    def resubmit_failed_jobs(self):
        failed_jobs = self.query("select * FROM jobs,job_files,downloads WHERE jobs.id=job_files.job_id AND job_files.file_id = downloads.id AND jobs.status='failed'")
        for failed_job in failed_jobs:
            running, queued = self.get_queue_status()
            if (running + queued) < config.max_jobs_running:
                self.submit(failed_job)
                
    def get_jobs_files_by_job_id(self,job_id):
        dls = self.query("SELECT * FROM jobs,downloads,job_files WHERE jobs.id=%u AND jobs.id=job_files.job_id AND downloads.id=job_files.file_id" %(int(job_id)))
        files=list()
        for dl in dls:
            files.append(dl['filename'])
        return files
        
    def submit(self,job_row):
        tmp_job = PulsarSearchJob([job_row['filename']])
        try:
            output_dir = tmp_job.get_output_dir()
        except Exception, e:
            jobpool_cout.outs("Error while reading %s. Job will not be submited" % ", ".join(tmp_job.datafiles))
            self.query("INSERT INTO job_submits (job_id,queue_id,output_dir,status,created_at,updated_at) VALUES (%u,'%s','%s','%s','%s','%s')"\
          % (int(job_row['id']),'did_not_queue','could not get output dir','failed',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            self.query("UPDATE jobs SET status='failed',updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job_row['id'])))
            try:
                notification = ErrorMailer("Error while reading %s. Job will not be submited" % ", ".join(tmp_job.datafiles))
                notification.send()
            except Exception,e:
                pass
            return
            
        queue_id = QueueManagerClass.submit([job_row['filename']], output_dir)
        self.query("INSERT INTO job_submits (job_id,queue_id,output_dir,status,created_at,updated_at,base_output_dir) VALUES (%u,'%s','%s','%s','%s','%s','%s')"\
          % (int(job_row['id']),queue_id,output_dir,'running',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),config.base_results_directory ))
        self.query("UPDATE jobs SET status='submitted',updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job_row['id'])))
    
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

    def query(self,query_string):
        not_connected = True
        while not_connected:
            try:
                db_conn = sqlite3.connect(config.bgs_db_file_path,timeout=40.0);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                db_cur.execute(query_string)
                if db_cur.lastrowid:
                    results = db_cur.lastrowid
                else:
                    results = db_cur.fetchall()
                db_conn.commit()
                db_conn.close()
                not_connected = False
            except Exception, e:
                try:
                    db_conn.close()
                except Exception:
                    pass
                jobpool_cout.outs("Couldn't connect to DB retrying in 1 sec.: %s" % str(e)) 
                time.sleep(1)
        return results

    def get_queue_status(self):
        """Connect to the PBS queue and return the number of
            survey jobs running and the number of jobs queued.


            Returns a 2-tuple: (numrunning, numqueued).
        """
        
        return QueueManagerClass.status()
        

#TODO: recreate search jobs from qsub
    def recover_from_qsub(self, testing=False):
        jobpool_cout.outs("Starting Queue Manager Recovering process.")
        datafiles = self.get_datafiles_from_db()
        
        for datafile in datafiles:
            is_processing, jobid = QueueManagerClass.is_processing_file(datafile)
            if is_processing:
                tmp_job = PulsarSearchJob([datafile],testing)
                tmp_job.status = PulsarSearchJob.RUNNING
                tmp_job.jobid = jobid
                self.jobs.append(tmp_job)
                self.datafiles.append(datafile)
                jobpool_cout.outs("Recovered a Search Job %s for: %s" % (jobid,datafile))
        
        jobpool_cout.outs("Job Recovered from Queue Manager: %s" % str(len(self.jobs)))

    #def qsub_status(self, job):
    def check_for_qsub_job_errors(self, job):
        """Check if qsub job terminated with an error.
            Return True if the job terminated with the error, False otherwise.
        """
        
        if QueueManagerClass.error(job.jobid):
            job.log.addentry(LogEntry(qsubid=job.jobid, status="Processing failed", host=socket.gethostname(), \
                info="Job ID: %s" % job.jobid.strip()))
            return True
        else:
            return False
    

    #Determines if the Job should be restarted
    #returns Tdebug.outrue or False
    def can_start_job(self, job):
        #TODO: is this needed. We can change check_for_qsub_job_errors to use filename instead of jobid
        if(job.count_status("deleted") > 0):
            return False

        log_status, job.jobid = job.get_log_status()
        self.check_for_qsub_job_errors(job)
        numfails = job.count_status("processing failed")
        if (numfails > config.max_attempts):
            return False

        return True

    #Fetchs new jobs from datafiles. Only adds jobs for files that are not already
    #being used by another job
    def fetch_new_jobs(self):
        files_to_x_check = self.get_datafiles_from_db()
        for file in self.datafiles:
            if file in files_to_x_check:
                files_to_x_check.remove(file)

        for file in files_to_x_check:
	        try:
	            tmp_job = PulsarSearchJob([file])
	            if not self.can_start_job(tmp_job):
                        jobpool_cout.outs("Removing file: %s" % file)
	                self.delete_job(tmp_job)
	                files_to_x_check.remove(file)
	            else:
                        jobpool_cout.outs("Will not remove file because i can restart the job: %s" % file)
	        except Exception, e:
                    jobpool_cout.outs("Error while creating a PulsarSearchJob: %s" % str(e))
        self.create_jobs_from_datafiles(files_to_x_check)


#The following class represents a search job that is either waiting to be submitted
#to QSUB or is running
class PulsarSearchJob:
    """A single pulsar search job object.
    """
    TERMINATED = 0
    NEW_JOB = 1
    RUNNING = 2
    
    def __init__(self, datafiles, testing=False):
        """PulsarSearchJob creator.
            'datafiles' is a list of data files required for the job.
        """
        
        
        if not issubclass(QueueManagerClass, PipelineQueueManager):
            job_cout.outs("You must derive queue manager class from QueueManagerClass",OutStream.ERROR)
            raise "You must derive queue manager class from QueueManagerClass"
        self.datafiles = datafiles
        if not testing:
            self.jobname = self.get_jobname()
        else:
            self.jobname = datafiles[0]
        self.jobid = None
        #self.logfilenm = self.jobname + ".log"
        if not testing:
            self.logfilenm = os.path.join(config.log_dir,os.path.basename(self.jobname) + ".log")
        else:
            self.logfilenm = os.path.join('/home/snip3/dev/pythonapps/pipeline2.0',os.path.basename(self.jobname) + ".log")
        #self.log = JobLog(self.logfilenm, self)
        self.status = self.NEW_JOB


    def get_output_dir(self):
        """Generate path to output job's results.
            
            path is: 
                {base_results_directory/{mjd}/{obs_name}/{beam_num}/{proc_date}/
            Note: 'base_results_directory' is defined in the config file.
                    'mjd', 'obs_name', and 'beam_num' are from parsing
                    the job's datafiles. 'proc_date' is the current date
                    in YYMMDD format.
        """
	
        data = datafile.autogen_dataobj(self.datafiles)
        if not isinstance(data, datafile.PsrfitsData):
            job_cout.outs("Data must be of PSRFITS format.",OutStream.ERROR)
            raise Exception("Data must be of PSRFITS format.")
        mjd = int(data.timestamp_mjd)
        beam_num = data.beam_id
        obs_name = data.obs_name
        proc_date=datetime.datetime.now().strftime('%y%m%d')
        presto_outdir = os.path.join(config.base_results_directory, str(mjd), \
                                        str(obs_name), str(beam_num), proc_date)
        
        # Directory should be made by rsync when results are 
        # copied by PALFA2_presto_search.py -PL Dec. 26, 2010
        # try:
        #     os.makedirs(presto_out_dir)
        # except OSError:
        #     if not os.path.exists(presto_out_dir):
        #         raise "Could not create directory: %s" % presto_out_dir
        
        return presto_outdir

    #Submit a search job to QSUB
    def submit(self):
        """Submit PulsarSearchJob job to the queue. Update job's log.
        """
        self.jobid = QueueManagerClass.submit(self.datafiles, self.get_output_dir())
    
    def delete(self):
        """Remove PulsarSearchJob job from the queue.
        """
        if self.jobid:
            if QueueManagerClass.delete(self.jobid):
                self.jobid = None
                return True
        return False
       
        
    def get_log_status(self):
        """Get and return the status of the most recent log entry.
        """
        self.log = JobLog(self.logfilenm, self)
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
            job_cout.outs("First data file is not a FITS file!\n(%s)" % datafile0, OutStream.ERROR)
            raise ValueError("First data file is not a FITS file!" \
                             "\n(%s)" % datafile0)
        return jobname
    
    def get_qsub_status(self):
        """Updates job's status according to the PBSQuery
            batch job status.
        """
        if self.jobid == None:
            return
        
        if QueueManagerClass.is_running(self.jobid):
            self.status = PulsarSearchJob.RUNNING
        else:
            self.status = PulsarSearchJob.TERMINATED
        
        return self.status
    
    def queue_error(self):
        """Check if queued job terminated with an error.
            Return True if the job terminated with the error, False otherwise.
        """
        if QueueManagerClass.error(self.jobid):
            self.log.addentry(LogEntry(qsubid=self.jobid, status="Processing failed", host=socket.gethostname(), \
                info="Job ID: %s" % self.jobid.strip()))
            return True
        else:
            return False
        
       


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

#helper function
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
