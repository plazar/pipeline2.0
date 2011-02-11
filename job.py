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

from master_config import bgs_screen_output\
                        , email_on_failures\
                        , email_on_terminal_failures\
                        , delete_rawdata\
                        , bgs_db_file_path\
                        , base_results_directory

from processor_config import rawdata_directory\
                            , max_jobs_running\
                            , max_attempts\
                            , QueueManagerClass

import time
from mailer import ErrorMailer


"""
from QTestManager import QTest
from PipelineQueueManager import PipelineQueueManager
QueueManagerClass = QTest
"""

from OutStream import OutStream as OutStream

jobpool_cout = OutStream("JobPool","background.log",bgs_screen_output)
job_cout = OutStream("Job","background.log",bgs_screen_output)

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

    
    #Returns a list of files that Downloader marked Finished:* in the qlite3db
    def get_datafiles_from_db(self):
        didnt_get_files = True
        tmp_datafiles = []
        while didnt_get_files:
            try:
                db_conn = sqlite3.connect(bgs_db_file_path);
                db_conn.row_factory = sqlite3.Row
                db_cur = db_conn.cursor();
                fin_file_query = "SELECT * FROM downloads WHERE status LIKE 'downloaded'"
                db_cur.execute(fin_file_query)
                row = db_cur.fetchone()
                while row:
                    #print row['filename'] +" "+ row['status']
                    tmp_datafiles.append(os.path.join(rawdata_directory,row['filename']))
                    row = db_cur.fetchone()                
                didnt_get_files = False
		for file in tmp_datafiles:
                        jobpool_cout.outs(file)
                return tmp_datafiles
            except Exception,e:
                jobpool_cout.outs("Database error: %s. Retrying in 1 sec" % str(e), OutStream.ERROR)
    
    def create_jobs_for_files_DB(self):
        files_with_no_jobs = self.query("SELECT * from downloads as d1 where d1.id not in (SELECT downloads.id FROM jobs, job_files, downloads WHERE jobs.id = job_files.job_id AND job_files.file_id = downloads.id) and d1.status = 'downloaded'")
        for file_with_no_job in files_with_no_jobs:
            self.create_job_entry(file_with_no_job)
     
    def create_job_entry(self,file_with_no_job):
        job_id = self.query("INSERT INTO jobs (status,created_at,updated_at) VALUES ('%s','%s','%s')"\
                                % ('new',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        self.query("INSERT INTO job_files (job_id,file_id,created_at,updated_at) VALUES (%u,%u,'%s','%s')"\
                                            % (job_id,file_with_no_job['id'],datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        

    def status(self,log=True):
        running_jobs = self.query("SELECT * FROM jobs WHERE status='submitted'")
        processed_jobs = self.query("SELECT * FROM jobs WHERE status='processed'")
        new_jobs = self.query("SELECT * FROM jobs WHERE status='new'")
        waiting_resubmit_jobs = self.query("SELECT * FROM jobs WHERE status='failed'")
        failed_jobs = self.query("SELECT * FROM jobs WHERE status='terminal_failure'")
        uploaded_jobs = self.query("SELECT * FROM job_uploads WHERE status='uploaded'")
        
        status_str= "\n\n================= Job Pool Status ==============\n"
        status_str+="Num. of jobs       running: %u\n" % len(running_jobs)
        status_str+="Num. of jobs     processed: %u\n" % len(processed_jobs)
        status_str+="Num. of jobs      uploaded: %u\n" % len(uploaded_jobs)
        status_str+="Num. of jobs       waiting: %u\n" % len(new_jobs)
        status_str+="Num. of jobs waiting retry: %u\n" % len(waiting_resubmit_jobs)
        status_str+="Num. of jobs        failed: %u\n" % len(failed_jobs)
        if log:
            jobpool_cout.outs(status_str)
        else:
            print status_str

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
        self.create_jobs_for_files_DB()
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
                        if self.get_submits_count_by_job_id(job['id']) < max_attempts:
                            self.query("UPDATE jobs SET status='failed', updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job['id'])))
                            if email_on_failures:
                                self.mail_job_failure(job['id'],last_job_submit[0]['queue_id'])
                        else:
                            self.query("UPDATE jobs SET status='terminal_failure', updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job['id'])))
                            if delete_rawdata:
                                self.delete_jobs_files_by_job_id(job['id'])
                            if email_on_terminal_failures:
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
                    if (running + queued) < max_jobs_running:
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

        email_content += "\n\nStandard Error Log:\n===================start==================\n %s \n====================end===================\n" % stderr_log
        email_content += "\n\n%s" % stdout_log

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
            if (running + queued) < max_jobs_running:
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
        job_cout.outs("Submitted job to process %s. Returned Queued iD: %s" % (job_row['filename'],queue_id))
        self.query("INSERT INTO job_submits (job_id,queue_id,output_dir,status,created_at,updated_at,base_output_dir) VALUES (%u,'%s','%s','%s','%s','%s','%s')"\
          % (int(job_row['id']),queue_id,output_dir,'running',datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),base_results_directory ))
        self.query("UPDATE jobs SET status='submitted',updated_at='%s' WHERE id=%u" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),int(job_row['id'])))

    def query(self,query_string):
        not_connected = True
        counter = 0
        while not_connected:
            try:
                db_conn = sqlite3.connect(bgs_db_file_path,timeout=40.0);
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
                if counter > 59:
                    jobpool_cout.outs("Couldn't connect to DB retrying in 1 sec.: %s" % str(e)) 
                time.sleep(1)
                counter += 1
        return results

    def get_queue_status(self):
        """Connect to the PBS queue and return the number of
            survey jobs running and the number of jobs queued.


            Returns a 2-tuple: (numrunning, numqueued).
        """
        
        return QueueManagerClass.status()
    


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
        presto_outdir = os.path.join(base_results_directory, str(mjd), \
                                        str(obs_name), str(beam_num), proc_date)
        return presto_outdir

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