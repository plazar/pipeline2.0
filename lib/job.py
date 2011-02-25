#!/usr/bin/env python
"""
A few objects for keeping track of pulsar search jobs.

Patrick Lazarus, June 5th, 2010
"""
import os
import re
import os.path
import datetime

import datafile
import config.background
import config.jobpooler
import config.email
import config.basic
import jobtracker
import mailer
import OutStream

jobpool_cout = OutStream.OutStream("JobPool","background.log",config.background.screen_output)
job_cout = OutStream.OutStream("Job","background.log",config.background.screen_output)

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

    def get_datafiles_from_db(self):
        """Returns a list of files that Downloader marked Finished:*
            in the job-tracker db.
        """
        didnt_get_files = True
        tmp_datafiles = []
        while didnt_get_files:
            try:
                fin_file_query = "SELECT * FROM downloads WHERE status LIKE 'downloaded'"
                row = jobtracker.query(fin_file_query, fetchone=True)
                while row:
                    #print row['filename'] +" "+ row['status']
                    tmp_datafiles.append(os.path.join(config.jobpooler.rawdata_directory,row['filename']))
                    row = db_cur.fetchone()
                didnt_get_files = False
		for file in tmp_datafiles:
                        jobpool_cout.outs(file)
                return tmp_datafiles
            except Exception,e:
                jobpool_cout.outs("Database error: %s. Retrying in 1 sec" % str(e), OutStream.ERROR)

    def create_jobs_for_files_DB(self):
        """
        Creates job entries for finished downloads (files)

        Input(s):
            None
        Output(s):
            None
        """
        files_with_no_jobs = jobtracker.query("SELECT * from downloads as d1 where d1.id not in (SELECT downloads.id FROM jobs, job_files, downloads WHERE jobs.id = job_files.job_id AND job_files.file_id = downloads.id) and d1.status = 'downloaded'")
        for file_with_no_job in files_with_no_jobs:
            self.create_job_entry(file_with_no_job)

    def create_job_entry(self,file_with_no_job):
        """
        Creates a single entry for a file.

        Input(s):
            sqllite3.row of a downloads entry
        Output(s):
            None
        """
        job_id = jobtracker.query("INSERT INTO jobs (status,created_at,updated_at) VALUES ('%s','%s','%s')"\
                                % ('new',jobtracker.nowstr(),jobtracker.nowstr()))
        jobtracker.query("INSERT INTO job_files (job_id,file_id,created_at,updated_at) VALUES (%u,%u,'%s','%s')"\
                                            % (job_id,file_with_no_job['id'],jobtracker.nowstr(),jobtracker.nowstr()))

    def status(self,log=True):
        """
        Displays number of jobs processed, uploaded, waiting, waiting retry, failed.

        Input(s):
            Optional:
                log : Default to True, will write to a configured log file,
                        else will only output the information to stdout
        Output(s):
            Displays number of jobs processed, uploaded, waiting, waiting retry, failed.
        """
        running_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='submitted'")
        processed_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='processed'")
        uploaded_jobs = jobtracker.query("SELECT * FROM jobs, job_uploads WHERE " \
                                            "jobs.id=job_uploads.job_id AND " \
                                            "jobs.status='processed' AND " \
                                            "job_uploads.status='uploaded'")
        new_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='new'")
        waiting_resubmit_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='failed'")
        failed_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='terminal_failure'")

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

    def rotate(self):
        """For each job;
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
        """
        self.create_jobs_for_files_DB()
        self.update_jobs_status_from_queue()
        self.resubmit_failed_jobs()
        self.submit_new_jobs()

    def update_jobs_status_from_queue(self):
        """
        Updates Database entries for job processing according to the Jobs' Queue Status.

        Input(s):
            None
        Output(s):
            None
        """

        #collect all non processed jobs from db linking to downloaded files
        jobs = jobtracker.query("SELECT * FROM jobs,job_files,downloads WHERE jobs.status NOT LIKE 'processed' AND jobs.status NOT LIKE 'new' AND jobs.status NOT LIKE 'failed' AND jobs.status NOT LIKE 'terminal_failure' AND jobs.status NOT LIKE 'uploaded' AND jobs.id=job_files.job_id AND job_files.file_id=downloads.id")
        for job in jobs:
            #check if Queue is processing a file for this job
            in_queue,queueidreported = config.jobpooler.queue_manager.is_processing_file(job['filename'])
            if not in_queue:
                #if it is not processing, collect the last job submit
                last_job_submit = jobtracker.query("SELECT * FROM job_submits WHERE job_id=%u ORDER by id DESC LIMIT 1" % int(job['id']))
                if len(last_job_submit) > 0:
                    #if there was a submit check if the job terminated with an error
                    if config.jobpooler.queue_manager.had_errors(last_job_submit[0]['queue_id']):
                        #if the job terminated with an error, update it's status to failed
                        if self.get_submits_count_by_job_id(job['id']) < config.jobpooler.max_attempts:
                            jobtracker.query("UPDATE jobs SET status='failed', updated_at='%s' WHERE id=%u" % (jobtracker.nowstr(),int(job['id'])))
                            if config.email.send_on_failures:
                                self.mail_job_failure(job['id'],last_job_submit[0]['queue_id'])
                        else:
                            jobtracker.query("UPDATE jobs SET status='terminal_failure', updated_at='%s' WHERE id=%u" % (jobtracker.nowstr(),int(job['id'])))
                            if config.email.send_on_terminal_failures:
                                self.mail_job_failure(job['id'],last_job_submit[0]['queue_id'],terminal=True)
                        #also update the last attempt
                        jobtracker.query("UPDATE job_submits SET status='failed', details='%s',updated_at='%s' WHERE id=%u" % ("Job terminated with an Error.",jobtracker.nowstr(),int(last_job_submit[0]['id'])))
                    else:
                        #if the job terminated without an error, update it's status to processed
                        jobtracker.query("UPDATE jobs SET status='processed', updated_at='%s' WHERE id=%u" % (jobtracker.nowstr(),int(job['id'])))
                        #also update the last attempt
                        jobtracker.query("UPDATE job_submits SET status='finished',details='%s',updated_at='%s' WHERE id=%u" % ("Job terminated with an Error.",jobtracker.nowstr(),int(last_job_submit[0]['id'])))
            else:
                #if queue is processing a file for this job update job's status
                jobtracker.query("UPDATE jobs SET status='submitted',updated_at='%s' WHERE id=%u" % (jobtracker.nowstr(),int(job['id'])))

    def delete_jobs_files_by_job_id(self,job_id):
        """
        Deletes file from the file system for a given job.

        Input(s):
            job_id: Job's entry id
        Output(s):
            None
        """

        files = self.query("SELECT * FROM job_files,downloads where job_files.job_id=%u AND job_files.file_id=downloads.id" % (job_id))
        for file_row in files:
            if os.path.exists(file_row['filename']):
                if os.path.isfile(file_row['filename']):
                    try:
                        os.remove(file_row['filename'])
                    except:
                        pass

    def mail_job_failure(self,job_id,queue_id,terminal=False):
        """
        Mails notification of failure/terminal failure to the Pipeline's supervisor.

        Input(s):
            job_id: Job's entry id
            queue_id: Job's id reported by Queue Manager
            Option:
                boolean terminal: When True will email notification of terminal failure,
                                    regular failure is email otherwise.
        Output(s):
            None
        """

        stderr_log = config.jobpooler.queue_manager.read_stderr_log(queue_id)
        if terminal:
            email_content = "Terminal Job Failure. \n\n" \
                            "*** Job will not be retried! ***\n"
            if config.basic.delete_rawdata:
                email_content += "File(s) used by this job were deleted.\n"
        else:
            email_content = "Job Failure\n\n"
        email_content += "JobId: %s\n" % job_id
        email_content += "Last Attempt queue_id: %s\n\n" % queue_id
        email_content += "Job's Datafile(s):\n\t%s\n" % ("\n\t".join(self.get_jobs_files_by_job_id(job_id)))
        email_content += "Error Log file path: %s\n" % config.jobpooler.queue_manager.get_stderr_path(queue_id)
        email_content += "\nStandard Error Log:\n"
        email_content += "===================start==================\n"
        email_content += stderr_log
        email_content += "\n====================end===================\n"

        mailer.ErrorMailer(email_content).send()

    def get_submits_count_by_job_id(self,job_id):
        """
        Returns number of submit attempts to the Queue Manager for a given job.

        Input(s):
            int job_id: Job's entry id
        Output(s):
            int number of submits for a given job entry.
        """

        job_submits = jobtracker.query("SELECT * FROM job_submits WHERE job_id=%u" % int(job_id))
        return len(job_submits)

    def submit_new_jobs(self):
        """
        Submits new jobs to the Queue Manager for processing.

        Input(s):
            None
        Output(s):
            None
        """
        new_jobs = jobtracker.query("select * FROM jobs,job_files,downloads WHERE jobs.id=job_files.job_id AND job_files.file_id = downloads.id AND jobs.status='new'")
        for new_job in new_jobs:
            if self.can_submit():
                self.submit(new_job)

    def resubmit_failed_jobs(self):
        """
        ReSubmits failed jobs to the Queue Manager for re-processing.

        Input(s):
            None
        Output(s):
            None
        """

        failed_jobs = jobtracker.query("select * FROM jobs,job_files,downloads WHERE jobs.id=job_files.job_id AND job_files.file_id = downloads.id AND jobs.status='failed'")
        for failed_job in failed_jobs:
            if self.can_submit():
                self.submit(failed_job)

    def can_submit(self):
        """Check if we can submit a job
            (i.e. limits imposed in config file aren't met)

            Inputs:
                None

            Output:
                Boolean value. True if submission is allowed.
        """
        running, queued = config.jobpooler.queue_manager.status()
        if ((running + queued) < config.jobpooler.max_jobs_running) and \
            (queued < config.jobpooler.max_jobs_queued):
            return True
        else:
            return False

    def get_jobs_files_by_job_id(self,job_id):
        """
        Returns files associated with the given job.

        Input(s):
            int job_id: Job's entry id
        Output(s):
            array of strings: Array of strings representing file paths belonging to a job entry.
        """

        dls = jobtracker.query("SELECT * FROM jobs,downloads,job_files WHERE jobs.id=%u AND jobs.id=job_files.job_id AND downloads.id=job_files.file_id" %(int(job_id)))
        files=list()
        for dl in dls:
            files.append(dl['filename'])
        return files

    def submit(self,job_row):
        """
        Submits a job to QueueManager, if successful will store returned queue id.

        Input(s):
            sqlite3.row job_row: Contains jobs, downloads associated tables (via job_files).
        Output(s):
            None
        """

        fns = [job_row['filename']]
        missingfiles = [fn for fn in fns if not os.path.exists(fn)]
        if not missingfiles:
            tmp_job = PulsarSearchJob(fns)
        else:
            jobtracker.query("INSERT INTO job_submits (job_id,queue_id,output_dir,status,created_at,updated_at) VALUES (%u,'%s','%s','%s','%s','%s')"\
          % (int(job_row['id']),'did_not_queue','some job files do not exist','failed',jobtracker.nowstr(),jobtracker.nowstr()))
            jobtracker.query("UPDATE jobs SET status='failed',updated_at='%s' WHERE id=%u" % (jobtracker.nowstr(),int(job_row['id'])))
            try:
                notification = mailer.ErrorMailer("Some of job's data files no longer exist (%s)!. Job will not be submited" % ", ".join(missingfiles))
                notification.send()
            except Exception,e:
                pass
            return
            
        try:
            output_dir = tmp_job.get_output_dir()
        except Exception, e:
            jobpool_cout.outs("Error while reading %s. Job will not be submited" % ", ".join(tmp_job.datafiles))
            jobtracker.query("INSERT INTO job_submits (job_id,queue_id,output_dir,status,created_at,updated_at) VALUES (%u,'%s','%s','%s','%s','%s')"\
          % (int(job_row['id']),'did_not_queue','could not get output dir','failed',jobtracker.nowstr(),jobtracker.nowstr()))
            jobtracker.query("UPDATE jobs SET status='failed',updated_at='%s' WHERE id=%u" % (jobtracker.nowstr(),int(job_row['id'])))
            try:
                notification = mailer.ErrorMailer("Error while reading %s. Job will not be submited" % ", ".join(tmp_job.datafiles))
                notification.send()
            except Exception,e:
                pass
            return

        queue_id = config.jobpooler.queue_manager.submit([job_row['filename']], output_dir)
        job_cout.outs("Submitted job to process:\n " \
                        "\tData file: %s.\n\tJob ID: %s" % \
                        (job_row['filename'],queue_id))
        jobtracker.query("INSERT INTO job_submits (job_id,queue_id,output_dir,status,created_at,updated_at,base_output_dir) VALUES (%u,'%s','%s','%s','%s','%s','%s')"\
          % (int(job_row['id']),queue_id,output_dir,'running',jobtracker.nowstr(),jobtracker.nowstr(),config.jobpooler.base_results_directory ))
        jobtracker.query("UPDATE jobs SET status='submitted',updated_at='%s' WHERE id=%u" % (jobtracker.nowstr(),int(job_row['id'])))

    def get_queue_status(self):
        """Connect to the PBS queue and return the number of
            survey jobs running and the number of jobs queued.

            Returns a 2-tuple: (numrunning, numqueued).
        """
        return config.jobpooler.queue_manager.status()


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
            raise TypeError("Data must be of PSRFITS format.")
        mjd = int(data.timestamp_mjd)
        beam_num = data.beam_id
        obs_name = data.obs_name
        proc_date=datetime.datetime.now().strftime('%y%m%d')
        presto_outdir = os.path.join(config.jobpooler.base_results_directory, str(mjd), \
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
