#!/usr/bin/env python
"""
A few objects for keeping track of pulsar search jobs.

Patrick Lazarus, June 5th, 2010
"""
import os
import re
import os.path
import datetime
import sys
import traceback

import datafile
import jobtracker
import mailer
import OutStream
import pipeline_utils
import config.background
import config.jobpooler
import config.email
import config.basic

jobpool_cout = OutStream.OutStream("JobPool", \
                    os.path.join(config.basic.log_dir, "jobpooler.log"), \
                    config.background.screen_output)

def status(log=True):
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
    uploaded_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='uploaded'")
    new_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='new'")
    failed_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='failed'")
    retrying_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='retrying'")
    dead_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='terminal_failure'")

    status_str= "\n\n================= Job Pool Status ==============\n"
    status_str+="Num. of jobs            running: %d\n" % len(running_jobs)
    status_str+="Num. of jobs          processed: %d\n" % len(processed_jobs)
    status_str+="Num. of jobs           uploaded: %d\n" % len(uploaded_jobs)
    status_str+="Num. of jobs            waiting: %d\n" % len(new_jobs)
    status_str+="Num. of jobs      waiting retry: %d\n" % len(retrying_jobs)
    status_str+="Num. of jobs             failed: %d\n" % len(failed_jobs)
    status_str+="Num. of jobs permanently failed: %d\n" % len(dead_jobs)
    if log:
        jobpool_cout.outs(status_str)
    else:
        print status_str

def create_jobs_for_new_files():
    """Check job-tracker DB for newly downloaded files. Group
        jobs that belong to the same observation and create
        entries in the jobs table.
    """
    # Get files that aren't already associated with a job
    rows = jobtracker.query("SELECT filename FROM files " \
                            "LEFT JOIN job_files " \
                                "ON job_files.file_id=files.id " \
                            "WHERE files.status='downloaded' " \
                                "AND job_files.id IS NULL")
    newfns = [str(row['filename']) for row in rows]

    # Group together files that belong together
    groups = datafile.group_files(newfns)

    # Keep only groups that are not missing any files
    complete_groups = [grp for grp in groups if datafile.is_complete(grp)]

    if complete_groups:
        jobpool_cout.outs("Inserting %d new entries into jobs table" % \
                            len(complete_groups))
    for complete in complete_groups:
        # Insert new job and link it to data files
        queries = []
        queries.append("INSERT INTO jobs (" \
                            "created_at, " \
                            "details, " \
                            "status, " \
                            "updated_at) " \
                       "VALUES ('%s', '%s', '%s', '%s')" % \
                        (jobtracker.nowstr(), 'Newly created job', \
                            'new', jobtracker.nowstr()))
        queries.append("INSERT INTO job_files (" \
                            "file_id, " \
                            "created_at, " \
                            "job_id, " \
                            "updated_at) " \
                       "SELECT id, '%s', (SELECT LAST_INSERT_ROWID()), '%s' " \
                       "FROM files " \
                       "WHERE filename IN ('%s')" % \
                       (jobtracker.nowstr(), jobtracker.nowstr(), \
                        "', '".join(complete)))
        jobtracker.query(queries)

def rotate():
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
    create_jobs_for_new_files()
    update_jobs_status_from_queue()
    recover_failed_jobs()
    submit_jobs()

def update_jobs_status_from_queue():
    """
    Updates Database entries for job processing according to the Jobs' Queue Status.
    """

    # Collect all non processed jobs from db linking to downloaded files
    submits = jobtracker.query("SELECT * FROM job_submits " \
                               "WHERE status='running'")
    for submit in submits:
        # Check if job is still running (according to queue manager)
        is_running = config.jobpooler.queue_manager.is_running(submit['queue_id'])
        if is_running:
            # Do nothing.
            pass
        else:
            # Check if processing had errors
            if config.jobpooler.queue_manager.had_errors(submit['queue_id']):
                # Errors during processing...
                errormsg = config.jobpooler.queue_manager.get_errors(submit['queue_id'])

                jobpool_cout.outs("Processing of Job #%d (%s) had errors." % \
                                (submit['job_id'], submit['queue_id']))

                # Mark job entry with status 'failed'
                # Mark job_submit entry with status 'processing_failed'
                queries = []
                arglists = []
                queries.append("UPDATE jobs " \
                               "SET status='failed', " \
                                    "updated_at=?, " \
                                    "details='Errors during processing' " \
                               "WHERE id=?")
                arglists.append((jobtracker.nowstr(), submit['job_id']))
                queries.append("UPDATE job_submits " \
                               "SET status='processing_failed', " \
                                    "details=?, " \
                                    "updated_at=? " \
                               "WHERE id=?")
                arglists.append((errormsg, jobtracker.nowstr(), submit['id']))
                jobtracker.execute(queries, arglists)
            else:
                # No errors. Woohoo!
                # Mark job and job_submit entries with status 'processed'
                queries = []
                queries.append("UPDATE jobs " \
                               "SET status='processed', " \
                                    "updated_at='%s', " \
                                    "details='Processed without errors' " \
                               "WHERE id=%d" % \
                            (jobtracker.nowstr(), submit['job_id']))
                queries.append("UPDATE job_submits " \
                               "SET status='processed', " \
                                    "updated_at='%s', " \
                                    "details='Processed without error' " \
                               "WHERE id=%d" % \
                            (jobtracker.nowstr(), submit['id']))
                jobtracker.query(queries)

def recover_failed_jobs():
    """Gather jobs with status 'failed' from the job-tracker DB.
        For each of these jobs see if it can be re-submitted.
        If it can, set the status to 'retrying'. If the
        job cannot be re-submitted, set the status to 'terminal_failure',
        and delete the raw data (if config is set for deletion).

        Depending on configurations emails may be sent.
    """
    failed_jobs = jobtracker.query("SELECT * FROM jobs " \
                                   "WHERE status='failed'")

    for job in failed_jobs:
        # Count the number of times this job has been submitted already
        submits = jobtracker.query("SELECT * FROM job_submits " \
                                   "WHERE job_id=%d " \
                                   "ORDER BY id DESC" % job['id'])
        if len(submits) < config.jobpooler.max_attempts:
            # We can re-submit this job. 
            if config.email.send_on_failures:
                # Send error email
                msg  = "Error! Job submit status: %s\n" % \
                            submits[0]['status']
                msg += "Job ID: %d, Job submit ID: %d\n\n" % \
                        (job['id'], submits[0]['id'])
                msg += str(submits[0]['details'])
                msg += "\n*** Job will be re-submitted to the queue ***\n"
                mailer.ErrorMailer(msg).send()

            # Set status to 'retrying'.
            jobtracker.query("UPDATE jobs " \
                             "SET status='retrying', " \
                                  "updated_at='%s', " \
                                  "details='Job will be retried' " \
                             "WHERE id=%d" % \
                             (jobtracker.nowstr(), job['id']))
            jobpool_cout.outs("Job #%d will be retried." % job['id'])
        else:
            # We've run out of attempts for this job
            if config.email.send_on_terminal_failures or \
                    config.email.send_on_failure:
                # Send error email
                msg  = "Error! Job submit status: %s\n" % \
                            str(submits[0]['status'])
                msg += "Job ID: %d, Job submit ID: %d\n\n" % \
                        (job['id'], submits[0]['id'])
                msg += str(submits[0]['details'])
                msg += "\n*** No more attempts for this job. ***\n"
                msg += "*** Job will NOT be re-submitted! ***\n"
                if config.basic.delete_rawdata:
                    jobpool_cout.outs("Job #%d will NOT be retried. " \
                                        "Data files will be deleted." % job['id'])
                    msg += "*** Raw data files will be deleted. ***\n"
                else:
                    jobpool_cout.outs("Job #%d will NOT be retried. " % job['id'])
                mailer.ErrorMailer(msg).send()

            if config.basic.delete_rawdata:
                pipeline_utils.clean_up(job['id'])

            # Set status to 'terminal_failure'.
            jobtracker.query("UPDATE jobs " \
                             "SET status='terminal_failure', " \
                                  "updated_at='%s', " \
                                  "details='Job has failed permanently' " \
                             "WHERE id=%d" % \
                             (jobtracker.nowstr(), job['id']))


def submit_jobs():
    """
    Submits jobs to the queue for processing.
    
    ***NOTE: Priority is given to jobs with status 'retrying'.
    """
    jobs = []
    jobs.extend(jobtracker.query("SELECT * FROM jobs " \
                                 "WHERE status='retrying' " \
                                 "ORDER BY updated_at ASC"))
    jobs.extend(jobtracker.query("SELECT * FROM jobs " \
                                 "WHERE status='new'" \
                                 "ORDER BY updated_at ASC"))
    for job in jobs:
        if can_submit():
            submit(job)
        else:
            break

def can_submit():
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

def submit(job_row):
    """
    Submits a job to QueueManager, if successful will store returned queue id.

    Input:
        job_row: A row from the jobs table. The datafiles associated
            with this job will be submitted to be processed.
    Outputs:
        None
    """
    fns = pipeline_utils.get_fns_for_jobid(job_row['id']) 
    
    try:
        outdir = get_output_dir(fns)
        # Submit job
        queue_id = config.jobpooler.queue_manager.submit(fns, outdir)
        msg  = "Submitted job to process:\n" 
        msg += "\tJob ID: %d, Queue ID: %s\n" % (job_row['id'], queue_id) 
        msg += "\tData file(s):\n" 
        for fn in fns:
            msg += "\t%s\n" % fn
        jobpool_cout.outs(msg)
        queries = []
        queries.append("INSERT INTO job_submits (" \
                            "job_id, " \
                            "queue_id, " \
                            "output_dir, " \
                            "status, " \
                            "created_at, " \
                            "updated_at, " \
                            "details) " \
                      "VALUES (%d,'%s','%s','%s','%s','%s','%s')" % \
                      (job_row['id'], queue_id, outdir, 'running', \
                        jobtracker.nowstr(), jobtracker.nowstr(), \
                        'Job submitted to queue'))
        queries.append("UPDATE jobs " \
                       "SET status='submitted', " \
                            "details='Job submitted to queue', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                    (jobtracker.nowstr(), job_row['id']))
        jobtracker.query(queries)
    except pipeline_utils.PipelineError:
        # Error caught during job submission.
        exceptionmsgs = traceback.format_exception(*sys.exc_info())
        errormsg  = "Error while submitting job!\n"
        errormsg += "\tJob ID: %d\n\n" % job_row['id']
        errormsg += "".join(exceptionmsgs)

        jobpool_cout.outs("Error while submitting job!\n" \
                          "\tJob ID: %d\n\t%s\n" % \
                          (job_row['id'], exceptionmsgs[-1])) 
        
        queries = []
        queries.append("INSERT INTO job_submits (" \
                            "job_id, " \
                            "status, " \
                            "created_at, " \
                            "updated_at, " \
                            "details) " \
                      "VALUES (%d,'%s','%s','%s','%s')" % \
                      (job_row['id'], 'submission_failed', \
                        jobtracker.nowstr(), jobtracker.nowstr(), \
                        errormsg))
        queries.append("UPDATE jobs " \
                       "SET status='failed', " \
                            "details='Error while submitting job', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                    (jobtracker.nowstr(), job_row['id']))
        jobtracker.query(queries)

def get_output_dir(fns):
    """Given a list of data files, 'fns', generate path to output results.

        path is:
            {base_results_directory}/{mjd}/{obs_name}/{beam_num}/{proc_date}/
        Note: 'base_results_directory' is defined in the config file.
                'mjd', 'obs_name', and 'beam_num' are from parsing
                the job's datafiles. 'proc_date' is the current date
                in YYMMDD format.
    """
    # Check that files exist
    missingfiles = [fn for fn in fns if not os.path.exists(fn)]
    if missingfiles:
        errormsg = "The following files cannot be found:\n"
        for missing in missingfiles:
            errormsg += "\t%s\n" % missing
        raise pipeline_utils.PipelineError(errormsg)

    # Get info from datafile headers
    data = datafile.autogen_dataobj([fns[0]])
    if not isinstance(data, datafile.PsrfitsData):
        errormsg  = "Data must be of PSRFITS format.\n"
        errormsg += "\tData type: %s\n" % type(data)
        raise pipeline_utils.PipelineError(errormsg)

    # Generate output directory
    mjd = int(data.timestamp_mjd)
    beam_num = data.beam_id
    obs_name = data.obs_name
    proc_date=datetime.datetime.now().strftime('%y%m%d')
    outdir = os.path.join(config.jobpooler.base_results_directory, \
                                    str(mjd), str(obs_name), \
                                    str(beam_num), proc_date)
    return outdir

