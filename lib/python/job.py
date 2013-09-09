#!/usr/bin/env python
"""
A few objects for keeping track of pulsar search jobs.

Patrick Lazarus, June 5th, 2010
"""
import os
import re
import os.path
import datetime
import time
import sys
import traceback
import string

import datafile
import jobtracker
import mailer
import OutStream
import pipeline_utils
import queue_managers
import config.background
import config.processing
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
                            "WHERE files.status IN ('downloaded', 'added') " \
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

                if errormsg.count("\n") > 100:
                    errormsg = string.join(errormsg.split("\n")[:50],"\n")

                jobpool_cout.outs("Processing of Job #%d (Submit ID: %d; Queue ID: %s) " \
                                    "had errors." % \
                                (submit['job_id'], submit['id'], submit['queue_id']))

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
                notification = mailer.ErrorMailer(msg, \
                                subject="Processing failed!")
                notification.send()

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
                    config.email.send_on_failures:
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
                notification = mailer.ErrorMailer(msg, \
                                subject="Processing job failed - Terminal")
                notification.send()

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
    special_query = "SELECT distinct j.* FROM jobs AS j JOIN job_files AS jf " \
                    "ON j.id=jf.job_id JOIN files AS f ON f.id=jf.file_id WHERE " \
                    "j.status in ('new','retrying') AND f.filename LIKE " \
                    "'%p2030.2013____.G__.%.fits' ORDER BY j.updated_at ASC"
    special_query2 = "SELECT distinct j.* FROM jobs AS j JOIN job_files AS jf " \
                    "ON j.id=jf.job_id JOIN files AS f ON f.id=jf.file_id WHERE " \
                    "j.status in ('new','retrying') AND f.filename LIKE " \
                    "'%p2030.20______.G__.%.fits' ORDER BY j.updated_at ASC"
    jobs.extend(jobtracker.query(special_query))
    print len(jobs),"in special query."
    if not len(jobs):
        jobs.extend(jobtracker.query(special_query2))
        print len(jobs),"in special query 2."
    if not len(jobs):
        jobs.extend(jobtracker.query("SELECT * FROM jobs " \
                                     "WHERE status='retrying' " \
                                     "ORDER BY updated_at ASC"))
        jobs.extend(jobtracker.query("SELECT * FROM jobs " \
                                     "WHERE status='new'" \
                                     "ORDER BY updated_at ASC"))

    for job in jobs[:50]:
        if config.jobpooler.queue_manager.can_submit():
            submit(job)
            if config.jobpooler.submit_sleep:
                time.sleep(config.jobpooler.submit_sleep)
        else:
            break

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
        presubmission_check(fns)
        outdir = get_output_dir(fns)
        # Attempt to submit the job
        if config.jobpooler.alternative_submit_script:
            print "Submitting:", config.jobpooler.alternative_submit_script
            queue_id = config.jobpooler.queue_manager.submit\
                        (fns, outdir, job_row['id'],\
                         script=config.jobpooler.alternative_submit_script)
        else:
            queue_id = config.jobpooler.queue_manager.submit\
                        (fns, outdir, job_row['id'])
    except (FailedPreCheckError):
        # Error caught during presubmission check.
        exceptionmsgs = traceback.format_exception(*sys.exc_info())
        errormsg = "Job ID: %d " % job_row['id']
        errormsg += "failed presubmission check!\n\n"
        errormsg += "".join(exceptionmsgs)

        jobpool_cout.outs("Job ID: %d failed presubmission check!\n\t%s\n" % \
                          (job_row['id'], exceptionmsgs[-1])) 
        
        if config.email.send_on_terminal_failures:
            # Send error email
            msg  = "Presubmission check failed!\n"
            msg += "Job ID: %d\n\n" % \
                    (job_row['id'])
            msg += errormsg
            msg += "\n*** Job has been terminally failed. ***\n"
            msg += "*** Job will NOT be re-submitted! ***\n"
            if config.basic.delete_rawdata:
                jobpool_cout.outs("Job #%d will NOT be retried. " \
                                    "Data files will be deleted." % job_row['id'])
                msg += "*** Raw data files will be deleted. ***\n"
            else:
                jobpool_cout.outs("Job #%d will NOT be retried. " % job_row['id'])
            notification = mailer.ErrorMailer(msg, \
                            subject="Job failed presubmission check - Terminal")
            notification.send()

        if config.basic.delete_rawdata:
            pipeline_utils.clean_up(job_row['id'])

        queries = []
        arglist = []
        queries.append("INSERT INTO job_submits (" \
                            "job_id, " \
                            "status, " \
                            "created_at, " \
                            "updated_at, " \
                            "details) " \
                      "VALUES (?, ?, ?, ?, ?)" )
        arglist.append( ( job_row['id'], 'precheck_failed', \
                        jobtracker.nowstr(), jobtracker.nowstr(), \
                        errormsg) )
        queries.append("UPDATE jobs " \
                       "SET status='terminal_failure', " \
                            "details='Failed presubmission check', " \
                            "updated_at=? " \
                       "WHERE id=?" )
        arglist.append( (jobtracker.nowstr(), job_row['id']) )
        jobtracker.execute(queries, arglist)
    except (queue_managers.QueueManagerJobFatalError,\
              datafile.DataFileError):
        # Error caught during job submission.
        exceptionmsgs = traceback.format_exception(*sys.exc_info())
        errormsg  = "Error while submitting job!\n"
        errormsg += "\tJob ID: %d\n\n" % job_row['id']
        errormsg += "".join(exceptionmsgs)

        jobpool_cout.outs("Error while submitting job!\n" \
                          "\tJob ID: %d\n\t%s\n" % \
                          (job_row['id'], exceptionmsgs[-1])) 
        
        queries = []
        arglist = []
        queries.append("INSERT INTO job_submits (" \
                            "job_id, " \
                            "status, " \
                            "created_at, " \
                            "updated_at, " \
                            "details) " \
                      "VALUES (?, ?, ?, ?, ?)" )
        arglist.append( ( job_row['id'], 'submission_failed', \
                        jobtracker.nowstr(), jobtracker.nowstr(), \
                        errormsg) )
        queries.append("UPDATE jobs " \
                       "SET status='failed', " \
                            "details='Error while submitting job', " \
                            "updated_at=? " \
                       "WHERE id=?" )
        arglist.append( (jobtracker.nowstr(), job_row['id']) )
        jobtracker.execute(queries, arglist)
    except queue_managers.QueueManagerNonFatalError:
        # Do nothing. Don't submit the job. Don't mark the job as 'submitted'.
        # Don't mark the job as 'failed'. The job submission will be retried.
        pass
    except queue_managers.QueueManagerFatalError:
        # A fatal error occurred. Re-raise!
        raise
    except:
        # Unexpected error
         sys.stderr.write("Unexpected error during job submission!\n")
         raise 
    else: 
        # No error occurred
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


def get_output_dir(fns):
    """Given a list of data files, 'fns', generate path to output results.

        path is:
            {base_results_directory}/{mjd}/{obs_name}/{beam_num}/{proc_date}/
        Note: 'base_results_directory' is defined in the processing config file.
                'mjd', 'obs_name', and 'beam_num' are from parsing
                the job's datafiles. 'proc_date' is the current date
                in yymmddThhmmss format.
    """

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
    proc_date = datetime.datetime.now().strftime('%y%m%dT%H%M%S')
    baseoutdir = os.path.join(config.processing.base_results_directory, \
                                    str(mjd), str(obs_name), \
                                    str(beam_num), proc_date)
    outdir = baseoutdir
    
    # Make sure our output directory doesn't already exist
    counter = 0
    while os.path.exists(outdir):
        counter += 1
        outdir = "%s_%d" % (baseoutdir, counter)
    
    # Make the directory immediately so the pipeline knows it's taken
    os.makedirs(outdir)

    # Send an email if our first choice for outdir wasn't available
    if counter:
        errormsg = "The first-choice output directory '%s' " \
                    "already existed. Had to settle for '%s' " \
                    "after %d tries. \n\n " \
                    "Data files:\n " \
                    "\t%s" % (baseoutdir, outdir, counter, "\n\t".join(fns))
        notification = mailer.ErrorMailer(errormsg, \
                        subject="Job outdir existance warning")
        notification.send()
    return outdir

def presubmission_check(fns):
    """Check to see if datafiles meet the critera for submission.
    """
    # Check that files exist
    missingfiles = [fn for fn in fns if not os.path.exists(fn)]
    if missingfiles:
        errormsg = "The following files cannot be found:\n"
        for missing in missingfiles:
            errormsg += "\t%s\n" % missing
        raise pipeline_utils.PipelineError(errormsg) # if files missing want to crash
    try:
        #for WAPP, check if coords are in table
        data = datafile.autogen_dataobj([fns[0]])
        if not isinstance(data, datafile.PsrfitsData):
            errormsg  = "Data must be of PSRFITS format.\n"
            errormsg += "\tData type: %s\n" % type(data)
            raise FailedPreCheckError(errormsg)
    except (datafile.DataFileError, ValueError), e:
        raise FailedPreCheckError(e)
    #check if observation is too short
    limit = float(config.jobpooler.obstime_limit)
    obs_time = data.observation_time
    if obs_time < limit:
        errormsg = 'Observation is too short (%.2f s < %.2f s)' % (obs_time, limit) 
        raise FailedPreCheckError(errormsg)
    #check if dynamic zaplist is available
    if not config.processing.use_default_zaplists and \
       not pipeline_utils.find_zaplist_in_tarball(fns[0]):
        errormsg = 'Custom zaplist not available for datafile %s' % (fns[0])
        raise FailedPreCheckError(errormsg)


class FailedPreCheckError(pipeline_utils.PipelineError):
    """Error to raise when datafile has failed the presubmssion check.
        Job should be terminally failed and Cornell (eventually) notified.
    """
    pass
