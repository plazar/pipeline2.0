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
import SPAN512_job
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
import config.searching

jobpool_cout = OutStream.OutStream("JobPool", \
                    os.path.join(config.basic.log_dir, "jobpooler.log"), \
                    config.background.screen_output)

def status(log=True):
    """
    Displays number of jobs finished, uploaded, waiting, waiting retry, failed.

    Input(s):
        Optional:
            log : Default to True, will write to a configured log file,
                    else will only output the information to stdout
    Output(s):
        Displays number of jobs finished, uploaded, waiting, waiting retry, failed.
    """
    running_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='submitted'")
    finished_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='finished'")
    uploaded_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='uploaded'")
    new_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='new'")
    failed_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='failed'")
    retrying_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='retrying'")
    dead_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='terminal_failure'")

    status_str= "\n\n================= Job Pool Status ==============\n"
    status_str+="Num. of jobs            running: %d\n" % len(running_jobs)
    status_str+="Num. of jobs           finished: %d\n" % len(finished_jobs)
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
    # Get files that were not associated with a job yet
    rows = jobtracker.query("SELECT filename FROM files " \
                            "LEFT JOIN job_files " \
                                "ON job_files.file_id=files.id " \
                            "WHERE files.status IN ('downloaded', 'added') " \
                                "AND job_files.id IS NULL")
    newfns = [str(row['filename']) for row in rows]

    # Group together files that belong together
    groups = datafile.simple_group_files(newfns)

    # Keep only groups that are not missing any files
    complete_groups = [grp for grp in groups if SPAN512_job.is_complete(grp)]

    if complete_groups:
        jobpool_cout.outs("Inserting %d new entries into jobs table" % \
                            len(complete_groups))

    # Label the first task
    task_name = 'rfifind'

    for complete in complete_groups:
        # Insert new job and link it to data files
        queries = []
        queries.append("INSERT INTO jobs (" \
                            "created_at, " \
                            "details, " \
                            "status, " \
                            "task, " \
                            "updated_at) " \
                       "VALUES ('%s', '%s', '%s', '%s', '%s')" % \
                        (jobtracker.nowstr(), 'Newly created job', \
                            'new', task_name, jobtracker.nowstr()))
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

def create_parallel_search_jobs():
    """Check job-tracker DB for processed jobs. Submit 
	successive jobs and create
        entries in the jobs table.
    """
    # Look for job with rfifind done
    rows = jobtracker.query("SELECT * from jobs " \
				"WHERE status='processed' " \
				"AND task='rfifind'")
   
    queries = []
    for row in rows:

        # retrieve file_ids
        rows2 = jobtracker.query("SELECT * from job_files " \
				"WHERE job_id=%d'"%row['job_id'])

        files_ids = [str(row2['file_id']) for row2 in rows2]

	# Submit all parallel jobs (1 job per DDplan)
        for istep in range(len(config.searching.ddplans['nuppi'])): 
            task_name = "search %d"%istep # TODO
            queries.append("INSERT INTO jobs (" \
                            "created_at, " \
                            "details, " \
                            "status, " \
                            "task, " \
                            "updated_at) " \
                       "VALUES ('%s', '%s', '%s', '%s')" % \
                        (jobtracker.nowstr(), 'Newly created job', \
                            'new', task_name, jobtracker.nowstr()))

            for file_id in file_ids:
                queries.append("INSERT INTO job_files (" \
                                "file_id, " \
                                "created_at, " \
                                "job_id, " \
                                "updated_at) " \
                           "%d, '%s', (SELECT LAST_INSERT_ROWID()), '%s' " %\
                           (file_id, jobtracker.nowstr(), jobtracker.nowstr(), \
                            "', '".join(complete)))

	# Mark the previous task as 'done'
	queries.append("UPDATE jobs " \
			   "SET status='done', " \
			   "updated_at='%s', " \
			   "details='Processed without errors' " \
			   "WHERE id=%d" % \
                           (jobtracker.nowstr(), row['job_id']))
    jobtracker.query(queries)
			    
    
def create_sifting_jobs():
    """Check job-tracker DB for processed jobs. Submit 
	successive jobs and create
        entries in the jobs table.
    """

    # First make sur that all plans are done
    rows = jobtracker.query("SELECT * from jobs " \
				"WHERE status='processed' " \
				"AND 'search' like task")
    # TODO: how to find out that the parallel task are done ?				

    rows = jobtracker.query("SELECT jobs.task, job_files.file_id  FROM jobs " \
                            "LEFT JOIN job_files " \
                            "ON job_files.job_id=jobs.id " \
                            "WHERE jobs.status='processed' and 'search' LIKE jobs.task")


def check_parallel_jobs(task, rows):
    # This should go into a new function
    # Now sort the details - Check that all parallel jobs are done
    # TODO: Should find something more clever !!
    a = {}
    for row in rows:
        if a.has_key(row['file_id']):
	    a[row['file_id']].append(row['task'])
	else:
	    a[row['file_id']] = []   
	    a[row['file_id']].append(row['task'])

    keys = a.keys()	    
    keys.sort()
    
    # Expected list of plans
    plans = ["search %d"%i for i in range(config.searching.ddplans['nuppi'])]

    finished_files = []
    for key in keys:
        # Len of the list should be equal to Number of plans
	finished_plans = [dbplan for dbplan in dbplans if dbplan in plans]
	if len(finished_plans) == len(config.searching.ddplans['nuppi']):
	    finished_files.append(key)
	    

    # TODO : Should return the good arrays
    return finished_files
        

def create_parallel_folding_jobs():
    """Check job-tracker DB for processed jobs. Submit 
	successive jobs and create
        entries in the jobs table.
    """
    # Look for job with rfifind done
    rows = jobtracker.query("SELECT * from jobs " \
				"WHERE status='processed' " \
				"AND task='sifting'")
   
    queries = []
    for row in rows:

        # retrieve file_ids
        rows2 = jobtracker.query("SELECT * from job_files " \
				"WHERE job_id=%d'"%row['job_id'])

        files_ids = [str(row2['file_id']) for row2 in rows2]

	# Submit all parallel jobs ()
        for istep in range(config.searching.ddplans['nuppi']): 
            task_name = "folding %d"%istep # TODO
            queries.append("INSERT INTO jobs (" \
                            "created_at, " \
                            "details, " \
                            "status, " \
                            "task, " \
                            "updated_at) " \
                       "VALUES ('%s', '%s', '%s', '%s')" % \
                        (jobtracker.nowstr(), 'Newly created job', \
                            'new', task_name, jobtracker.nowstr()))

            for file_id in file_ids:
                queries.append("INSERT INTO job_files (" \
                                "file_id, " \
                                "created_at, " \
                                "job_id, " \
                                "updated_at) " \
                           "%d, '%s', (SELECT LAST_INSERT_ROWID()), '%s' " %\
                           (file_id, jobtracker.nowstr(), jobtracker.nowstr(), \
                            "', '".join(complete)))

	# Mark the previous task as 'done'
	queries.append("UPDATE jobs " \
			   "SET status='done', " \
			   "updated_at='%s', " \
			   "details='Processed without errors' " \
			   "WHERE id=%d" % \
                           (jobtracker.nowstr(), row['job_id']))
    jobtracker.query(queries)


def create_jobs():
    """
    """
    # Create initial jobs for newly downloaded or added files
    create_jobs_for_new_files()

    # Create parallel jobs after rfifind
    create_parallel_search_jobs()

    # Create single sifting job after all accelsearch
    create_sifting_jobs()

    # Create parallel folding jobs after sifting 
    create_parallel_folding_jobs()


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
    create_jobs()
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
    jobs.extend(jobtracker.query("SELECT * FROM jobs " \
                                 "WHERE status='retrying' " \
                                 "ORDER BY updated_at ASC"))
    jobs.extend(jobtracker.query("SELECT * FROM jobs " \
                                 "WHERE status='new'" \
                                 "ORDER BY updated_at ASC"))
    for job in jobs:
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

    script = os.path.join(config.basic.pipelinedir, 'bin', '%s_search.py'%config.basic.survey)

    # Specify requested resources for job submission
    if job_row['task']=='rfifind':
        res = [4*60*60, 1024, 24]
    elif search in job_row['task']:
        res = [24*60*60, 1024, 24]
    elif job_row['task']=='sifting': # Sifting should be quick
        res = [30*60, 256, 5]
    elif folding in job_row['task']:
        res = [4*60*60, 1024, 24]
    #elif job_row['task']=='tidyup':
    #    res = [30*60, 256, 5]
    options = job_row['task']
    
    res = []
    
    try:
        SPAN512_job.presubmission_check(fns)
        outdir = SPAN512_job.get_output_dir(fns)
        # Attempt to submit the job
        queue_id = config.jobpooler.queue_manager.submit\
                            (fns, outdir, job_row['id'], resources=res, script=script, opts=options)
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



class FailedPreCheckError(pipeline_utils.PipelineError):
    """Error to raise when datafile has failed the presubmssion check.
        Job should be terminally failed and Cornell (eventually) notified.
    """
    pass
