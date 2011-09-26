#!/usr/bin/env python

import sys
import optparse

import jobtracker
import pipeline_utils
import config.jobpooler

"""
This script allows safe/force-fail removal of the job from the Pipeline
"""

def main():
    jobsubmit_ids = options.submit_ids
    queue_ids = args + options.queue_ids
    for queue_id in queue_ids:
        qids = jobtracker.query("SELECT id " \
                                "FROM job_submits " \
                                "WHERE queue_id LIKE '%s'" % queue_id)
        if len(qids) != 1:
            sys.stderr.write("Bad number (%d) of job submissions for queue " \
                                "ID provided: %s\nSkipping...\n" % (len(qids), queue_id))
        else:
            jobsubmit_ids.append(qids[0]['id'])
        
    for jobsubmit_id in jobsubmit_ids:
        job_submits = jobtracker.query("SELECT id, job_id, status, queue_id " \
                                       "FROM job_submits " \
                                       "WHERE id LIKE '%s'" % jobsubmit_id)
        if len(job_submits) != 1:
            sys.stderr.write("Bad number (%d) of job submissions for job submit " \
                                "ID provided: %s\nSkipping...\n" % (len(job_submits), jobsubmit_id))
            continue
        elif config.jobpooler.queue_manager.is_running(job_submits[0]['queue_id']):
            isrunning = True
        elif job_submits[0]['status'] == 'processed' and options.also_processed:
            isrunning = False
        else:
            sys.stderr.write("The job submit ID/queue ID provided is invalid. " \
                             "This code only allows jobs currently running " \
                             "(i.e. in the queue), or job submits with " \
                             "status='processed' (if the --also-processed flag " \
                             "is given), to be stopped/failed. Sorry. Please " \
                             "try again!\nSkipping...\n")
            continue

        queries = []
        if options.fail:
            queries.append("UPDATE job_submits " \
                           "SET status='stopped', " \
                                "updated_at='%s', " \
                                "details='Job was manually failed' " \
                           "WHERE id=%d" % \
                            (jobtracker.nowstr(), job_submits[0]['id']))
            queries.append("UPDATE jobs " \
                           "SET status='failed', " \
                                "updated_at='%s', " \
                                "details='Job was manually failed' " \
                           "WHERE id=%d" % \
                            (jobtracker.nowstr(), job_submits[0]['job_id']))
        else:
            queries.append("DELETE FROM job_submits " \
                           "WHERE id=%d" % job_submits[0]['id'])
            queries.append("UPDATE jobs " \
                           "SET status='retrying', " \
                                "updated_at='%s', " \
                                "details='Job was manually removed, politely' " \
                           "WHERE id=%d" % \
                            (jobtracker.nowstr(), job_submits[0]['job_id']))
        jobtracker.query(queries)
        if isrunning:
            print "Stopping job: %s" % job_submits[0]['queue_id']
            try:
                config.jobpooler.queue_manager.delete(job_submits[0]['queue_id'])
            except pipeline_utils.PipelineError, e:
                print "PipelineError: %s" % str(e)


if __name__ == "__main__":
    parser = pipeline_utils.PipelineOptions(usage="%prog [OPTIONS] QUEUE_ID [QUEUE_ID ...]", \
                                   description="Stop a job running in the queue. " \
                                        "There are two ways to stop jobs: " \
                                        "1) Failing the job (i.e. the submission " \
                                        "counts towards the job's number of retries, " \
                                        "and 2) Removing the job (the submission " \
                                        "doesn't count towards retries). Both " \
                                        "possibilities are done safely, with respect " \
                                        "to the job-tracker DB. The default is to " \
                                        "remove the jobs (not fail).")
    parser.add_option('-q', '--queue-id', dest='queue_ids', action='append', \
                        help="A queue_id of a job to stop. Many -q/--queue-id " \
                             "options can be provided.", \
                        default=[])
    parser.add_option('-s', '--submit-id', dest='submit_ids', action='append', \
                        help="A jobsubmit_id of a job to stop. Many -s/--submit-id " \
                             "options can be provided.", \
                        default=[])
    parser.add_option('-f', '--fail', dest='fail', action='store_true', \
                        help="Remove jobs from the queue and mark them " \
                             "as 'failed' in the job-tracker database. " \
                             "(Default: Remove jobs and don't mark them " \
                             "as 'failed').", \
                        default=False)
    parser.add_option('--also-processed', dest='also_processed', action='store_true', \
                        help="Permit job submits with status 'processed' to be " \
                             "stopped/failed. (Default: don't stop/fail 'processed' " \
                             "job submits).", \
                        default=False)
    options, args = parser.parse_args()
    main()
