import sys
import optparse

import jobtracker
import pipeline_utils
import config.jobpooler

"""
This script allows safe/force-fail removal of the job from the Pipeline
"""

def main():
    for queue_id in args:
        job_submits = jobtracker.query("SELECT id, job_id, queue_id " \
                                       "FROM job_submits " \
                                       "WHERE queue_id LIKE '%s'" % queue_id)
        if len(job_submits) != 1:
            sys.stderr.write("Bad number (%d) of job submissions for queue " \
                                "ID provided: %s\n" % (len(job_submits), queue_id))
        elif config.jobpooler.queue_manager.is_running(job_submits[0]['queue_id']):
            print "Stopping job: %s" % job_submits[0]['queue_id']

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
            try:
                config.jobpooler.queue_manager.delete(job_submits[0]['queue_id'])
            except pipeline_utils.PipelineError, e:
                print "PipelineError: %s" % str(e)

        else:
            sys.stderr.write("There is no job currently in the queue with " \
                                "the ID provided: %s\n" % job_submits[0]['queue_id'])


if __name__ == "__main__":
    parser = optparse.OptionParser(usage="%prog [OPTIONS] QUEUE_ID [QUEUE_ID ...]", \
                                   description="Stop a job running in the queue. " \
                                        "There are two ways to stop jobs: " \
                                        "1) Failing the job (i.e. the submission " \
                                        "counts towards the job's number of retries, " \
                                        "and 2) Removing the job (the submission " \
                                        "doesn't count towards retries). Both " \
                                        "possibilities are done safely, with respect " \
                                        "to the job-tracker DB. The default is to " \
                                        "remove the jobs (not fail).")
    parser.add_option('-f', '--fail', dest='fail', action='store_true', \
                        help="Remove jobs from the queue and mark them " \
                             "as 'failed' in the job-tracker database. " \
                             "(Default: Remove jobs and don't mark them " \
                             "as 'failed').", \
                        default=False)
    options, args = parser.parse_args()
    main()
