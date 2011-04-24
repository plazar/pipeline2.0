"""
This script is used to set a job's status to 'terminal_failure'.
"""
import sys
import optparse

import jobtracker
import pipeline_utils

def main():
    jobids = set([int(id) for id in args])
    jobids.update(options.jobids)

    for fn in options.files:
        rows = jobtracker.query("SELECT job_files.job_id FROM job_files " \
                               "LEFT JOIN files " \
                                    "ON files.id = job_files.file_id " \
                               "WHERE files.filename LIKE '%%%s' " % fn)
        for row in rows:
            jobids.add(row['job_id'])
    print "Number of jobs to kill: %d" % len(jobids)
    for jobid in jobids:
        print "Attempting to kill job with id %d" % jobid
        row = jobtracker.query("SELECT status FROM jobs " \
                                "WHERE id=%d" % jobid, \
                                fetchone=True)
        if row['status'] in ['new', 'retrying']:
            jobtracker.query("UPDATE jobs " \
                             "SET status='terminal_failure', " \
                                  "updated_at='%s', " \
                                  "details='Job was killed manually' " \
                             "WHERE id=%d" % \
                             (jobtracker.nowstr(), jobid))
            print "Job's status has been set to 'terminal_failure'"
            pipeline_utils.clean_up(jobid)
        else:
            print "Only jobs whose status is 'waiting' or 'retrying' " \
                  "can be killed. (Current status of job %d: %s)" % \
                  (jobid, row['status'])


if __name__ == '__main__':
    parser = optparse.OptionParser(usage="%prog ID [ID ...]", \
                                   description="Kill a job. That is set its " \
                                        "status as 'terminal_failure', and " \
                                        "clean up its datafiles (if applicable). ")
    parser.add_option('-f', '--file', dest='files', action='append', \
                        help="File belonging to a job that should be killed.", 
                        default=[])
    parser.add_option('-i', '--id', dest='jobids', action='append', type='int', \
                        help="ID number of a job that should be killed.", \
                        default=[])
    options, args = parser.parse_args()
    main()
