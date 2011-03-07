import optparse
import jobtracker

"""
Displays details about jobs that are terminally failed.
"""
def main():
    failed_jobs = jobtracker.query("SELECT id, updated_at FROM jobs " \
                                   "WHERE status='terminal_failure'" \
                                   "ORDER BY updated_at ASC")
    for job in failed_jobs:
        last_submit = jobtracker.query("SELECT id, job_id, status, details, updated_at " \
                                       "FROM job_submits " \
                                       "WHERE job_id=%d " \
                                       "ORDER BY updated_at DESC" % job['id'], \
                                       fetchone=True)
        headerline = "========== Job ID: %d, last job submission ID: %d ==========" % \
            (last_submit['job_id'], last_submit['id'])
        print headerline
        print "Last job submission status: %s (%s)" % \
            (last_submit['status'], last_submit['updated_at'])
        if options.full:
            print last_submit['details']
        print "-"*len(headerline)
        print ""

if __name__ == '__main__':
    parser = optparse.OptionParser(usage="%prog [OPTIONS]", \
                                   description="Show an overview of all " \
                                        "terminally failed jobs.")
    parser.add_option('-f', '--full', dest='full', action='store_true', \
                        help="Show full details for each entry displayed. " \
                                "(Default: Omit details).", \
                        default=False)
    options, args = parser.parse_args()
    main()
    
