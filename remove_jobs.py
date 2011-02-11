from job import *
import sys
import getopt

import jobtracker


def usage():
    print "python %s [<options>] [Queued Id [<Queue Id>...]]\n \t -f | --force-fail \t\t Terminates the job with a failed attempt for processing." % __file__
    
def main(argv):
    print "\n\n"
    force_fail = False
    force_fail_str = "Disabled"
    try:                                
        opts, args = getopt.getopt(argv, "f", ["force-fail"]) 
    except getopt.GetoptError:           
        usage()                          
        sys.exit(2)
    for opt, arg in opts:
        if opt in ['--force-fail','-f']:
            force_fail = True
            force_fail_str = "Enabled"
        
    print "Force Fail: %s" % force_fail_str
    
    for job_id in args:
        if QueueManagerClass.is_running(job_id):
            
            job_submit = jobtracker.query("SELECT * FROM job_submits WHERE queue_id='%s' ORDER BY ID DESC LIMIT 1" % job_id)
            if len(job_submit) == 0:
                print("Job submit with id %s not found." % job_id)
                continue
            print "Stopping job: %s" % job_id
            #QueueManagerClass.delete(job_id)
            job = jobtracker.query("SELECT * FROM jobs WHERE id=%u" % job_submit[0]['job_id'])
            if len(job) == 0:
                print("No Job with job submit %s was found in database table 'jobs'." % job_id)
                continue
            
            #if force_fail:
                #jobtracker.query("UPDATE job_submits SET status='failed' WHERE id=%u" % job_submit['id'])
                #jobtracker.query("UPDATE jobs SET status='failed' WHERE id=%u" % job_submit['job_id'])
            #else:
                #jobtracker.query("DELETE job_submits WHERE id=%u" % job_submit['id'])
                #jobtracker.query("DELETE jobs WHERE id=%u" % job_submit['job_id'])
         
        else:
            print("No Job with queue_id %s was found in Queue Manager." % job_id)




if __name__ == "__main__":
    main(sys.argv[1:])
