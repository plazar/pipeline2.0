from job import *
import sys

def main(argv):
    for job_id in argv:
        if QueueManagerClass.is_running(job_id):
            print "Stopping job: %s" % job_id
        else:
            print "Job with id %s not found." % job_id



if __name__ == "__main__":
    main(sys.argv[1:])