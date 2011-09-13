#!/usr/bin/env python
"""
Submit 'test_job.py' to the queue and wait for it to terminate.
Then check for errors.
"""
import sys
import time
import os.path

import config.basic
import config.jobpooler

# Submit 'test_job.py' to the queue.
print "Submitting job to queue"
queue_id = config.jobpooler.queue_manager.submit(datafiles=['testfn1', 'testfn2'], \
                                        outdir='/fake/out/dir/', job_id='testjob',\
                                        script=os.path.join(config.basic.pipelinedir, \
                                                            'tests', 'test_job.py'))
count = 1
sys.stdout.write("\rWaiting for job to terminate     ")
sys.stdout.flush()
while config.jobpooler.queue_manager.is_running(queue_id):
    sys.stdout.write("\rWaiting for job to terminate" + \
                            "."*(count%6) + " "*(5-count%6))
    sys.stdout.flush()
    count = (count+1)%6
    time.sleep(1)

print "\nJob is done. Checking for errors."
if config.jobpooler.queue_manager.had_errors(queue_id):
    print "="*60
    print "Error report from 'test_job.py:"
    print config.jobpooler.queue_manager.get_errors(queue_id)
    print "="*60
    sys.exit(1)
else:
    print "No errors from 'test_job.py'..."

