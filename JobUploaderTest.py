from JobUploader import *
import time
import exceptions


class testJob(object):
    def __init__(self):
        self.status = 'NEW'
        self.presto_output_dir = "/home/snip3/Downloads"
        
myJob = testJob()
uploader = JobUploader(job=myJob,test=True)
uploader.start()


while uploader.is_alive():
    print "Job Status: "+ str(myJob.status)
    print "JobUploader Status: "+ uploader.get_status_str()
    time.sleep(1)
print uploader.output
print uploader.exitcode

print "Job Status: "+ str(myJob.status)
print "JobUploader Status: "+ uploader.get_status_str()
