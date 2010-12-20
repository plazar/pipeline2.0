from threading import Thread
import subprocess
import sys
from job import PulsarSearchJob
import exceptions
from candidate_uploader import CandUploader
import re

class JobUploader(Thread):
    UPLOAD_FAILED = 0
    NEW = 1
    STARTING_HEADERS_UPLOAD = 2
    UPLOADING_HEADERS = 3
    STARTING_CANDIDATES_UPLOAD = 4
    UPLOADING_CANDIDATES = 5
    STARTING_DIAGNOSTICS_UPLOAD = 6
    UPLOADING_DIAGNOSTICS = 7
    UPLOAD_COMPLETED = 8

    STATUS_TRANSLATION = {
    0:"UPLOAD_FAILED",
    1:"NEW",
    2:"STARTING_HEADERS_UPLOAD",
    3:"UPLOADING_HEADERS",
    4:"STARTING_CANDIDATES_UPLOAD",
    5:"UPLOADING_CANDIDATES",
    6:"STARTING_DIAGNOSTICS_UPLOAD",
    7:"UPLOADING_DIAGNOSTICS",
    8:"UPLOAD_COMPLETED"}

    def __init__(self, job, test=False):
        Thread.__init__(self)
        self.output = ''
        self.exitcode = ''
        self.job = job
        self.status = self.NEW
        self.test=test
        
    def run(self):

        #upload header
        self.job.status = PulsarSearchJob.UPLOAD_STARTED
        self.status = self.STARTING_HEADERS_UPLOAD

        if self.test:
            cmd = subprocess.Popen("ping -c 3 google.ca",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        else:
            cmd = subprocess.Popen("python header_uploader.py",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        while True:
            line = cmd.stdout.readline()
            self.exitcode = cmd.poll()
            if (not line) and (self.exitcode is not None):
                if self.exitcode != 0:
                    self.job.status = PulsarSearchJob.UPLOAD_FAILED
                break
            else:
                self.status = self.UPLOADING_HEADERS
            self.output += line


        
        if self.test:
            line = "Success! (Return value: 123)"
        header_id = re.match("^Success! \(Return value: (?P<header_id>\d+)\)",line).groupdict()['header_id']
        del(cmd)

        if self.job.status == PulsarSearchJob.UPLOAD_FAILED:
            sys.exit(self.job.status)
        else:
            self.status = self.STARTING_CANDIDATES_UPLOAD

        try:
            if self.test:
                cand_upler = CandUploader(presto_jobs_output_dir=self.job.presto_output_dir)
            else:
                #cand_upler = CandUploader(presto_jobs_output_dir=self.job.presto_output_dir,db_name='palfa-common')
                pass
            self.status = self.UPLOADING_CANDIDATES
            cand_upler.upload()
        except Exception as e:
            print str(e)
            self.job.status = PulsarSearchJob.UPLOAD_FAILED
            sys.exit(self.job.status)

            
#        cmd = subprocess.Popen("ping -c 3 google.ca",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#
#        while True:
#            line = cmd.stdout.readline()
#            self.exitcode = cmd.poll()
#            if (not line) and (self.exitcode is not None):
#                if self.exitcode != 0:
#                    self.job.status = PulsarSearchJob.UPLOAD_FAILED
#                break
#            else:
#                self.status = self.UPLOADING_CANDIDATES
#            self.output += line
#        del(cmd)


        if self.job.status == PulsarSearchJob.UPLOAD_FAILED:
            sys.exit(self.job.status)
        else:
            self.status = self.STARTING_DIAGNOSTICS_UPLOAD
        cmd = subprocess.Popen("ping -c 3 google.ca",stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

        while True:
            line = cmd.stdout.readline()
            self.exitcode = cmd.poll()
            if (not line) and (self.exitcode is not None):
                if self.exitcode != 0:
                    self.job.status = PulsarSearchJob.UPLOAD_FAILED
                break
            else:
                self.status = self.UPLOADING_DIAGNOSTICS
            self.output += line
        del(cmd)

        if self.job.status == PulsarSearchJob.UPLOAD_FAILED:
            sys.exit(self.job.status)
        else:
            self.status = self.UPLOAD_COMPLETED
            self.job.status = PulsarSearchJob.UPLOAD_COMPLETED

    def get_status(self):
        self.status = parse_for_status(self.output)
        return self.status

    def parse_for_status(self):
        return self.UPLOADING

    def get_status_str(self):
        return self.STATUS_TRANSLATION[self.status]
    