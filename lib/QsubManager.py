import PipelineQueueManager
import PBSQuery
import subprocess
import os
import time

class Qsub(PipelineQueueManager.PipelineQueueManager):
    def __init__(self, job_basename, qsublogdir, resource_list):
        self.job_basename = job_basename
        self.qsublogdir = qsublogdir
        self.resource_list = resource_list

    def submit(self, datafiles, outdir, imp_test=False):
        """Submits a job to the queue to be processed.
            Returns a unique identifier for the job.

            Inputs:
                datafiles: A list of the datafiles being processed.
                outdir: The directory where results will be copied to.

            Output:
                jobid: A unique job identifier.
        """
        if imp_test:
            return True

        cmd = 'qsub -V -v DATAFILES="%s",OUTDIR="%s" -l %s -N %s -e %s -o %s search.py' % \
                            (','.join(datafiles), outdir, self.resource_list, \
                                    self.job_basename, self.qsublogdir, self.qsublogdir)
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE)
        jobid = pipe.communicate()[0].strip()
        pipe.stdin.close()
        if not jobid:
            raise ValueError("No job identifier returned by qsub!")
        return jobid

    def is_running(self, jobid_str=None, imp_test=False):
        """Returns True/False wheather the job is in the Queue or not
            respectively

        Input(s):
            jobid_str: Unique String identifier for a job.
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            Boolean: True - if the job is still managed by queue manager
                    False - otherwise
        """
        if imp_test:
            return True

        batch = PBSQuery.PBSQuery().getjobs()
        if jobid_str in batch:
            return True
        else:
            return False

    def delete(self, jobid_str=None, imp_test=False):
        """Must guarantee the removal of the job from the Queue.

        Input(s):
            jobid_str: Unique String identifier for a job.
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            Boolean: True - if the job having given jobid_str was removed by from the queue manager
                    False - if the error occurred and the job was not removed.
        """

        if imp_test:
            return True

        cmd = "qdel %s" % jobid_str
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE)
        response = pipe.communicate()[0]
        pipe.stdin.close()
        time.sleep(3)
        batch = PBSQuery.PBSQuery().getjobs()
        if not (jobid_str in batch) or 'E' in batch[jobid_str]['job_state']:
            return True
        return False

    def status(self, imp_test=False):
        """Must return a tuple of number of jobs running and queued for the pipeline

        Input(s):
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            Tuple: (running, queued); running - number of jobs currently being run by queue manager,
                                    queued - number of jobs queued by queue manager
        """
        if imp_test:
            return True

        numrunning = 0
        numqueued = 0
        batch = PBSQuery.PBSQuery().getjobs()
        for j in batch.keys():
            if batch[j]['Job_Name'][0].startswith(self.job_basename):
                if 'R' in batch[j]['job_state']:
                    numrunning += 1
                elif 'Q' in batch[j]['job_state']:
                    numqueued += 1
        return (numrunning, numqueued)

    def get_stderr_path(self, jobid_str):
        """Must return a string file path to the error log of the given job
        defined by input jobid_str

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Path to the error log file provided by queue manger for this job .
        """
        jobnum = jobid_str.split(".")[0]
        stderr_path = os.path.join(self.qsublogdir, self.job_basename+".e"+jobnum)
        return stderr_path

    def get_stdout_path(self, jobid_str):
        """Must return a string file path to the output log of the given job
        defined by input jobid_str.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Path to the output log file provided by queue manger for this job .
        """
        jobnum = jobid_str.split(".")[0]
        stdout_path = os.path.join(self.qsublogdir, self.job_basename+".o"+jobnum)
        return stdout_path

    def had_errors(self, jobid_str):
        """Must return True/False if the job terminated with an error or without
        accordingly, given the unique string identifier for a job.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            Boolean: True - if this job terminated with an error,
                    False - otherwise.
        """

        errorlog = self.get_stderr_path(jobid_str)
        if os.path.exists(errorlog):
            if os.path.getsize(errorlog) > 0:
                return True
            else:
                return False
        else:
            raise ValueError("Cannot find error log for job (%s): %s" % (jobid_str, errorlog))

    def read_stderr_log(self, jobid_str):
        """Must return the content of error log file for a given job unique string
        identifier.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Content of error log file for this job.
        """

        errorlog = self.get_stderr_path(jobid_str)
        if os.path.exists(errorlog):
            err_f = open(errorlog, 'r')
            stderr_log = err_f.read()
            err_f.close()
        return stderr_log

    def read_stdout_log(self, jobid_str):
        """Must return the content of error log file for a given job unique string
        identifier.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Content of output log file for this job.

        """

        outlog = self.get_stdout_path(jobid_str)
        if os.path.exists(outlog):
            out_f = open(outlog, 'r')
            stdout_log = out_f.read()
            out_f.close()
        return stdout_log
