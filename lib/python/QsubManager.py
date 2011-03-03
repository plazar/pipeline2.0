import subprocess
import os
import time

import PBSQuery

import PipelineQueueManager
import pipeline_utils
import config.basic

class Qsub(PipelineQueueManager.PipelineQueueManager):
    def __init__(self, job_basename, resource_list):
        self.job_basename = job_basename
        self.resource_list = resource_list
        self.qsublogdir = os.path.join(config.basic.log_dir, "qsublog")

    def submit(self, datafiles, outdir, imp_test=False):
        """Submits a job to the queue to be processed.
            Returns a unique identifier for the job.

            Inputs:
                datafiles: A list of the datafiles being processed.
                outdir: The directory where results will be copied to.

            Output:
                jobid: A unique job identifier.
        
            *** NOTE: A pipeline_utils.PipelineError is raised if
                        the queue submission fails.
        """
        if imp_test:
            return True
        searchscript = os.path.join(config.basic.pipelinedir, 'bin', 'search.py')
        cmd = 'qsub -V -v DATAFILES="%s",OUTDIR="%s" -l %s -N %s -e %s -o %s %s' % \
                            (','.join(datafiles), outdir, self.resource_list, \
                                    self.job_basename, self.qsublogdir, \
                                    self.qsublogdir, searchscript)
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE)
        jobid = pipe.communicate()[0].strip()
        pipe.stdin.close()
        if not jobid:
            errormsg  = "No job identifier returned by qsub!\n"
            errormsg += "\tCommand executed: %s\n" % cmd
            raise pipeline_utils.PipelineError(errormsg)
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

    def _get_stderr_path(self, jobid_str):
        """Must return a string file path to the error log of the given job
        defined by input jobid_str

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Path to the error log file provided by queue manger for this job .
        """
        jobnum = jobid_str.split(".")[0]
        stderr_path = os.path.join(self.qsublogdir, self.job_basename+".e"+jobnum)
        if not os.path.exists(stderr_path):
            raise ValueError("Cannot find error log for job (%s): %s" % \
                        (jobid_str, stderr_path))
        return stderr_path

    def had_errors(self, jobid_str):
        """Must return True/False if the job terminated with an error or without
        accordingly, given the unique string identifier for a job.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            Boolean: True - if this job terminated with an error,
                    False - otherwise.
        """

        try:
            errorlog = self._get_stderr_path(jobid_str)
        except ValueError:
            errors = True
        else:
            if os.path.getsize(errorlog) > 0:
                errors = True
            else:
                errors = False
        return errors

    def get_errors(self, queue_id):
        """Return content of error log file for a given queue ID.
        
            Input:
                queue_id: Queue's unique identifier for the job.

            Output:
                errors: The content of the error log for this job (a string).
        """
        try:
            errorlog = self._get_stderr_path(queue_id)
        except ValueError, e:
            errors = str(e)
        else:
            if os.path.exists(errorlog):
                err_f = open(errorlog, 'r')
                errors = err_f.read()
                err_f.close()
        return errors

