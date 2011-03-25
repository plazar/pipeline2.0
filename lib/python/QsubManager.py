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

    def submit(self, datafiles, outdir):
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
        searchscript = os.path.join(config.basic.pipelinedir, 'bin', 'search.py')
        cmd = 'qsub -V -v DATAFILES="%s",OUTDIR="%s" -l %s -N %s -e %s -o /dev/null %s' % \
                        (';'.join(datafiles), outdir, self.resource_list, \
                            self.job_basename, self.qsublogdir, searchscript)
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, \
                                stdin=subprocess.PIPE)
        queue_id = pipe.communicate()[0].strip()
        pipe.stdin.close()
        if not queue_id:
            errormsg  = "No job identifier returned by qsub!\n"
            errormsg += "\tCommand executed: %s\n" % cmd
            raise pipeline_utils.PipelineError(errormsg)
        else:
            # There is occasionally a short delay between submission and 
            # the job appearing on the queue, so sleep for 1 second. 
            time.sleep(1)
        return queue_id

    def is_running(self, queue_id):
        """Must return True/False whether the job is in the queue or not
            respectively.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            in_queue: Boolean value. True if the job identified by 'queue_id'
                        is still running.
        """
        batch = PBSQuery.PBSQuery().getjobs()
        return (queue_id in batch)

    def delete(self, queue_id):
        """Remove the job identified by 'queue_id' from the queue.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            None
            
            *** NOTE: A pipeline_utils.PipelineError is raised if
                        the job removal fails.
        """
        cmd = "qsig -s SIGINT %s" % queue_id
        pipe = subprocess.Popen(cmd, shell=True)
        
        # Wait a few seconds a see if the job is still being tracked by
        # the queue manager, or if it marked as exiting.
        time.sleep(5)
        batch = PBSQuery.PBSQuery().getjobs()
        if (queue_id in batch) and ('E' not in batch[queue_id]['job_state']):
            errormsg  = "The job (%s) is still in the queue " % queue_id
            errormsg += "and is not marked as exiting (status = 'E')!\n"
            raise pipeline_utils.PipelineError(errormsg)

    def status(self):
        """Return a tuple of number of jobs running and queued for the pipeline

        Inputs:
            None

        Outputs:
            running: The number of pipeline jobs currently marked as running 
                        by the queue manager.
            queued: The number of pipeline jobs currently marked as queued 
                        by the queue manager.
        """
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
        """A private method not required by the PipelineQueueManager interface.
            Return the path to the error log of the given job, 
            defined by its queue ID.

            Input:
                queue_id: Unique identifier for a job.

            Output:
                stderr_path: Path to the error log file provided by queue 
                        manger for this job.
        
            NOTE: A ValueError is raised if the error log cannot be found.
        """
        jobnum = jobid_str.split(".")[0]
        stderr_path = os.path.join(self.qsublogdir, self.job_basename+".e"+jobnum)
        if not os.path.exists(stderr_path):
            raise ValueError("Cannot find error log for job (%s): %s" % \
                        (jobid_str, stderr_path))
        return stderr_path

    def had_errors(self, queue_id):
        """Given the unique identifier for a job, return if the job 
            terminated with an error or not.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            errors: A boolean value. True if this job terminated with an error.
                    False otherwise.
        """

        try:
            errorlog = self._get_stderr_path(queue_id)
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

