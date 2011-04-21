class PipelineQueueManager(object):
    def __init__(self):
        """Construct a PipelineQueueManager object.
        """
        raise NotImplementedError

    def submit(self, datafiles, outdir, script):
        """Submits a job to the queue to be processed.
            Returns a unique identifier for the job.

            Inputs:
                datafiles: A list of the datafiles being processed.
                outdir: The directory where results will be copied to.
                script: The script to submit to the queue. It should 
                        default to '{config.basic.pipelinedir}/bin/search.py'

            Output:
                jobid: A unique job identifier.
        
            *** NOTE: A pipeline_utils.PipelineError is raised if
                        the queue submission fails.
        """
        raise NotImplementedError

    def can_submit(self):
        """Check if we can submit a job
            (i.e. limits imposed in config file aren't met)

            Inputs:
                None

            Output:
                Boolean value. True if submission is allowed.
        """
        raise NotImplementedError

    def is_running(self, queue_id):
        """Must return True/False whether the job is in the queue or not
            respectively.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            in_queue: Boolean value. True if the job identified by 'queue_id'
                        is still running.
        """
        raise NotImplementedError

    def delete(self, queue_id):
        """Remove the job identified by 'queue_id' from the queue.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            is_removed: A boolean value. Is True if the job was removed from 
                        the queue, False otherwise.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def had_errors(self, queue_id):
        """Given the unique identifier for a job, return if the job 
            terminated with an error or not.

        Input:
            queue_id: Unique identifier for a job.
        
        Output:
            errors: A boolean value. True if this job terminated with an error.
                    False otherwise.
        """
        raise NotImplementedError

    def get_errors(self, queue_id):
        """Return content of error log file for a given queue ID.
        
            Input:
                queue_id: Queue's unique identifier for the job.

            Output:
                errors: The content of the error log for this job (a string).
        """
        raise NotImplementedError

