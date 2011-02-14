class PipelineQueueManager(object):
    def __init__(self):
        """Construct a PipelineQueueManager object.
        """
        raise NotImplementedError
    
    def submit(self, files_str_array=None, output_dir_str=None, imp_test=False):
        """Must return a unique identifier for the job.
        """
        raise NotImplementedError
    
    def is_running(self, jobid_str=None, imp_test=False):
        """Must return True/False whether the job is in the Queue or not
            respectively.
        """
        raise NotImplementedError

    def is_processing_file(self, filename_str=None, imp_test=False):
        """Must return True/False whether the job is in the Queue or not
            respectively.
        """
        raise NotImplementedError
    
    def delete(self, jobid_str=None, imp_test=False):
        """Must guarantee the removal of the job from the Queue.
        """ 
        raise NotImplementedError
    
    def status(self, imp_test=False):
        """Must return a tuple of number of jobs running and queued for the pipeline
        Note:
        """
        raise NotImplementedError
    
    def get_stderr_path(self, jobid_str):
        raise NotImplementedError

    def get_stdout_path(self, jobid_str):
        raise NotImplementedError
    
    def had_errors(self, jobid_str):
        raise NotImplementedError
    
    def read_stderr_log(self, jobid_str):
        raise NotImplementedError
    
    def read_stdout_log(self, jobid_str):
        raise NotImplementedError
    
