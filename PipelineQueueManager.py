class PipelineQueueManager:
    
    @staticmethod
    def submit(files_str_array=None, output_dir_str=None, imp_test=False):
        """Must return a unique identifier for the job"""
        raise NotImplementedError
    
    @staticmethod
    def is_running(jobid_str=None, imp_test=False):
        """Must return True/False wheather the job is in the Queue or not
            respectively
        """
        raise NotImplementedError

    @staticmethod
    def is_processing_file(filename_str=None, imp_test=False):
        """Must return True/False wheather the job is in the Queue or not
            respectively
        """
        raise NotImplementedError
    
    @staticmethod
    def delete(jobid_str=None, imp_test=False):
        """Must garantee the removal of the job from the Queue"""
        
        raise NotImplementedError
    
    @staticmethod
    def status(imp_test=False):
        """Must return a tuple of number of jobs running and queued for the pipeline
        Note:
        """
        raise NotImplementedError
    
    @staticmethod
    def error(jobid_str=None, imp_test=False):
        raise NotImplementedError
    
    @staticmethod
    def getLogs(jobid_str=None,imp_test=False):
        """Must return a tuple, both must be strings from logs error and std out
        of the queue manager for that jobid_str
        """
        raise NotImplementedError