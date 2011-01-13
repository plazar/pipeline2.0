class PipelineQueueManager:
    
    @staticmethod
    def submit(files_str_array, output_dir_str):
        """Must return a unique identifier for the job"""
        raise NotImplementedError
    
    @staticmethod
    def is_running(jobid_str):
        """Must return True/False wheather the job is in the Queue or not
            respectively
        """
        raise NotImplementedError

    @staticmethod
    def is_processing_file(filename_str):
        """Must return True/False wheather the job is in the Queue or not
            respectively
        """
        raise NotImplementedError
    
    @staticmethod
    def delete(jobid_str):
        """Must garantee the removal of the job from the Queue"""
        
        raise NotImplementedError
    
    @staticmethod
    def status():
        """Must return a tuple of number of jobs running and queued for the pipeline
        Note:
        """
        raise NotImplementedError
    
    @staticmethod
    def error(jobid_str):
        raise NotImplementedError