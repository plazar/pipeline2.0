class PipelineQueueManager:
    
    def submit(self, files_str_array, output_dir_str):
        """Must return a unique identifier for the job"""
        raise NotImplementedError
    
    def is_running(self, jobid_str):
        """Must return True/False wheather the job is in the Queue or not
            respectively
        """
        raise NotImplementedError
    
    def delete(self, jobid_str):
        """Must garantee the removal of the job from the Queue"""
        
        raise NotImplementedError
    
    def status(self):
        """Must return a tuple of number of jobs running and queued for the pipeline
        Note:
        """
        raise NotImplementedError