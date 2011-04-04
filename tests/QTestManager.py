import config.download
from PipelineQueueManager import PipelineQueueManager
import subprocess
import os

fake_queue = [
{'job_name':'job0','datafile':config.download.datadir + '/file0'},
{'job_name':'job1','datafile':config.download.datadir + '/file1'},
{'job_name':'job2','datafile':config.download.datadir + '/file2'},
{'job_name':'job3','datafile':config.download.datadir + '/file3'},
{'job_name':'job4','datafile':config.download.datadir + '/file4'}
]

class QTest(PipelineQueueManager):
    
    @staticmethod
    def submit(files_str_array, output_dir_str):
        """Must return a unique identifier for the job"""
        
        jobname = 'job'+str(len(fake_queue) + 1)
        jobfile = 'file'+str(len(fake_queue) + 1)
        fake_queue.append({'jobname':jobname,'datafile':jobfile})
        return jobname
    
    @staticmethod
    def is_processing_file(filename_str):
        """Must return True/False wheather the job processing the input filename
            is running.
        """
        
        for job in fake_queue:
            if job['datafile'] == filename_str:
                return True
        return False
    
    @staticmethod
    def is_running(jobid_str):
        """Must return True/False wheather the job is in the Queue or not
            respectively
        """
        
        for job in fake_queue:
            if job['job_name'] == jobid_str:
                return True
        return False
    
    @staticmethod
    def delete(jobid_str):
        """Must garantee the removal of the job from the Queue"""
        for job in fake_queue[:]:
            if job['job_name'] == jobid_str:
                fake_queue.remove(job)
                return True
        return False
    
    @staticmethod
    def status():
        """Must return a tuple of number of jobs running and queued for the pipeline
        Note:
        """
        
        return (len(fake_queue), 0)
    
    @staticmethod
    def error(jobid_str):
        return False
