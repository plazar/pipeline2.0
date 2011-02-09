import config
from PipelineQueueManager import PipelineQueueManager
import PBSQuery
import subprocess
import os
import time

class Qsub(PipelineQueueManager):
    
    @staticmethod
    def submit(files_str_array=None, output_dir_str=None, imp_test=False):
        """Must return a unique identifier for the job"""
        
        if imp_test:
            return True
        
        cmd = 'qsub -V -v DATAFILES="%s",OUTDIR="%s" -l %s -N %s -e %s -o %s search.py' % \
                            (','.join(files_str_array), output_dir_str, config.resource_list, \
                                    config.job_basename, 'qsublog', 'qsublog')
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stdin=subprocess.PIPE)
        jobid = pipe.communicate()[0]
        pipe.stdin.close()
        if not jobid:
            return None
        return jobid.rstrip()
    
    @staticmethod
    def is_running(jobid_str=None, imp_test=False):
        """Must return True/False wheather the job is in the Queue or not
            respectively
        """
        
        if imp_test:
            return True
        
        batch = PBSQuery.PBSQuery().getjobs()
        if jobid_str in batch:
            return True
        else:
            return False
    
    @staticmethod
    def is_processing_file(filename_str=None, imp_test=False):
        """Must return True/False wheather the job processing the input filename
            is running.
        """
        
        if imp_test:
            return True
        
        batch = PBSQuery.PBSQuery().getjobs()
        for j in batch.keys():
            if batch[j]['Job_Name'][0].startswith(config.job_basename):
                if batch[j]['Variable_List']['DATAFILES'][0] == filename_str:
                    return True,j
        return False, None
    
    @staticmethod
    def delete(jobid_str=None, imp_test=False):
        """Must garantee the removal of the job from the Queue"""
        
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
    
    @staticmethod
    def status(imp_test=False):
        """Must return a tuple of number of jobs running and queued for the pipeline
        Note:
        """
        
        if imp_test:
            return True
        
        numrunning = 0
        numqueued = 0
        batch = PBSQuery.PBSQuery().getjobs()
        for j in batch.keys():
            if batch[j]['Job_Name'][0].startswith(config.job_basename):
                if 'R' in batch[j]['job_state']:
                    numrunning += 1
                elif 'Q' in batch[j]['job_state']:
                    numqueued += 1
        return (numrunning, numqueued)
    
    @staticmethod
    def error(jobid_str=None, imp_test=False):
        
        if imp_test:
            return True
        
        if os.path.exists(os.path.join("qsublog",config.job_basename+".e"+jobid_str.split(".")[0])):
            if os.path.getsize(os.path.join("qsublog",config.job_basename+".e"+jobid_str.split(".")[0])) > 0:
                return True
        else:
            return False

    @staticmethod
    def getLogs(jobid_str=None,imp_test=False):
        
        if imp_test:
            return True
        
        stderr_log = ""
        stdout_log = ""
        if os.path.exists(os.path.join("qsublog",config.job_basename+".e"+jobid_str.split(".")[0])):
            err_f = open(os.path.join("qsublog",config.job_basename+".e"+jobid_str.split(".")[0]),'r')
            stderr_log = err_f.read()
            err_f.close()
        
        if os.path.exists(os.path.join("qsublog",config.job_basename+".o"+jobid_str.split(".")[0])):
            out_f = open(os.path.join("qsublog",config.job_basename+".o"+jobid_str.split(".")[0]),'r')
            stdout_log = out_f.read()
            out_f.close()
        
        return (stdout_log, stderr_log)