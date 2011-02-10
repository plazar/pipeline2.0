import config
import os
import stat
import sys
from PipelineQueueManager import PipelineQueueManager


class SanityCheck:
    
    def __init__(self):
        self.report = ""
    
    

    def run(self):
        read_write_directories = {
        'config.base_results_directory':config.base_results_directory , 
        'config.downloader_temp':config.downloader_temp,
        'config.bgs_db_file_path':config.bgs_db_file_path
        }
        for key,rw_dir in read_write_directories.items():
            if os.path.exists(rw_dir):
                if not (os.access(rw_dir,os.R_OK) and os.access(rw_dir,os.W_OK)):
                    self.report +="\n!!!ERROR!!!\tIt is possible that you don't have the right permissions for %s[%s]. Should be able to read/write." % (key,rw_dir)
            else:
                self.report +="\n!!!ERROR!!!\t%s[%s] does not exist." % (key,rw_dir)


        path_to_check_for_existance_and_read_access = {
        'config.default_zaplist':config.default_zaplist,
        'config.zaplistdir':config.zaplistdir,
        'config.uploader_result_dir':config.uploader_result_dir
        }

        for key,ptcfe in path_to_check_for_existance_and_read_access.items():
            if os.path.exists(ptcfe):
                if not (os.access(ptcfe,os.R_OK)):
                    self.report +="\n!!!ERROR!!!\tIt is possible that you don't have the right permissions for %s[%s]. Should be able to read." % (key,ptcfe)               
            else:
                self.report +="\n!!!ERROR!!!\t%s[%s] does not exist." % (key,ptcfe)

        self.check_queuemanager()
        self.check_constants()
        
        if self.report != "":
            raise Exception(self.report)
        
    def check_queuemanager(self):
            import job
            try:
                for name,func in job.PipelineQueueManager.__dict__.items():
                    if name[:2] != "__":
                        exec_string = "job.QueueManagerClass.%s(imp_test=True)" % name
                        eval(exec_string)

            except NotImplementedError:
                self.report +="\n!!!ERROR!!!\tYou must implement '%s' class method in you QueueManager class." % name
                
    def check_constants(self):
        attributes={
            'institution':str,
            'pipeline':str,
            'survey':str,
            'base_results_directory':str,
            'base_working_directory':str,
            'default_zaplist':str,
            'zaplistdir':str,
            'log_dir':str,
            'log_archive':str,
            'max_jobs_running':int,
            'job_basename':str,
            'sleep_time':int,
            'max_attempts':int,
            'resource_list':str,
            'delete_rawdata':bool,
            'rawdata_directory':str,
            'rawdata_re_pattern':str,
            'downloader_api_service_url':str,
            'downloader_api_username':str,
            'downloader_api_password':str,
            'downloader_temp':str,
            'downloader_space_to_use':int,
            'downloader_numofdownloads':int,
            'downloader_numofrestores':int,
            'downloader_numofretries':int,
            'uploader_result_dir_overide':bool,
            'uploader_result_dir':str,
            'uploader_version_num':str,
            'bgs_sleep':int,
            'bgs_screen_output':bool,
            'bgs_db_file_path':str,
            'email_on_failures':bool,
            'email_on_terminal_failures':bool
        }
        
        for attr,thetype in attributes.items():
            try:
                eval('config.%s'%attr)
                if not isinstance(eval('config.%s'%attr),thetype):
                    self.report += '\n!!!ERROR!!!\tconfig.%s supposed to be a %s'% (attr,str(thetype))
            except AttributeError,e:
                self.report +="\n!!!ERROR!!!\tYou have to define following constant in config.py: %s\n" % attr
        
if __name__ == "__main__":
    sanity = SanityCheck()
    sanity.run()
