import os
import stat
import sys



class SanityCheck:
    
    def __init__(self):
        self.errors=list()
    
    def check_read_write(self,directory_dict):
        for key,rw_dir in directory_dict.items():
            if os.path.exists(rw_dir):
                if not (os.access(rw_dir,os.R_OK) and os.access(rw_dir,os.W_OK)):
                    self.errors.append(SanityError(key,SanityError.RW_ERROR,var_value=rw_dir))
            else:
                self.errors.append(SanityError(key,SanityError.DIR_DNE,var_value=rw_dir))
    
    def check_read(self,directory_dict):
        for key,ptcfe in directory_dict.items():
            if os.path.exists(ptcfe):
                if not (os.access(ptcfe,os.R_OK)):
                    self.errors.append(SanityError(key,SanityError.R_ERROR,var_value=ptcfe))              
            else:
                self.errors.append(SanityError(key,SanityError.DIR_DNE,var_value=ptcfe))
                
    def run(self,module_name):
        
        if module_name=='master_config':
            print "Verifying %s.py" % module_name
            
            import master_config 
            
            read_write_directories = {
            'master_config.base_results_directory':master_config.base_results_directory ,
            'master_config.bgs_db_file_path':master_config.bgs_db_file_path
            }
            self.check_read_write(read_write_directories)
            
            consts_dict = {
            'master_config.institution':[master_config.institution,str],
            'master_config.pipeline':[master_config.pipeline,str],
            'master_config.survey':[master_config.survey,str],
            'master_config.base_results_directory':[master_config.base_results_directory,str],
            'master_config.base_working_directory':[master_config.base_working_directory,str],
            'master_config.default_zaplist':[master_config.default_zaplist,str],
            'master_config.zaplistdir':[master_config.zaplistdir,str],
            'master_config.bgs_screen_output':[master_config.bgs_screen_output,bool],
            'master_config.bgs_db_file_path':[master_config.bgs_db_file_path,str],
            'master_config.email_on_failures':[master_config.email_on_failures,bool],
            'master_config.email_on_terminal_failures':[master_config.email_on_terminal_failures,bool]
            }
            self.check_constants(consts_dict)
            self.check_git()
            
        elif module_name=='uploader_config':
            print "Verifying %s.py" % module_name
            import uploader_config
            path_to_check_for_existance_and_read_access ={
                'uploader_config.uploader_result_dir':uploader_config.uploader_result_dir
            }
            self.check_read(path_to_check_for_existance_and_read_access)

            constants_dict={
            'uploader_config.uploader_result_dir_overide':[uploader_config.uploader_result_dir_overide,bool],
            'uploader_config.uploader_result_dir':[uploader_config.uploader_result_dir,str],
            'uploader_config.uploader_version_num':[uploader_config.uploader_version_num,str]
            }
            self.check_constants(constants_dict)
        elif module_name=='downloader_config':
            print "Verifying %s.py" % module_name
            import downloader_config
            
            read_write_directories = {
            'downloader_config.downloader_temp':downloader_config.downloader_temp
            }
            self.check_read_write(read_write_directories)
            
            constants_dict={
            'downloader_config.downloader_api_service_url':[downloader_config.downloader_api_service_url,str],
            'downloader_config.downloader_api_username':[downloader_config.downloader_api_username,str],
            'downloader_config.downloader_api_password':[downloader_config.downloader_api_password,str],
            'downloader_config.downloader_space_to_use':[downloader_config.downloader_space_to_use,int],
            'downloader_config.downloader_numofdownloads':[downloader_config.downloader_numofdownloads,int],
            'downloader_config.downloader_numofrestores':[downloader_config.downloader_numofrestores,int],
            'downloader_config.downloader_numofretries':[downloader_config.downloader_numofretries,int]
            }
            self.check_constants(constants_dict)
        elif module_name=='processor_config':
            print "Verifying %s.py" % module_name
            import processor_config
            
            read_write_directories = {
            'processor_config.rawdata_directory':processor_config.rawdata_directory
            }
            self.check_read_write(read_write_directories)
            
            constants_dict={
            'processor_config.bgs_sleep':[processor_config.bgs_sleep,int],
            'processor_config.max_jobs_running':[processor_config.max_jobs_running,int],
            'processor_config.max_attempts':[processor_config.max_attempts,int],
            }
            self.check_constants(constants_dict)
            self.check_queuemanager()
        
        for error in self.errors:
                print error    
                
    def check_git(self):
        import commands
        status,string = commands.getstatusoutput('git --version')
        if not string.startswith('git version'):
            self.errors.append(SanityError('git', SanityError.GIT))
            
    def git_exists(self):
        import commands
        status,string = commands.getstatusoutput('git --version')
        return string.startswith('git version')
            
    def check_queuemanager(self):
            
            from processor_config import QueueManagerClass
            from PipelineQueueManager import PipelineQueueManager
            try:
                for name,func in PipelineQueueManager.__dict__.items():
                    if name[:2] != "__":
                        exec_string = "QueueManagerClass.%s(imp_test=True)" % name
                        eval(exec_string)
            except NotImplementedError:
                self.errors.append(SanityError(var_name=name,errtype=SanityError.Q_MANAGER))
                
    def check_constants(self,constants_dict):
        for attr_name,var_n_type in constants_dict.items():
            
            try:
                if not isinstance(var_n_type[0],var_n_type[1]):
                    self.errors.append(SanityError(attr_name,SanityError.TYPE_ERROR,should_be_type=var_n_type[1],var_value=var_n_type[0]))
            except AttributeError,e:
                self.errors.append(SanityError(var_name=attr_name,errtype=SanityError.NOT_DEFINED))


class SanityError():
    NOT_DEFINED=1
    TYPE_ERROR=2
    RW_ERROR=3
    R_ERROR=4
    DIR_DNE=5
    Q_MANAGER=6
    GIT=7
    
    def __init__(self, var_name, errtype, should_be_type=None, var_value=None):
        self.var_name=var_name
        self.var_value=var_value
        self.should_be_type=should_be_type
        self.errtype=errtype
        if self.errtype==self.NOT_DEFINED:
            self.message = "%sERROR%s\tYou have to define following constant: %s\n" % ("\033[1m","\033[0;0m",self.var_name)
        elif self.errtype==self.TYPE_ERROR:
            self.message = "%sERROR%s\t %s should be:  given." % ("\033[1m","\033[0;0m",self.var_name)
            self.message = "%sERROR%s\t %s should be: %s, given." % ("\033[1m","\033[0;0m",self.var_name,self.should_be_type)
            self.message = "%sERROR%s\t %s should be: %s, %s given." % ("\033[1m","\033[0;0m",self.var_name,self.should_be_type,type(self.var_value))
        elif self.errtype==self.RW_ERROR:
            self.message = "%sERROR%s\tIt is possible that you don't have the right permissions for %s[%s]. Should be able to read and write." % ("\033[1m","\033[0;0m",self.var_name,self.var_value)
        elif self.errtype==self.R_ERROR:
            self.message = "%sERROR%s\tIt is possible that you don't have the right permissions for %s[%s]. Should be able to read." % ("\033[1m","\033[0;0m",self.var_name,var_value)
        elif self.errtype==self.DIR_DNE:
            self.message = "%sERROR%s\t%s[%s] does not exist." % ("\033[1m","\033[0;0m",self.var_name,self.var_value)
        elif self.errtype==self.Q_MANAGER:
            self.message = "%sERROR%s\tYou must implement '%s' class method in your Queue Manager class." % ("\033[1m","\033[0;0m",self.var_name)
        elif self.errtype==self.GIT:
            self.message = "%sERROR%s\tYou must have %sgit%s installed on your system." % ("\033[1m","\033[0;0m","\033[1m","\033[0;0m")
        
    def __str__(self):
        return "%s"% self.message