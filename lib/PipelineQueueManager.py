class PipelineQueueManager(object):
    def __init__(self):
        """Construct a PipelineQueueManager object.
        """
        raise NotImplementedError

    def submit(self, files_str_array=None, output_dir_str=None, imp_test=False):
        """Must return a unique identifier for the job.

        Input(s):
            files_str_array: String array of file paths
            output_dir_Str: String specifying the output directory for the job
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            String: unique string identifier for this submitted job in queue manager.
                    later used to get information about the job.
        """
        raise NotImplementedError

    def is_running(self, jobid_str=None, imp_test=False):
        """Must return True/False whether the job is in the Queue or not
            respectively.

        Input(s):
            jobid_str: Unique String identifier for a job.
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            Boolean: True - if the job is still managed by queue manager
                    False - otherwise
        """
        raise NotImplementedError

    def is_processing_file(self, filename_str=None, imp_test=False):
        """Must return True/False whether the job processing given filepath
            is in the Queue or not respectively.

        Input(s):
            filename_str: String - full file path .
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            Boolean: True - if the job processing give file is still managed by queue manager
                    False - otherwise
        """
        raise NotImplementedError

    def delete(self, jobid_str=None, imp_test=False):
        """Must guarantee the removal of the job from the Queue.

        Input(s):
            jobid_str: Unique String identifier for a job.
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            Boolean: True - if the job having given jobid_str was removed by from the queue manager
                    False - if the error occurred and the job was not removed
        """
        raise NotImplementedError

    def status(self, imp_test=False):
        """Must return a tuple of number of jobs running and queued for the pipeline

        Input(s):
            imp_test: boolean for testing if the derived class implemented this function
                        (in the derived class if set to True, the function must return
                        from the first line.
        Output(s):
            Tuple: (running, queued); running - number of jobs currently being run by queue manager,
                                    queued - number of jobs queued by queue manager
        """
        raise NotImplementedError

    def get_stderr_path(self, jobid_str):
        """Must return a string file path to the error log of the given job
        defined by input jobid_str

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Path to the error log file provided by queue manger for this job .
        """
        raise NotImplementedError

    def get_stdout_path(self, jobid_str):
        """Must return a string file path to the output log of the given job
        defined by input jobid_str.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Path to the output log file provided by queue manger for this job .
        """
        raise NotImplementedError

    def had_errors(self, jobid_str):
        """Must return True/False if the job terminated with an error or without
        accordingly, given the unique string identifier for a job.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            Boolean: True - if this job terminated with an error,
                    False - otherwise.
        """
        raise NotImplementedError

    def read_stderr_log(self, jobid_str):
        """Must return the content of error log file for a given job unique string
        identifier.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Content of error log file for this job.
        """
        raise NotImplementedError

    def read_stdout_log(self, jobid_str):
        """Must return the content of error log file for a given job unique string
        identifier.

        Input(s):
            jobid_str: Unique String identifier for a job.
        Output(s):
            String: Content of output log file for this job.

        """
        raise NotImplementedError

