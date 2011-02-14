import os.path
import QsubManager
import config.basic
################################################################
# JobPooler Configurations
################################################################
base_results_directory = "/data/data7/PALFA/test_new_pipeline_clean/"
max_jobs_running = 50
max_jobs_queued = 1
max_attempts = 2 # Maximum number of times a job is attempted due to errors
delete_rawdata = True
queue_manager = QsubManager.Qsub("%s_batchjob" % config.basic.survey, \
                        os.path.join(config.basic.pipelinedir, "qsublog"), \
                        "nodes=P248:ppn=1")

import jobpooler_check
jobpooler_check.jobpooler.populate_configs(locals())
jobpooler_check.jobpooler.check_sanity()
