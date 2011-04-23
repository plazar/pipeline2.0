import os.path
import queue_managers.pbs
import config.basic
################################################################
# JobPooler Configurations
################################################################
# This is where all of the output files are stored
# Must be writable and have lots of free space...
base_results_directory = "/data/data7/PALFA/test_new_pipeline_clean/"
max_jobs_running = 50 # maximum number of running jobs
max_jobs_queued = 1  # Can be kept small so that you don't hog the queue (>=1)
max_attempts = 2 # Maximum number of times a job is attempted due to errors
# Arguments to pbs.PBSManager are (node name, node property, max # cores to use)
queue_manager = queue_managers.pbs.PBSManager("%s_batchjob" % config.basic.survey, \
                                    "node-property", 8)

import jobpooler_check
jobpooler_check.jobpooler.populate_configs(locals())
jobpooler_check.jobpooler.check_sanity()
