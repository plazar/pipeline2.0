import os.path
import queue_managers.pbs
import config.basic
################################################################
# JobPooler Configurations
################################################################
max_jobs_running = 50 # maximum number of running jobs
max_jobs_queued = 1  # Can be kept small so that you don't hog the queue (>=1)
max_attempts = 2 # Maximum number of times a job is attempted due to errors
# Arguments to pbs.PBSManager are (node name, node property, max # cores to use)
queue_manager = queue_managers.pbs.PBSManager(
                    "%s_batchjob" % config.basic.survey, # Job name in PBS
                    "node-property", # Use nodes with this name/property
                    8) # Maximum number of jobs running on a node

import jobpooler_check
jobpooler_check.jobpooler.populate_configs(locals())
jobpooler_check.jobpooler.check_sanity()
