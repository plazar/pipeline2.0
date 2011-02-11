bgs_sleep = 60 #sleep time for background script in seconds
rawdata_directory = "/data/alfa/test_pipeline/"
max_jobs_running = 50
max_attempts = 2 # Maximum number of times a job is attempted due to errors
from QsubManager import Qsub
from PipelineQueueManager import PipelineQueueManager
QueueManagerClass = Qsub

import sanity_check
sanity = sanity_check.SanityCheck()
sanity.run(__name__)