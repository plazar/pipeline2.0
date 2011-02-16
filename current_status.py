from job import *
"""
This script displays number of jobs processed, uploaded, waiting, waiting retry, failed.
"""
j = JobPool()
j.update_jobs_status_from_queue()
j.status(False)
