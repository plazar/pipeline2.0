from JobUploader import JobUploader

upl = JobUploader()
for job_row in upl.get_processed_jobs():
    print "Job id:%s \nJob Status: %s \nJob Files: %s \nOutput Dir.: %s\n" % (job_row['id'],job_row['status'],", ".join(upl.get_jobs_files(job_row)), job_row['output_dir'] )

