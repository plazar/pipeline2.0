import jobtracker
"""
This scripts displays details about jobs that failed and failed permanently.
"""
failed_jobs = jobtracker.query("SELECT * FROM jobs WHERE status='terminal_failure' AND status='failed'")
output = ''
for job_row in failed_jobs:
    failed_submits = jobtracker.query("SELECT * FROM job_submits WHERE job_id=%u" % \
                                        job_row['id'])
    output += str(job_row['id'])
    for submit in failed_submits:
        output += '\t'+ str(submit['details']) +'\t'+ str(submit['output_dir']) + \
                    '\t'+ str(submit['updated_at'])+'\n'
    output += '\n'

print output
