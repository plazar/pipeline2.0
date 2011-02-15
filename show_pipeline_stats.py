#!/usr/bin/env python

import numpy as np
import matplotlib.pyplot as plt

import jobtracker
import config.download

create_times = jobtracker.query("SELECT JULIANDAY(created_at) FROM jobs")
upload_times = jobtracker.query("SELECT JULIANDAY(job_uploads.updated_at) " \
                                    "FROM jobs, job_uploads " \
                                    "WHERE jobs.id = job_uploads.job_id AND " \
                                        "job_uploads.status='uploaded'")
fail_times = jobtracker.query("SELECT JULIANDAY(updated_at) FROM jobs " \
                                    "WHERE status='terminal_failure'")

bytes_downloaded = jobtracker.query("SELECT size, JULIANDAY(updated_at) " \
                                    "FROM downloads " \
                                    "WHERE status='downloaded'")
bytes_deleted_pass = jobtracker.query("SELECT -SUM(downloads.size), MAX(JULIANDAY(job_uploads.updated_at)) " \
                                    "FROM job_uploads, downloads, job_files " \
                                    "WHERE downloads.id=job_files.file_id AND job_uploads.job_id=job_files.job_id AND job_uploads.status='uploaded'" \
                                    "GROUP BY job_files.job_id")
bytes_deleted_fail = jobtracker.query("SELECT -SUM(downloads.size), MAX(JULIANDAY(jobs.updated_at)) " \
                                    "FROM jobs, downloads, job_files " \
                                    "WHERE downloads.id=job_files.file_id AND jobs.id=job_files.job_id AND jobs.status='terminal_failure'" \
                                    "GROUP BY job_files.job_id")


create_times = np.asarray([row[0] for row in create_times])
upload_times = np.asarray([row[0] for row in upload_times])
fail_times = np.asarray([row[0] for row in fail_times])

bytes_times = bytes_downloaded + bytes_deleted_pass + bytes_deleted_fail
bytes = np.asarray([row[0] for row in bytes_times])
times = np.asarray([row[1] for row in bytes_times])
isort = np.argsort(times)
times = times[isort]
bytes = np.cumsum(bytes[isort])

print "Num jobs (all jobs):", len(create_times)
print "Num jobs successfully uploaded:", len(upload_times)
print "Num jobs failed:", len(fail_times)

day0 = np.concatenate((create_times, times)).min()

plt.figure()
jobax = plt.subplot(2,1,1)
plt.plot(create_times-day0, np.arange(1, len(create_times)+1), 'k-', lw=2)
plt.plot(upload_times-day0, np.arange(1, len(upload_times)+1), 'g-')
plt.plot(fail_times-day0, np.arange(1, len(fail_times)+1), 'r-')

plt.subplot(2,1,2, sharex=jobax)
plt.plot(times-day0, bytes/1024.0**3, 'k-')
plt.axhline(config.download.space_to_use/1024.0**3, c='k', ls=':')
plt.xlabel("Days since pipeline started")
plt.ylabel("Pipeline disk usage (raw data, GB)")
plt.show()
