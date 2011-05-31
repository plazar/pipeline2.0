#!/usr/bin/env python
import datetime

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates
import jobtracker
import config.download


class PipelineStatsFigure(matplotlib.figure.Figure):
    def __init__(self, *args, **kwargs):
        super(PipelineStatsFigure, self).__init__(*args, **kwargs)
        create_times, upload_times, fail_times, bytes, times, \
            restore_times, restore_sizes = get_data()
        self.jobax = self.add_subplot(3,1,1)
        self.jobax.totline = self.jobax.plot(create_times, \
                    np.arange(1, len(create_times)+1), 'k-', \
                    lw=2, label="Total")[0]
        self.jobax.upline = self.jobax.plot(upload_times, \
                    np.arange(1, len(upload_times)+1), 'g-', \
                    label="Successful")[0]
        self.jobax.failline = self.jobax.plot(fail_times, \
                    np.arange(1, len(fail_times)+1), 'r-', \
                    label="Failed")[0]
        self.jobax.legend(loc="best", prop=dict(size='x-small'))
        self.jobax.set_ylabel("Number of Jobs", size='small')
        plt.setp(self.jobax.get_yticklabels(), size='x-small')

        self.restoreax = self.add_subplot(3,1,2, sharex=self.jobax)
        self.restoreax.line = self.restoreax.plot(restore_times, \
                    restore_sizes, 'bo', label="restores", alpha=0.5)[0]
        self.restoreax.set_ylabel("Restore size", size='small')
        self.restoreax.set_yscale("log")
        self.restoreax.set_ylim(0.5, 300)
        fmt = matplotlib.ticker.LogFormatter()
        self.restoreax.yaxis.set_major_formatter(fmt)

        self.diskax = self.add_subplot(3,1,3, sharex=self.jobax)
        self.diskax.diskline = self.diskax.plot(times, bytes/1024.0**3, 'k-')[0]
        self.diskax.axhline(config.download.space_to_use/1024.0**3, c='k', ls=':')
        self.diskax.set_xlabel("Date", size='small')
        self.diskax.set_ylabel("Pipeline disk usage (raw data, GB)", size='small')
        self.diskax.fmt_xdata = matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M')
        plt.setp(self.diskax.get_xticklabels(), size='x-small')
        plt.setp(self.diskax.get_yticklabels(), size='x-small')

        self.jobax.fmt_xdata = matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M')
        self.autofmt_xdate()
        loc = matplotlib.dates.AutoDateLocator(minticks=4)
        fmt = matplotlib.dates.AutoDateFormatter(loc)
        fmt.scaled[1./24.] = '%a, %I:%M%p'
        self.diskax.xaxis.set_major_locator(loc)
        self.diskax.xaxis.set_major_formatter(fmt)

        dt = datetime.datetime.now().strftime("%Y-%m-%d %I:%M%p")
        self.timetext = self.text(0.95, 0.02, \
                    "Updated at: %s - Press 'Q' to quit" % dt, \
                    ha='right', va='center', size='x-small')

    def close(self):
        print "Exiting..."
        plt.close(self)

    def update(self):
        create_times, upload_times, fail_times, bytes, times, \
            restore_times, restore_sizes = get_data()
        self.jobax.totline.set_xdata(create_times)
        self.jobax.totline.set_ydata(np.arange(1, len(create_times)+1))
        self.jobax.upline.set_xdata(upload_times)
        self.jobax.upline.set_ydata(np.arange(1, len(upload_times)+1))
        self.jobax.failline.set_xdata(fail_times)
        self.jobax.failline.set_ydata(np.arange(1, len(fail_times)+1))
        self.jobax.relim()

        self.restoreax.line.set_xdata(restore_times)
        self.restoreax.line.set_ydata(restore_sizes)
        self.restoreax.relim()

        self.diskax.diskline.set_xdata(times)
        self.diskax.diskline.set_ydata(bytes/1024.0**3)
        self.diskax.relim()
        
        dt = datetime.datetime.now().strftime("%Y-%m-%d %I:%M%p")
        self.timetext.set_text("Updated at: %s - Press 'Q' to quit" % dt)
        plt.draw()
        

def get_data():
    create_times = jobtracker.query("SELECT DATETIME(created_at) FROM jobs")

    upload_times = jobtracker.query("SELECT DATETIME(updated_at) FROM jobs " \
                                        "WHERE status='uploaded'")

    fail_times = jobtracker.query("SELECT DATETIME(updated_at) FROM jobs " \
                                        "WHERE status='terminal_failure'")

    restore_times = jobtracker.query("SELECT DATETIME(created_at) FROM requests")
    restore_sizes = jobtracker.query("SELECT numrequested FROM requests")

    bytes_downloaded = jobtracker.query("SELECT size, DATETIME(updated_at) " \
                                        "FROM files " \
                                        "WHERE status='downloaded'")
    bytes_deleted_pass = jobtracker.query("SELECT -SUM(files.size), " \
                                                "MAX(DATETIME(jobs.updated_at)) " \
                                          "FROM jobs, files, job_files " \
                                          "WHERE files.id=job_files.file_id " \
                                                "AND jobs.id=job_files.job_id " \
                                                "AND jobs.status='uploaded'" \
                                          "GROUP BY jobs.id")
    bytes_deleted_fail = jobtracker.query("SELECT -SUM(files.size), " \
                                                "MAX(DATETIME(jobs.updated_at)) " \
                                          "FROM jobs, files, job_files " \
                                          "WHERE files.id=job_files.file_id " \
                                                "AND jobs.id=job_files.job_id " \
                                                "AND jobs.status='terminal_failure'" \
                                          "GROUP BY jobs.id")

    mkdatetime = lambda dt: datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')

    create_times = np.asarray(sorted([mkdatetime(row[0]) for row in create_times]))
    upload_times = np.asarray(sorted([mkdatetime(row[0]) for row in upload_times]))
    fail_times = np.asarray(sorted([mkdatetime(row[0]) for row in fail_times]))

    restore_times = np.asarray([mkdatetime(row[0]) for row in restore_times])
    restore_sizes = np.asarray([row[0] for row in restore_sizes])

    bytes_times = bytes_downloaded + bytes_deleted_pass + bytes_deleted_fail
    bytes = np.asarray([row[0] for row in bytes_times])
    times = np.asarray([mkdatetime(row[1]) for row in bytes_times])
    isort = np.argsort(times)
    times = times[isort]
    bytes = np.cumsum(bytes[isort])

    return create_times, upload_times, fail_times, bytes, times, \
            restore_times, restore_sizes


def main():
    fig = plt.figure(FigureClass=PipelineStatsFigure)

    # print "Num jobs (all jobs):", len(create_times)
    # print "Num jobs successfully uploaded:", len(upload_times)
    # print "Num jobs failed:", len(fail_times)

    fig.canvas.mpl_connect("key_press_event", \
                lambda e: (e.key in ('q', 'Q') and fig.close()))

    timer = fig.canvas.new_timer(60*1000) # Time interval in milliseconds
    timer.add_callback(fig.update)
    timer.start()
    plt.show()


if __name__=='__main__':
    main()
