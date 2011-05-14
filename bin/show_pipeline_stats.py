#!/usr/bin/env python
import datetime

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates
import jobtracker
import config.download


def get_data():
    create_times = jobtracker.query("SELECT DATETIME(created_at) FROM jobs")

    upload_times = jobtracker.query("SELECT DATETIME(updated_at) FROM jobs " \
                                        "WHERE status='uploaded'")

    fail_times = jobtracker.query("SELECT DATETIME(updated_at) FROM jobs " \
                                        "WHERE status='terminal_failure'")

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

    bytes_times = bytes_downloaded + bytes_deleted_pass + bytes_deleted_fail
    bytes = np.asarray([row[0] for row in bytes_times])
    times = np.asarray([mkdatetime(row[1]) for row in bytes_times])
    isort = np.argsort(times)
    times = times[isort]
    bytes = np.cumsum(bytes[isort])

    return create_times, upload_times, fail_times, bytes, times


def update_plot(fig):
    create_times, upload_times, fail_times, bytes, times = get_data()
    fig.axes[0].lines[0].set_xdata(create_times)
    fig.axes[0].lines[0].set_ydata(np.arange(1, len(create_times)+1))
    fig.axes[0].lines[1].set_xdata(upload_times)
    fig.axes[0].lines[1].set_ydata(np.arange(1, len(upload_times)+1))
    fig.axes[0].lines[2].set_xdata(fail_times)
    fig.axes[0].lines[2].set_ydata(np.arange(1, len(fail_times)+1))
    fig.axes[1].lines[0].set_xdata(times)
    fig.axes[1].lines[0].set_ydata(bytes/1024.0**3)
    dt = datetime.datetime.now().strftime("%Y-%m-%d %I:%M%p")
    fig.texts[0].set_text("Updated at: %s - Press 'Q' to quit" % dt)
    plt.draw()


def make_plot():
    create_times, upload_times, fail_times, bytes, times = get_data()

    fig = plt.figure()
    jobax = plt.subplot(2,1,1)
    plt.plot(create_times, np.arange(1, len(create_times)+1), 'k-', lw=2, \
                label="Total")
    plt.plot(upload_times, np.arange(1, len(upload_times)+1), 'g-', \
                label="Successful")
    plt.plot(fail_times, np.arange(1, len(fail_times)+1), 'r-', \
                label="Failed")
    jobax.fmt_xdata = matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M')
    plt.legend(loc="best", prop=dict(size='x-small'))
    plt.ylabel("Number of Jobs", size='small')
    plt.yticks(size='x-small')

    diskax = plt.subplot(2,1,2, sharex=jobax)
    plt.plot(times, bytes/1024.0**3, 'k-')
    plt.axhline(config.download.space_to_use/1024.0**3, c='k', ls=':')
    plt.xlabel("Date", size='small')
    plt.ylabel("Pipeline disk usage (raw data, GB)", size='small')
    diskax.fmt_xdata = matplotlib.dates.DateFormatter('%Y-%m-%d %H:%M')
    plt.xticks(size='x-small')
    plt.yticks(size='x-small')

    fig.autofmt_xdate()
    loc = matplotlib.dates.AutoDateLocator(minticks=4)
    fmt = matplotlib.dates.AutoDateFormatter(loc)
    fmt.scaled[1./24.] = '%a, %I:%M%p'
    diskax.xaxis.set_major_locator(loc)
    diskax.xaxis.set_major_formatter(fmt)

    dt = datetime.datetime.now().strftime("%Y-%m-%d %I:%M%p")
    plt.figtext(0.95, 0.02, "Updated at: %s - Press 'Q' to quit" % dt, \
                ha='right', va='center', size='x-small')
    return fig
    

def main():
    fig = make_plot()
    # print "Num jobs (all jobs):", len(create_times)
    # print "Num jobs successfully uploaded:", len(upload_times)
    # print "Num jobs failed:", len(fail_times)

    fig.canvas.mpl_connect("key_press_event", \
                lambda e: (e.key in ('q', 'Q') and plt.close(fig)))

    timer = fig.canvas.new_timer(60*1000) # Time interval in milliseconds
    timer.add_callback(update_plot, fig)
    timer.start()
    plt.show()


if __name__=='__main__':
    main()
