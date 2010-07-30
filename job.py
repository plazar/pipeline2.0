#!/usr/bin/env python
"""
A few objects for keeping track of pulsar search jobs.

Patrick Lazarus, June 5th, 2010
"""
import os.path
import config
import datetime
import re

import socket

class JobPool:
    def __init__(self):
        self.jobs = [PulsarSearchJob]
        self.datafiles = []
        print "Loading datafile(s)..."
        self.get_datafiles()
        print "Creating Jobs from datafile(s)..."
        self.create_jobs_from_datafiles
        print "Created "+str(len(self.jobs))+" job(s)"

    def create_jobs_from_datafiles( self):
        """Given a list of datafiles, group them into jobs.
            For each job return a PulsarSearchJob object.
        """
        # For PALFA2.0 each observation is contained within a single file.
        
        for datafile in (self.datafiles):
            p_searchjob = PulsarSearchJob([datafile])
            if  isinstance(p_searchjob, PulsarSearchJob):
                self.jobs.append(p_searchjob)

    def delete_job(self):
        return

    def get_datafiles(self):
        """Return a list of data files found in:
                config.rawdata_directory and its subdirectories
            matching the regular expression pattern:
                config.rawdata_re_pattern
       """
        for (dirpath, dirnames, filenames) in os.walk(config.rawdata_directory):
            for fn in filenames:
                if re.match(config.rawdata_re_pattern, fn) is not None:
                    self.datafiles.append(os.path.join(dirpath, fn))
        if len(self.datafiles) > 0:
            return True
        else:
            return False


class PulsarSearchJob:
    """A single pulsar search job object.
    """
    def __init__(self, datafiles):
        """PulsarSearchJob creator.
            'datafiles' is a list of data files required for the job.
        """
        self.datafiles = datafiles
        self.jobname = self.get_jobname()
        self.jobid = None
        self.logfilenm = self.jobname + ".log"
        self.log = JobLog(self.logfilenm, self)

    def get_status(self):
        """Get and return the status of the most recent log entry.
        """
        return self.log.logentries[-1].status

    def count_status(self, status):
        """Count and return the number of times the job has reported
            'status' in its log.
        """
        count = 0
        for entry in self.log.logentries:
            if entry.status.lower() == status.lower():
                count += 1
        return count

    def get_jobname(self):
        """Based on data files determine the job's name and return it.
        """
        datafile0 = self.datafiles[0]
        if datafile0.endswith(".fits"):
            jobname = datafile0[:-5]
        else:
            raise ValueError("First data file is not a FITS file!" \
                             "\n(%s)" % datafile0)
        return jobname

    def submit(self):
        """Submit PulsarSearchJob j to the queue. Update j's log.
        """
        pipe = subprocess.Popen('qsub -V -v DATA_FILE="%s" -l %s -N %s' % \
                            (','.join(self.datafiles), config.resource_list, \
                                    config.job_basename), \
                            shell=True, stdout=subprocess.PIPE)
        jobid = pipe.communicate()[0]
        self.jobid = jobid
        pipe.close()
        self.log.addentry(job.LogEntry(status="Submitted to queue", host=socket.gethostname(), \
                                        info="Job ID: %s" % jobid.strip()))



def get_jobname(datafiles):
    """Based on data files determine the job's name and return it.
    """
    datafile0 = datafiles[0]
    if datafile0.endswith(".fits"):
        jobname = datafile0[:-5]
    else:
        raise ValueError("First data file is not a FITS file!" \
                         "\n(%s)" % datafile0)
    return jobname


class JobLog:
    """A object for reading/writing logfiles for search jobs.
    """
    def __init__(self, logfn, job):
        self.logfn = logfn
        self.job = job # PulsarSearchJob object that this log belongs to
        self.logfmt_re = re.compile("^(?P<date>.*) -- (?P<status>.*) -- " \
                                    "(?P<host>.*) -- (?P<info>.*)$")
        if os.path.exists(self.logfn):
            # Read the logfile
            self.logentries = self.read()
        else:
            # Create the log file
            entry = LogEntry(status="New job", host=socket.gethostname(), \
                             info="Datafiles: %s" % self.job.datafiles)
            self.addentry(entry)
            self.logentries = [entry]
        self.lastupdate = os.path.getmtime(self.logfn)

    def parse_logline(self, logline):
        """Parse a line from a log and return a LogEntry object.
        """
        m = self.logfmt_re.match(logline)
        return LogEntry( ** m.groupdict())

    def read(self):
        """Open the log file, parse it and return a list
            of entries.
            
            Notes: '#' defines a comment.
                   Each entry should have the following format:
                   'datetime' -- 'status' -- 'hostname' -- 'additional info'
        """
        logfile = open(self.logfn)
        lines = [line.partition("#")[0] for line in logfile.readlines()]
        logfile.close()
        lines = [line for line in lines if line.strip()] # remove empty lines

        # Check that all lines have the correct format:
        for line in lines:
            if self.logfmt_re.match(line) is None:
                raise ValueError("Log file line doesn't have correct format" \
                                 "\n(%s)!" % line)
        logentries = [self.parse_logline(line) for line in lines]
        return logentries

    def update(self):
        """Check if log has been modified since it was last read.
            If so, read the log file.
        """
        mtime = os.path.getmtime(self.logfn)
        if self.lastupdate < mtime:
            # Log has been modified recently
            self.logentries = self.read_log()
            self.lastupdate = mtime
        else:
            # Everything is up to date. Do nothing.
            pass

    def addentry(self, entry):
        """Open the log file and add 'entry', a LogEntry object.
        """
        logfile = open(self.logfn, 'a')
        logfile.write(str(entry) + "\n")
        logfile.close()


class LogEntry:
    """An object for describing entries in a JobLog object.
    """
    def __init__(self, status, host, info="", date=datetime.datetime.now().isoformat(' ')):
        self.status = status
        self.host = host
        self.info = info
        self.date = date

    def __str__(self):
        return "%s -- %s -- %s -- %s" % (self.date, self.status, self.host, \
                                         self.info)

"""
Mapping of status to action:

Submitted to queue -> Do nothing
Processing in progress -> Do nothing
Processing successful -> Upload/tidy results, delete file, archive log
Processing failed -> if attempts<thresh: resubmit, if attempts>=thresh: delete file, archive log
"""
