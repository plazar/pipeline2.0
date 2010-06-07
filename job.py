#!/usr/bin/env python
"""
A background script for running pulsar search jobs.

Patrick Lazarus, June 5th, 2010
"""
import datetime
import re
import socket
import os.path

class PulsarSearchJob:
    """A single pulsar search job object.
    """
    def __init__(self, datafiles):
        """PulsarSearchJob creator.
            'datafiles' is a list of data files required for the job.
        """
        self.datafiles = datafiles
        self.jobname = self.get_jobname(self.datafiles)
        self.logfilenm = self.jobname + ".log"
        self.log = JobLog(self.logfilenm)

    def get_jobname(self, datafiles):
        """Based on data files determine the job's name and return it.
        """
        datafile0 = datafiles[0]
        if datafile0.endswith(".fits")
            jobname = datafile0[:-5]
        else:
            raise ValueError("First data file is not a FITS file!" \
                                "\n(%s)" % datafile0)
        return jobname


class JobLog:
    """A object for reading/writing logfiles for search jobs.
    """
    def __init__(self, logfn):
        self.logfn = logfn
        self.logfmt_re = re.compile("^(?P<date>.*) -- (?P<status>.*) -- " \
                                    "(?P<host>.*) -- (?P<info>.*$)")
        if os.path.exists(self.logfn):
            # Read the logfile
            self.logentries = self.read(self.logfn)
        else:
            # Create the log file
            entry = LogEntry(status="New job", host=socket.gethostname(), \
                                        info="Brand new job log.")
            self.addentry(entry)
            self.logentries = [entry]
        self.lastupdate = os.path.getmtime(self.logfn)

    def parse_logline(self, logline):
        """Parse a line from a log and return a LogEntry object.
        """
        m = self.logfmt_re.match(logline)
        return LogEntry(**m.groupdict())

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
        logentries = [parse_logline(line) for line in lines]
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
        logfile.write(str(entry))
        logfile.close()


class LogEntry:
    """An object for describing entries in a JobLog object.
    """
    def __init__(self, status, host, info, **kwargs):
        self.status = status
        self.host = host
        self.info = info
        if "date" in kwargs:
            self.date = date
        else:
            self.date = datetime.datetime.now().isoformat(' ')

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
