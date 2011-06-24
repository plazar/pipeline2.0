"""
pipeline_utils.py

Defines utilities that will be re-used by multiple modules/scripts
in the pipeline package.
"""
import os
import os.path
import sys
import subprocess
import types
import traceback
import optparse

import debug

class PipelineError(Exception):
    """A generic exception to be thrown by the pipeline.
    """
    def __init__(self, *args, **kwargs):
        super(PipelineError, self).__init__(*args, **kwargs)
        exctype, excval, exctb = sys.exc_info()
        if (exctype is not None) and (excval is not None) and \
                (exctb is not None):
            self.orig_exc_info = exctype, excval, exctb

    def __str__(self):
        msg = super(PipelineError, self).__str__()
        if 'orig_exc_info' in self.__dict__.keys():
            msg += "\n\n========== Original Traceback ==========\n"
            msg += "".join(traceback.format_exception(*self.orig_exc_info))
            msg += "\n(See PipelineError traceback above)\n"
        return msg


def get_fns_for_jobid(jobid):
    """Given a job ID number, return a list of that job's data files.

        Input:
            jobid: The ID number from the job-tracker DB to get files for.
        
        Output:
            fns: A list of data files associated with the job ID.
    """
    import jobtracker

    query = "SELECT filename " \
            "FROM files, job_files " \
            "WHERE job_files.file_id=files.id " \
                "AND job_files.job_id=%d" % jobid
    rows = jobtracker.query(query)
    fns = [str(row['filename']) for row in rows]
    return fns


def clean_up(jobid):
    """Deletes raw files for a given job ID.

        Input:
            jobid: The ID corresponding to a row from the job_submits table.
                The files associated to this job will be removed.
        Outputs:
            None
    """
    fns = get_fns_for_jobid(jobid)
    for fn in fns:
        if os.path.exists(fn):
            os.remove(fn)
            print "Deleted: %s" % fn


def can_add_file(fn, verbose=False):
    """Checks a file to see if it should be added to the 'files'
        table in the jobtracker DB.

        Input:
            fn: The file to check.
            verbose: Print messages to stdout. (Default: be silent).

        Outputs:
            can_add: Boolean value. True if the file should be added. 
                    False otherwise.
    """
    import jobtracker
    import datafile
    try:
        datafile_type = datafile.get_datafile_type([fn])
    except datafile.DataFileError, e:
        if verbose:
            print "Unrecognized data file type: %s" % fn
        return False
    parsedfn = datafile_type.fnmatch(fn)
    if parsedfn.groupdict().setdefault('beam', '-1') == '7':
        if verbose:
            print "Ignoring beam 7 data: %s" % fn
        return False
    # Check if file is already in the job-tracker DB
    files = jobtracker.query("SELECT * FROM files " \
                             "WHERE filename LIKE '%%%s'" % os.path.split(fn)[-1])
    if len(files):
        if verbose:
            print "File is already being tracked: %s" % fn
        return False
    return True


def execute(cmd, stdout=None, stderr=sys.stderr): 
    """Execute the command 'cmd' after logging the command
        to STDOUT.  Return the wall-clock amount of time
        the command took to execute.

        Output standard output to 'stdout' and standard
        error to 'stderr'. Both are strings containing filenames.
        If values are None, the out/err streams are not recorded.
        By default stdout is None and stderr is sent to sys.stderr.
    """
    # Log command to stdout
    sys.stdout.write("\n'"+cmd+"'\n")
    sys.stdout.flush()

    stdoutfile = False
    stderrfile = False
    if type(stdout) == types.StringType:
        stdout = open(stdout, 'w')
        stdoutfile = True
    if type(stderr) == types.StringType:
        stderr = open(stderr, 'w')
        stderrfile = True
    
    # Run (and time) the command. Check for errors.
    retcode = subprocess.call(cmd, shell=True, stdout=stdout, stderr=stderr)
    if retcode < 0:
        raise PipelineError("Execution of command (%s) terminated by signal (%s)!" % \
                                (cmd, -retcode))
    elif retcode > 0:
        raise PipelineError("Execution of command (%s) failed with status (%s)!" % \
                                (cmd, retcode))
    else:
        # Exit code is 0, which is "Success". Do nothing.
        pass
    
    # Close file objects, if any
    if stdoutfile:
        stdout.close()
    if stderrfile:
        stderr.close()


class PipelineOptions(optparse.OptionParser):
    def __init__(self, *args, **kwargs):
        optparse.OptionParser.__init__(self, *args, **kwargs)
       
    def parse_args(self, *args, **kwargs):
        # Add debug group just before parsing so it is the last set of
        # options displayed in help text
        self.add_debug_group()
        return optparse.OptionParser.parse_args(self, *args, **kwargs)

    def add_debug_group(self):
        group = optparse.OptionGroup(self, "Debug Options", \
                    "The following options turn on various debugging " \
                    "features in the pipeline. Multiple debugging " \
                    "options can be provided.")
        group.add_option('-d', '--debug', action='callback', \
                          callback=self.debug_callback, \
                          callback_args=('all',), \
                          help="Turn on all debugging modes. (Same as --debug-all).")
        for m, desc in debug.modes:
            group.add_option('--debug-%s' % m.lower(), action='callback', \
                              callback=self.debug_callback, \
                              callback_args=(m,), \
                              help=desc)
        self.add_option_group(group)

    def debug_callback(self, option, opt_str, value, parser, mode):
        debug.set_mode_on(eval('debug.%s' % mode.upper()))

