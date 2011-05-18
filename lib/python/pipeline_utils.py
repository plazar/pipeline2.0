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

DEBUG=False

class PipelineError(Exception):
    """A generic exception to be thrown by the pipeline.
    """
    pass


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
    """
    Deletes raw files for a given job ID.

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
