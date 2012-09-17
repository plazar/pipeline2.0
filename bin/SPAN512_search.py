#!/usr/bin/env python
"""
A batch script to search pulsar data.
"""

import sys
import os
import os.path
import socket
import tempfile
import shutil
import subprocess
import tarfile

import datafile
import astro_utils.calendar

import config.processing
import config.basic


def get_datafns():
    """Get data filenames from command line or environment variable. 
        Environment variable option is only checked if no files
        are provided on command line. 
        
        (NOTE: PBS does not provide batch scripts wtih command line 
                arguments, so we check for if "DATAFILES" environment 
                variable is set.)
    """
    if sys.argv[3:]:
        # First argument is results directory
        # Files provided on command line
        fns = sys.argv[3:]
    else:
        # Files provided with environment variable
        fns = os.getenv("DATAFILES", "").split(';')

    # Ensure all files exist
    # for fn in fns:
    #    if not os.path.exists(fn):
    #        raise ValueError("Data file %s doesn't exist!" % fn)

    # Ensure there are files
    if not fns:
        raise ValueError("No data files provided!")
    return fns


def get_outdir():
    """Get output directory from command line or environment variable.
        Environment variable option is only checked if no files
        are provided on command line. 
        
        (NOTE: PBS does not provide batch scripts wtih command line 
                arguments, so we check for if "OUTDIR" environment 
                variable is set.)
    """
    if sys.argv[1:]:
        # Check command line
        outdir = sys.argv[1]
    else:
        # Use environment variable
        outdir = os.getenv("OUTDIR", "")

    # Ensure output directory is defined
    if not outdir:
        raise ValueError("Output directory is not defined!")
    return outdir
        

def get_options():
    """Get output directory from command line or environment variable.
        Environment variable option is only checked if no files
        are provided on command line. 
        
        (NOTE: PBS does not provide batch scripts wtih command line 
                arguments, so we check for if "OUTDIR" environment 
                variable is set.)
    """
    if sys.argv[2:]:
        # Check command line
        options = sys.argv[2]
    else:
        # Use environment variable
        options = os.getenv("OPTIONS", "")

    # return options, can be none
    return options


def init_workspace():
    """Initialize workspace. 
        - Create working directory.
        - Create results directory.
        - Return 2-tuple (working directory, results directory).
    """
    # Generate temporary working directory
    if not os.path.isdir(config.processing.base_working_directory):
        print "Creating base work directory..."
        os.makedirs(config.processing.base_working_directory)
    workdir = tempfile.mkdtemp(suffix="_tmp", prefix="SPAN512_processing_", \
                        dir=config.processing.base_working_directory)
    resultsdir = tempfile.mkdtemp(suffix="_tmp", prefix="SPAN512_results_", \
                        dir=config.processing.base_working_directory)
    return (workdir, resultsdir)


def system_call(cmd):
    retcode = subprocess.call(cmd, shell=True)
    if retcode < 0:
        raise SystemCallError("System call (%s) terminated by signal (%s)!" % \
                                (cmd, -retcode))
    elif retcode > 0:
        raise SystemCallError("System call (%s) failed with status (%s)!" % \
                                (cmd, retcode))
    else:
        # Exit code is 0, which is "Success". Do nothing.
        pass


class SystemCallError(Exception):
    """An exception to throw when a system call returns 
        with a non-zero exit code.
    """
    pass


def set_up():
    print "Running on ", socket.gethostname()
    fns = get_datafns()
    print "Searching %d files:" % len(fns)
    outdir = get_outdir()
    task = get_options()
    workdir, resultsdir = init_workspace()
   
    print "Local working directory:", workdir
    print "Local results directory:", resultsdir
    print "When finished results will be copied to: %s" % outdir

    # Get the filenames in the working directory
    wfns = [os.path.join(workdir, os.path.split(fn)[-1]) for fn in fns]

    return fns, wfns, workdir, resultsdir, outdir, task


def copy_data(fns, workdir):
    # Copy data to working directory
    for fn in fns:
        print "Copying data %s to %s"%(fn, workdir)
        if config.basic.use_HPSS:
	    system_call("rfcp %s %s"%(fn, workdir))
	else:
	    shutil.copy(fn, workdir)

def copy_zaplist(fns, workdir):
    # Copy zaplist to working directory
    print "Copying default zaplist %s to %s"%(config.processing.default_zaplist, workdir)
    shutil.copy(config.processing.default_zaplist, workdir)


def copy_intermediate_results(outdir, workdir):
    print "Copying contents of main results directory %s to %s"%(outdir, workdir)
    system_call("rsync -av %s/ %s" % (outdir, workdir))

def copy_results(resultsdir, outdir):
    # Copy search results to outdir (only if no errors occurred)
    print "Copying contents of local results directory to", outdir
    system_call("mkdir -p %s" % outdir)
    system_call("rsync -auvl %s/ %s" % (resultsdir, outdir))


def clean_up(workdir, resultsdir):
    print "Cleaning up..."
    if workdir is not None and os.path.isdir(workdir):
        print "Removing working directory:", workdir
        shutil.rmtree(workdir)
    if resultsdir is not None and os.path.isdir(resultsdir):
        print "Removing local results directory:", resultsdir
        shutil.rmtree(resultsdir)
    
def search(fns, workdir, resultsdir, task):
    import SPAN512_presto_search
    SPAN512_presto_search.main(fns, workdir, resultsdir, task)

def main():
    workdir = None
    resultsdir = None
    try:
        fns, wfns, workdir, resultsdir, outdir, task = set_up()
        os.chdir(workdir)

	copy_data(fns,workdir)
        copy_zaplist(config.processing.default_zaplist, workdir)

	# TODO  Copy intermediate results
	# There is currently no check to see if we have all intermediate products required
	tasks2copy = ['search', 'sifting', 'folding']
	if any(tk2 in task for tk2 in tasks2copy):
	    copy_intermediate_results(outdir, workdir)
        search(ppfns, workdir, resultsdir, task)

        copy_results(resultsdir, outdir)
    except:
        # Some error was encountered
        sys.stderr.write("\nProcessing errors! Job ran on %s\n\n" % socket.gethostname())
        # Now, simply re-raise the error so it gets reported in the error logs
        raise
    finally:
        # Remove working directory and output directory
        # even if an error occurred
        clean_up(workdir, resultsdir)


if __name__=='__main__':
    main()
