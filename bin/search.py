#!/usr/bin/env python
"""
A batch script to search pulsar data.

Patrick Lazarus, May 20, 2010
"""

import sys
import os
import socket
import tempfile
import shutil
import subprocess

import datafile

import config.processing


def get_datafns():
    """Get data filenames from command line or environment variable. 
        Environment variable option is only checked if no files
        are provided on command line. 
        
        (NOTE: PBS does not provide batch scripts wtih command line 
                arguments, so we check for if "DATAFILES" environment 
                variable is set.)
    """
    if sys.argv[2:]:
        # First argument is results directory
        # Files provided on command line
        fns = sys.argv[2:]
    else:
        # Files provided with environment variable
        fns = os.getenv("DATAFILES", "").split(',')

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
    workdir = tempfile.mkdtemp(suffix="_tmp", prefix="PALFA_processing_", \
                        dir=config.processing.base_working_directory)
    resultsdir = tempfile.mkdtemp(suffix="_tmp", prefix="PALFA_results_", \
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
    workdir, resultsdir = init_workspace()
   
    print "Local working directory:", workdir
    print "Local results directory:", resultsdir
    print "When finished results will be copied to: %s" % outdir

    # Copy data files locally
    for fn in fns:
        system_call("rsync -auvl %s %s" % (fn, workdir))
    fns = [os.path.join(workdir, os.path.split(fn)[-1]) for fn in fns]

    return fns, workdir, resultsdir, outdir


def search(fns, workdir, resultsdir):
    # Search the data
    print "Go-Go-Gadget pulsar search..."
    import PALFA2_presto_search
    PALFA2_presto_search.main(fns, workdir, resultsdir)
    
    # Remove data, weights, scales and offsets from fits files
    # and stash them in the results directory.
    print "Removing data, weights, scales and offsets."
    for fn in fns:
        system_call("fitsdelcol %s[SUBINT] DATA DAT_WTS DAT_SCL DAT_OFFS" % fn)
        system_call("rsync -auvl %s %s" % (fn, resultsdir))


def copy_zaplist(fns, workdir):
    # Copy zaplist to working directory
    data = datafile.autogen_dataobj(fns)

    # First, try to find custom zaplist for this MJD
    customzapfn = os.path.join(config.processing.zaplistdir, \
                                "autozap_mjd%d.zaplist" % int(data.timestamp_mjd))
    if os.path.exists(customzapfn):
        # Copy custom zaplist to workdir and rename to the expected zaplist fn
        shutil.copy(customzapfn, workdir)
        print "Copied custom zaplist: %s" % customzapfn
    else:
        # Copy default zaplist
        shutil.copy(config.processing.default_zaplist, workdir)
        print "No custom zaplist found. Copied default zaplist: %s" % \
                config.processing.default_zaplist


def copy_results(resultsdir, outdir):
    # Copy search results to outdir (only if no errors occurred)
    print "Copying contents of local results directory to", outdir
    system_call("mkdir -m 750 -p %s" % outdir)
    system_call("rsync -auvl --chmod=Dg+rX,Fg+r %s/ %s" % (resultsdir, outdir))


def clean_up(workdir, resultsdir):
    print "Cleaning up..."
    if workdir is not None and os.path.isdir(workdir):
        print "Removing working directory:", workdir
        shutil.rmtree(workdir)
    if resultsdir is not None and os.path.isdir(resultsdir):
        print "Removing local results directory:", resultsdir
        shutil.rmtree(resultsdir)
    

def main():
    workdir = None
    resultsdir = None
    try:
        fns, workdir, resultsdir, outdir = set_up()
        os.chdir(workdir)
        ppfns = [os.path.split(fn)[-1] for fn in datafile.preprocess(fns)]
        copy_zaplist(ppfns, workdir)
        search(ppfns, workdir, resultsdir)
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
