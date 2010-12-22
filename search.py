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
import config


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
    if not os.path.isdir(config.base_working_directory):
	os.makedirs(config.base_working_directory)
    workdir = tempfile.mkdtemp(suffix="_tmp", prefix="PALFA_processing_", \
                        dir=config.base_working_directory)
    resultsdir = tempfile.mkdtemp(suffix="_tmp", prefix="PALFA_results_", \
                        dir=config.base_working_directory)
    # Copy zaplist to working directory
    shutil.copy(config.zaplist, workdir)
    return (workdir, resultsdir)


def main():
    fns = get_datafns()
    outdir = get_outdir()
    workdir, resultsdir = init_workspace()
   
    print "Running on ", socket.gethostname()
    print "Local working directory:", workdir
    print "Local results directory:", resultsdir
    print "When finished results will be copied to:", outdir

    # Update job's log 
    # Copy data file locally?
    for fn in fns:
        os.system("rsync -auvl %s %s" % (fn, workdir))

    fns = [os.path.join(workdir, os.path.split(fn)[-1]) for fn in fns]
    
    presto_search = config.init_presto_search()
    presto_search.main(fns, workdir, resultsdir)

    # Copy search results to results RAID
    os.system("rsync -auvl %s %s" % (resultsdir.rstrip('/'), outdir))

    # Remove working directory and output directory
    shutil.rmtree(workdir)
    shutil.rmtree(resultsdir)


if __name__=='__main__':
    main()
