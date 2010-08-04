#!/usr/bin/env python
"""
A batch script to search pulsar data.

Patrick Lazarus, May 20, 2010
"""

import sys
import os
import tempfile
import shutil

print "================="
print sys.path
print "================="
import config


def get_datafns():
    """Get data filenames from command line or environment variable. 
        Environment variable option is only checked if no files
        are provided on command line. 
        
        (NOTE: PBS does not provide batch scripts wtih command line 
                arguments, so we check for if "DATAFILES" environment 
                variable is set.)
    """
    if sys.argv[1:]:
        # Files provided on command line
        fns = sys.argv[1:]
    else:
        # Files provided with environment variable
        fns = os.getenv("DATAFILES", "").split(',')

    # Ensure all files exist
    for fn in fns:
        if not os.path.exists(fn):
            raise ValueError("Data file %s doesn't exist!" % fn)

    # Ensure there are files
    if not fn:
        raise ValueError("No data files provided!")
    return fns


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
    workdir, resultsdir = init_workspace()
    
    # Update job's log 
    # Copy data file locally?

    presto_search = config.init_presto_search()
    presto_search.main(fns, workdir, resultsdir)

    # Copy search results to results RAID

    # Remove working directory and output directory
#    shutil.rmtree(workdir)
#    shutil.rmtree(resultsdir)

if __name__=='__main__':
    main()
