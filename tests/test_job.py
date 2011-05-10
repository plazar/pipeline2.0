#!/usr/bin/env python
"""
A test job to be submitted to the queue.

Patrick Lazarus, May 6, 2011
"""
import os
import os.path
import sys
import subprocess
import socket

import config.processing

def system_call(cmd):
    pipe = subprocess.Popen(cmd, shell=True, \
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = pipe.communicate()
    retcode = pipe.returncode
    if retcode < 0:
        raise SystemCallError("System call (%s) terminated by signal (%s)!" % \
                                (cmd, -retcode))
    elif retcode > 0:
        raise SystemCallError("System call (%s) failed with status (%s)!" % \
                                (cmd, retcode))
    else:
        # Exit code is 0, which is "Success". Do nothing.
        pass
    return out, err


class SystemCallError(Exception):
    """An exception to throw when a system call returns 
        with a non-zero exit code.
    """
    pass


def check_env():
    """Check if enviroment variables are set.

        Return an error message, which is an empty string
        if no errors were encountered.
    """
    to_test = ["DATAFILES", "OUTDIR"]
    errors = ""
    for var in to_test:
        print "Testing %s" % var
        if os.getenv(var) is None:
            errors += "%s environment variable is not set in test job!\n" % var
    return errors


def check_software():
    """Check if software needed by pipeline is installed.

        Return an error message, which is an empty string
        if no errors were encountered.
    """
    to_test = ["rfifind", "prepsubband", "single_pulse_search.py", \
                "realfft", "zapbirds", "rednoise", "accelsearch", \
                "prepfold", "convert", "gzip", "tempo"]
    errors = ""
    for prog in to_test:
        print "Testing %s" % prog
        try:
            out, err = system_call("which %s" % prog)
        except SystemCallError:
            errors += "The program '%s' cannot be found.\n" % prog
    return errors


def check_dirs():
    to_test = [config.processing.zaplistdir, \
                config.processing.base_working_directory, \
                config.processing.base_tmp_dir, \
                config.processing.base_results_directory]
    errors = ""
    for dir in to_test:
        print "Testing %s" % dir
        if not (os.path.isdir(dir) and os.access(dir, os.R_OK | os.W_OK)):
            errors += "The directory '%s' either doesn't exist, " \
                        "or doesn't have read/write permission!\n" % dir
    return errors


def check_modules():
    to_test = ["numpy", "pyfits", "psr_utils", "presto", "sifting"]
    errors = ""
    for m in to_test:
        print "Testing %s" % m
        try:
            __import__(m)
        except Exception, e:
            errors += "Error importing %s - %s: %s\n" % (m, type(e), str(e))
    return errors


def main():
    error_msg = ""
    error_msg += check_env()
    error_msg += check_software()
    error_msg += check_dirs()
    error_msg += check_modules()

    if error_msg:
        sys.stderr.write("Ran on %s\n\n" % socket.gethostname())
        sys.stderr.write("Errors:\n")
        sys.stderr.write(error_msg)


if __name__=='__main__':
    main()
