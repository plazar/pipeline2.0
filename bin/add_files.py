#!/usr/bin/env python
"""
This script allows addition of the files from a given directory 
for processing by pipeline. Common use scenario would be when the 
files were downloaded manually and not via Downloader module 
included with Pipeline2.0.
"""
import os.path
import sys
import os
import datetime
import optparse
import glob
import types

import jobtracker
import datafile
import pipeline_utils

def create_download(file_path):
    filename = os.path.basename(file_path)
    filesize = os.path.getsize(file_path)
    query = "INSERT INTO files (" \
                "remote_filename, " \
                "filename, " \
                "status, " \
                "created_at, " \
                "updated_at, " \
                "size, " \
                "details) " \
            "VALUES ('%s','%s','%s','%s','%s',%u,'%s')" % \
            (filename, file_path, 'downloaded', jobtracker.nowstr(), \
                jobtracker.nowstr(), filesize, \
                "Manually added via add_files.py")
    return jobtracker.query(query)


def check_file(fn):
    """Check file to make sure it exists, it's readable, 
        it's a non-beam-7 PALFA data file, and it isn't
        already in the job-tracker DB.

        Input:
            fn: The file to check.

        Output:
            valid: A boolean value that is True if the file is
                    valid for insert into the job-tracker DB.
    """
    if os.path.exists(fn) and os.access(fn, os.R_OK):
        return pipeline_utils.can_add_file(fn, verbose=True)
    else:
        print "Not an existing readable file: %s" % fn
        return False


def usage():
    exit( "\nUsage: python add_files.py [directory to pick up file from]\n")

def main():
    files = args # Leftover arguments on command line
    for g in options.fileglobs:
        files += glob.glob(g)
    
    for fn in files:
        if check_file(fn):
            try:
                id = create_download(fn)
                if type(id) == types.IntType:
                    print "File entry created (ID=%d): %s" % (id, fn)
            except Exception,e:
                print "Couldn't create an entry for: %s \n\t%s" % (fn, str(e))


if __name__ == "__main__":
    parser = pipeline_utils.PipelineOptions(usage="%prog [OPTIONS] FILES ...", \
                                   description="Add files to the 'files' " \
                                        "table in the job-tracker database.")
    parser.add_option('-g', '--glob', dest='fileglobs', action='append', \
                        help="A (properly quoted) glob expression indentifying " \
                             "files to add to the job-tracker DB.", \
                        default=[])
    options, args = parser.parse_args()
    main()
