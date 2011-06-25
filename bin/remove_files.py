#!/usr/bin/env python

"""
This script is used to remove datafiles that are 
not part of data analysis jobs.
"""
import os
import os.path

import pipeline_utils
import jobtracker

def main():
    fns = set(args)
    fns.update(options.files)

    for fn in fns:
        fn = os.path.abspath(fn)
        rows = jobtracker.query("SELECT * FROM files " \
                                "WHERE filename='%s' " \
                                    "AND status IN ('added', 'downloaded')" % fn)
        if not len(rows):
            print "Cannot remove %s. Either file isn't tracked, " \
                    "or it doesn't have status 'added' or 'downloaded'." % fn
            continue
                    
        rows = jobtracker.query("SELECT * " \
                                "FROM job_files, files " \
                                "WHERE job_files.file_id=files.id " \
                                   "AND files.filename='%s'" % fn)
        if len(rows):
            print "Cannot remove %s. It is part of a job." % fn
            continue
        pipeline_utils.remove_file(fn)


if __name__ == '__main__':
    parser = pipeline_utils.PipelineOptions(usage="%prog FILE [FILE ...]", \
                                   description="Remove files that are not " \
                                        "part of a job.")
    parser.add_option('-f', '--file', dest='files', action='append', \
                        help="File that should be removed.", 
                        default=[])
    options, args = parser.parse_args()
    main()
