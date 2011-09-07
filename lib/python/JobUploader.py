import os
import warnings
import traceback
import glob
import sys
import time

import debug
import datafile
import header
import candidates
import sp_candidates
import diagnostics
import jobtracker
import database
import upload
import pipeline_utils
import CornellFTP
import config.upload
import config.basic

# Suppress warnings produced by uploaders
# (typically because data, weights, scales, offsets are missing
#       from PSRFITS files)
warnings.filterwarnings("ignore", message="Can't find the .* column")
warnings.filterwarnings("ignore", message=".*NSUBOFFS reports 0 previous rows.*")
warnings.filterwarnings("ignore", message="Channel spacing changes in file 0!")

def run():
    """
    Drives the process of uploading results of the completed jobs.

    """
    query = "SELECT * FROM jobs " \
            "WHERE status='processed'"
    processed_jobs = jobtracker.query(query)
    print "Found %d processed jobs waiting for upload" % len(processed_jobs)
    for ii, job in enumerate(processed_jobs):
        # Get the job's most recent submit
        submit = jobtracker.query("SELECT * FROM job_submits " \
                                  "WHERE job_id=%d " \
                                    "AND status='processed' " \
                                  "ORDER BY id DESC" % job['id'], fetchone=True)
        print "Upload %d of %d" % (ii+1, len(processed_jobs))
        upload_results(submit)


def get_version_number(dir):
    """Given a directory containing results check to see if there is a file 
        containing the version number. If there is read the version number
        and return it. Otherwise get the current versions, create the file
        and return the version number.

        Input:
            dir: A directory containing results

        Output:
            version_number: The version number for the results contained in 'dir'.
    """
    vernum_fn = os.path.join(dir, "version_number.txt")
    if os.path.exists(vernum_fn):
        f = open(vernum_fn, 'r')
        version_number = f.readline()
        f.close()
    else:
        version_number = config.upload.version_num()
        f = open(vernum_fn, 'w')
        f.write(version_number+'\n')
        f.close()
    return version_number.strip()


def upload_results(job_submit):
    """
    Uploads Results for a given submit.

        Input:
            job_submit: A row from the job_submits table.
                Results from this job submission will be
                uploaded.

        Output:
            None
    """
    print "Attempting to upload results"
    print "\tJob ID: %d, Job submission ID: %d" % \
            (job_submit['job_id'], job_submit['id'])
    if debug.UPLOAD:
        upload.upload_timing_summary = {}
        starttime = time.time()
    try:
        # Connect to the DB
        db = database.Database('default', autocommit=False)
        # Prepare for upload
        dir = job_submit['output_dir']
        if not os.path.exists(dir):
            errormsg = 'ERROR: Results directory, %s, does not exist for job_id=%d' %\
                       (dir, job_submit['job_id'])
            raise upload.UploadNonFatalError(errormsg)

        fitsfiles = get_fitsfiles(job_submit)
        data = datafile.autogen_dataobj(fitsfiles)
        version_number = get_version_number(dir)

        if debug.UPLOAD: 
            parsetime = time.time()
        # Upload results
        hdr = header.get_header(fitsfiles)
        
        print "\tHeader parsed."

        cands = candidates.get_candidates(version_number, dir)
        print "\tPeriodicity candidates parsed."
        sp_cands = sp_candidates.get_spcandidates(version_number, dir)
        print "\tSingle pulse candidates parsed."

        for c in (cands + sp_cands):
            hdr.add_dependent(c)
        diags = diagnostics.get_diagnostics(data.obs_name, 
                                             data.beam_id, \
                                             data.obstype, \
                                             version_number, \
                                             dir)
        print "\tDiagnostics parsed."
        
        if debug.UPLOAD: 
            upload.upload_timing_summary['Parsing'] = \
                upload.upload_timing_summary.setdefault('Parsing', 0) + \
                (time.time()-parsetime)

        # Perform the upload
        header_id = hdr.upload(db)
        for d in diags:
            d.upload(db)
        print "\tEverything uploaded and checked successfully. header_id=%d" % \
                    header_id
    except (upload.UploadNonFatalError):
        # Parsing error caught. Job attempt has failed!
        exceptionmsgs = traceback.format_exception(*sys.exc_info())
        errormsg  = "Error while checking results!\n"
        errormsg += "\tJob ID: %d, Job submit ID: %d\n\n" % \
                        (job_submit['job_id'], job_submit['id'])
        errormsg += "".join(exceptionmsgs)
        
        sys.stderr.write("Error while checking results!\n")
        sys.stderr.write("Database transaction will not be committed.\n")
        sys.stderr.write("\t%s" % exceptionmsgs[-1])

        queries = []
        arglists = []
        queries.append("UPDATE job_submits " \
                       "SET status='upload_failed', " \
                            "details=?, " \
                            "updated_at=? " \
                       "WHERE id=?")
        arglists.append((errormsg, jobtracker.nowstr(), job_submit['id']))
        queries.append("UPDATE jobs " \
                       "SET status='failed', " \
                            "details='Error while uploading results', " \
                            "updated_at=? " \
                       "WHERE id=?")
        arglists.append((jobtracker.nowstr(), job_submit['job_id']))
        jobtracker.execute(queries, arglists)
        
        # Rolling back changes. 
        db.rollback()
    except (database.DatabaseConnectionError, CornellFTP.CornellFTPTimeout,\
               upload.UploadDeadlockError, database.DatabaseDeadlockError), e:
        # Connection error while uploading. We will try again later.
        sys.stderr.write(str(e))
        sys.stderr.write("\tRolling back DB transaction and will re-try later.\n")
        
        # Rolling back changes. 
        db.rollback()
    except:
        # Unexpected error!
        sys.stderr.write("Unexpected error!\n")
        sys.stderr.write("\tRolling back DB transaction and re-raising.\n")
        
        # Rolling back changes. 
        db.rollback()
        raise
    else:
        # No errors encountered. Commit changes to the DB.
        db.commit()

        # Update database statuses
        queries = []
        queries.append("UPDATE job_submits " \
                       "SET status='uploaded', " \
                            "details='Upload successful (header_id=%d)', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % 
                       (header_id, jobtracker.nowstr(), job_submit['id']))
        queries.append("UPDATE jobs " \
                       "SET status='uploaded', " \
                            "details='Upload successful (header_id=%d)', " \
                            "updated_at='%s' " \
                       "WHERE id=%d" % \
                       (header_id, jobtracker.nowstr(), job_submit['job_id']))
        jobtracker.query(queries)

        print "Results successfully uploaded"

        if config.basic.delete_rawdata:
            pipeline_utils.clean_up(job_submit['job_id'])

        if debug.UPLOAD: 
            upload.upload_timing_summary['End-to-end'] = \
                upload.upload_timing_summary.setdefault('End-to-end', 0) + \
                (time.time()-starttime)
            print "Upload timing summary:"
            for k in sorted(upload.upload_timing_summary.keys()):
                print "    %s: %.2f s" % (k, upload.upload_timing_summary[k])
        print "" # Just a blank line

def get_fitsfiles(job_submit):
    """Find the fits files associated with this job.
        There should be a single file in the job's result
        directory.

        Input:
            job_submit: A row from the job_submits table.
                A list of fits files corresponding to the submit
                are returned.
        Output:
            fitsfiles: list of paths to *.fits files in results
                directory.
    """
    return glob.glob(os.path.join(job_submit['output_dir'], "*.fits"))

