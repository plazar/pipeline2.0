import os
import warnings
import traceback
import glob
import sys
import datafile

import header
import candidate_uploader
import diagnostic_uploader
import jobtracker
import database
import pipeline_utils
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
    try:
        db = database.Database('common-copy', autocommit=False)
        # Prepare for upload
        dir = job_submit['output_dir']
        fitsfiles = get_fitsfiles(job_submit)

        # Upload results
        header_id = header.upload_header(fitsfiles, dbname=db)
        print "\tHeader ID: %d" % header_id
        candidate_uploader.upload_candidates(header_id, \
                                             config.upload.version_num(), \
                                             dir, dbname=db)
        print "\tCandidates uploaded."
        data = datafile.autogen_dataobj(fitsfiles)
        diagnostic_uploader.upload_diagnostics(data.obs_name, 
                                             data.beam_id, \
                                             data.obstype, \
                                             config.upload.version_num(), \
                                             dir, dbname=db)
        print "\tDiagnostics uploaded."
    except (header.HeaderError, \
            candidate_uploader.PeriodicityCandidateError, \
            diagnostic_uploader.DiagnosticError):
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
    except database.DatabaseConnectionError, e:
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

