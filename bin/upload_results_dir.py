#!/usr/bin/env python

import os
import warnings
import traceback
import glob
import sys
import time
import shutil

import datafile
import header
import candidates
import sp_candidates
import diagnostics
import database
import upload
import CornellFTP
import JobUploader
import config.upload
import config.basic

def upload_results(dir):
  
    try:
	db = database.Database('default', autocommit=False)

	if not os.path.exists(dir) or not os.listdir(dir):
	    errormsg = 'ERROR: Results directory, %s, does not exist or is empty' % dir
            raise upload.UploadNonFatalError(errormsg)

	
	fitsfiles = glob.glob(os.path.join(dir, "*.fits"))
	data = datafile.autogen_dataobj(fitsfiles)
	version_number = JobUploader.get_version_number(dir)

	hdr = header.get_header(fitsfiles)
	print "\tHeader parsed."

	cands, tempdir = candidates.get_candidates(version_number, dir)
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

	header_id = hdr.upload(db)
	for d in diags:
	    d.upload(db)
	print "\tEverything uploaded and checked successfully. header_id=%d" % \
		    header_id

    except (upload.UploadNonFatalError):
        exceptionmsgs = traceback.format_exception(*sys.exc_info())
        errormsg  = "Error while checking results!\n"
        errormsg += "\tResults Dir: %s\n\n" % dir
        errormsg += "".join(exceptionmsgs)

        sys.stderr.write("Error while checking results!\n")
        sys.stderr.write("Database transaction will not be committed.\n")
        sys.stderr.write("\t%s" % exceptionmsgs[-1])

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
        try:
            cftp = CornellFTP.CornellFTP()
            hdr.upload_FTP(cftp,db)
            cftp.quit()
            shutil.rmtree(tempdir)
        except:
            # add error handling here to catch FTP fails and do something smart
            db.rollback()
            raise

        print "Results successfully uploaded"

if __name__ == '__main__':
   if len(sys.argv) != 2:
     print "Usage: %s results_dir" % sys.argv[0] 
     exit(1)
   dir = sys.argv[1]
   upload_results(dir)
