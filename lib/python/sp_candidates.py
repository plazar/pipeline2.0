#!/usr/bin/env python

"""
A singpulse candidate uploader for the PALFA survey.

Patrick Lazarus, May 15th, 2011
"""
import os.path
import optparse
import glob
import types
import traceback
import binascii
import sys

import CornellFTP
import database
import upload
import pipeline_utils

import config.basic

class SinglePulseTarball(upload.Uploadable):
    """A class to represent a tarball of single pulse files.
    """
    def __init__(self, header_id, filename, versionnum):
        self.header_id = header_id
        self.filename = filename
        self.versionnum = versionnum

    def get_upload_sproc_call(self):
        """Return the EXEC spSinglePulseFileUpload string to upload
            this tarball's info to the PALFA common DB.
        """
        sprocstr = "EXEC spSinglePulseFileUpload " + \
            "@filename='%s', " % os.path.split(self.filename)[-1] + \
            "@header_id=%d, " % self.header_id + \
            "@filetype='%s', " % self.filetype + \
            "@institution='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s'" % self.versionnum
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding file info from DB and compare values.
            Return True if all values match. Return False otherwise.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                match: Boolean. True if all values match, False otherwise.
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT spf.header_id, " \
                        "spf.filename, " \
                        "spft.sp_files_type, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number " \
                  "FROM sp_files_info AS spf " \
                  "LEFT JOIN versions AS v ON v.version_id=spf.version_id " \
                  "LEFT JOIN sp_files_types AS spft " \
                        "ON spft.sp_files_type_id=spf.sp_files_type_id " \
                  "WHERE spft.sp_files_type='%s' AND v.version_number='%s' AND " \
                            "spf.header_id=%d AND v.institution='%s' AND " \
                            "v.pipeline='%s'" % \
                        (self.filetype, self.versionnum, self.header_id, \
                            config.basic.institution, config.basic.pipeline))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(header_id: %d, filetype: %s, version_number: %s)" % \
                                (self.header_id, self.filetype, self.versionnum))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(header_id: %d, filetype: %s, version_number: %s)" % \
                                (self.header_id, self.filetype, self.versionnum))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            matches = [('%s' % os.path.split(self.filename)[-1] == '%s' % r['filename']), \
                     ('%d' % self.header_id == '%d' % r['header_id']), \
                     ('%s' % self.filetype == '%s' % r['sp_files_type']), \
                     ('%s' % self.versionnum == '%s' % r['version_number']), \
                     ('%s' % config.basic.institution.lower() == '%s' % r['institution'].lower()), \
                     ('%s' % config.basic.pipeline.lower() == '%s' % r['pipeline'].lower())]
            # Match is True if _all_ matches are True
            match = all(matches)
        return match

    def upload(self, *args, **kwargs):
        """And extension to the inherited 'upload' method.
            This method FTP's the file to Cornell instead of
            inserting it into the DB as a BLOB.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        id, path = super(SinglePulseTarball, self).upload(*args, **kwargs)
        if id < 0:
            # An error has occurred
            raise SinglePulseCandidateError(path)
        else:
            cftp = CornellFTP.CornellFTP()
            ftp_path = os.path.join(path, os.path.split(self.filename)[-1]) 
            cftp.upload(self.filename, ftp_path)
            cftp.quit()


class SinglePulseCandsTarball(SinglePulseTarball):
    """A class to represent a tarball of *.singlepulse files.
    """
    filetype = "PRESTO singlepulse candidate tarball"


class SinglePulseInfTarball(SinglePulseTarball):
    """A class to represent a tarball of the *.inf files 
        required by *.singlepulse files.
    """
    filetype = "PRESTO singlepulse inf tarball"


class SinglePulseBeamPlot(upload.Uploadable):
    """A class to represent a per-beam single pulse plot.
    """
    def __init__(self, header_id, plotfn, versionnum):
        self. header_id = header_id
        self.versionnum = versionnum
        self.filename = os.path.split(plotfn)[-1]
        plot = open(plotfn, 'r')
        self.filedata = plot.read()
        plot.close()

    def get_upload_sproc_call(self):
        """Return the EXEC spSPSingleBeamCandPlotLoader string to
            upload this singlepulse plot to the PALFA common DB.
        """
        sprocstr = "EXEC spSPSingleBeamCandPlotLoader " + \
            "@header_id='%d', " % self.header_id + \
            "@sp_plot_type='%s', " % self.sp_plot_type + \
            "@filename='%s', " % os.path.split(self.filename)[-1] + \
            "@filedata=0x%s, " % self.filedata.encode('hex') + \
            "@institution='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s'" % self.versionnum
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding singlepulse plot from DB and compare values.
            Return True if all values match. Return False otherwise.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                match: Boolean. True if all values match, False otherwise.
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT spsb.header_id, " \
                        "spsbtype.sp_single_beam_plot_type, " \
                        "spsb.filename, " \
                        "spsb.filedata, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number " \
                    "FROM sp_plots_single_beam AS spsb " \
                    "LEFT JOIN versions AS v on v.version_id=spsb.version_id " \
                    "LEFT JOIN sp_single_beam_plot_types AS spsbtype " \
                        "ON spsb.sp_single_beam_plot_type_id=spsbtype.sp_single_beam_plot_type_id " \
                    "WHERE spsb.header_id=%d AND v.version_number='%s' AND " \
                            "v.institution='%s' AND v.pipeline='%s' AND " \
                            "spsbtype.sp_single_beam_plot_type='%s'" % \
                        (self.header_id, self.versionnum, \
                            config.basic.institution, config.basic.pipeline, \
                            self.sp_plot_type))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(header_id: %d, sp_plot_type: %s, version_number: %s)" % \
                                (self.header_id, self.sp_plot_type, self.versionnum))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(header_id: %d, sp_plot_type: %s, version_number: %s)" % \
                                (self.header_id, self.sp_plot_type, self.versionnum))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            matches = [('%s' % os.path.split(self.filename)[-1] == '%s' % r['filename']), \
                     ('%d' % self.header_id == '%d' % r['header_id']), \
                     ('%s' % self.sp_plot_type == '%s' % r['sp_single_beam_plot_type']), \
                     ('%s' % self.versionnum == '%s' % r['version_number']), \
                     ('%s' % config.basic.institution.lower() == '%s' % r['institution'].lower()), \
                     ('%s' % config.basic.pipeline.lower() == '%s' % r['pipeline'].lower()), \
                     ('0x%s' % self.filedata.encode('hex') == '0x%s' % binascii.b2a_hex(r['filedata']))]
            # Match is True if _all_ matches are True
            match = all(matches)
        return match


class SinglePulseBeamPlotDMs0_110(SinglePulseBeamPlot):
    """A class to represent a PRESTO per-beam singlepulse plot for
        DMs 0 to 110.
    """
    sp_plot_type = "PRESTO singlepulse per-beam plot (DMs 0 to 110)"


class SinglePulseBeamPlotDMs100_310(SinglePulseBeamPlot):
    """A class to represent a PRESTO per-beam singlepulse plot for
        DMs 100 to 310.
    """
    sp_plot_type = "PRESTO singlepulse per-beam plot (DMs 100 to 310)"


class SinglePulseBeamPlotDMs300AndUp(SinglePulseBeamPlot):
    """A class to represent a PRESTO per-beam singlepulse plot for
        DMs 300 and up.
    """
    sp_plot_type = "PRESTO singlepulse per-beam plot (DMs 300 and up)"


class SinglePulseCandidateError(pipeline_utils.PipelineError):
    """Error to throw when a single pulse candidate-specific problem is encountered.
    """
    pass


def check_spcandidates(header_id, versionnum, directory, dbname='default'):
    """Check single pulse candidates in common DB.

        Inputs:
            header_id: header_id number for this beam, as returned by
                        spHeaderLoader/header.upload_header
            versionnum: A combination of the githash values from 
                        PRESTO, the pipeline, and psrfits_utils. 
            directory: The directory containing results from the pipeline.
            dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        Output:
            match: Boolean value. True if all single pulse candidates and 
                    plots match what is in the DB, False otherwise.
    """
    matches = []
    if isinstance(dbname, database.Database):
        db = dbname
    else:
        db = database.Database(dbname)
   
    to_check = []
    # Gather plots to upload
    fns = glob.glob(os.path.join(directory, "*DMs0-110_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs0-110_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    to_check.append(SinglePulseBeamPlotDMs0_110(header_id, fns[0], versionnum))

    fns = glob.glob(os.path.join(directory, "*DMs100-310_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs100-310_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    to_check.append(SinglePulseBeamPlotDMs100_310(header_id, fns[0], versionnum))

    fns = glob.glob(os.path.join(directory, "*DMs300-1000+_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs300-1000_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    to_check.append(SinglePulseBeamPlotDMs300AndUp(header_id, fns[0], versionnum))
    
    # Gather tarballs to upload
    fns = glob.glob(os.path.join(directory, "*_inf.tgz"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *_inf.tgz " \
                                        "tarballs found (%d)!" % len(fns))
    to_check.append(SinglePulseInfTarball(header_id, fns[0], versionnum))
    
    fns = glob.glob(os.path.join(directory, "*_singlepulse.tgz"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *_singlepulse.tgz " \
                                        "tarballs found (%d)!" % len(fns))
    to_check.append(SinglePulseCandsTarball(header_id, fns[0] , versionnum))

    for to_chk in to_check:
        matches.append(to_chk.compare_with_db(dbname))
        if not matches[-1]:
            break

    if type(dbname) == types.StringType:
        db.close()
    return all(matches)


def upload_spcandidates(header_id, versionnum, directory, verbose=False, \
                        dry_run=False, *args, **kwargs):
    """Upload single pulse candidates to common DB.

        Inputs:
            header_id: header_id number for this beam, as returned by
                        spHeaderLoader/header.upload_header
            versionnum: A combination of the githash values from 
                        PRESTO, the pipeline, and psrfits_utils.
            directory: The directory containing results from the pipeline.
            verbose: An optional boolean value that determines if information 
                        is printed to stdout.
            dry_run: An optional boolean value. If True no connection to DB
                        will be made and DB command will not be executed.
                        (If verbose is True DB command will be printed 
                        to stdout.)

            *** NOTE: Additional arguments are passed to the uploader function.

        Ouputs:
            sp_cand_ids: List of single pulse candidate IDs corresponding 
                        to these candidates in the common DB. (Or a list 
                        of None values if dry_run is True).
    """
    results = []
    to_upload = []
   
    # Gather plots to upload
    fns = glob.glob(os.path.join(directory, "*DMs0-110_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs0-110_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    to_upload.append(SinglePulseBeamPlotDMs0_110(header_id, fns[0], versionnum))

    fns = glob.glob(os.path.join(directory, "*DMs100-310_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs100-310_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    to_upload.append(SinglePulseBeamPlotDMs100_310(header_id, fns[0], versionnum))

    fns = glob.glob(os.path.join(directory, "*DMs300-1000+_singlepulse.png"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *DMs300-1000_singlepulse.png " \
                                        "plots found (%d)!" % len(fns))
    to_upload.append(SinglePulseBeamPlotDMs300AndUp(header_id, fns[0], versionnum))
    
    # Gather tarballs to upload
    fns = glob.glob(os.path.join(directory, "*_inf.tgz"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *_inf.tgz " \
                                        "tarballs found (%d)!" % len(fns))
    to_upload.append(SinglePulseInfTarball(header_id, fns[0], versionnum))
    
    fns = glob.glob(os.path.join(directory, "*_singlepulse.tgz"))
    if len(fns) != 1:
        raise SinglePulseCandidateError("Wrong number of *_singlepulse.tgz " \
                                        "tarballs found (%d)!" % len(fns))
    to_upload.append(SinglePulseCandsTarball(header_id, fns[0] , versionnum))

    for to_up in to_upload:
        if dry_run:
            to_up.get_upload_sproc_call()
            if verbose:
                print to_up
        else:
            to_up.upload(*args, **kwargs)
    
    return results


def main():
    db = database.Database('default', autocommit=False)
    try:
        upload_spcandidates(options.header_id, options.versionnum, \
                            options.directory, options.verbose, \
                            options.dry_run, dbname=db)
        check_spcandidates(options.header_id, options.versionnum, \
                            options.directory, dbname=db)
    except upload.UploadError, e:
        print "Rolling back..."
        db.rollback()
        traceback.print_exception(*sys.exc_info())
        sys.stderr.write("\nOriginal exception thrown:\n")
        traceback.print_exception(*e.orig_exc)
    except:
        print "Rolling back..."
        db.rollback()
        raise
    else:
        db.commit()
    finally:
        db.close()


if __name__ == '__main__':
    parser = optparse.OptionParser(prog="spcandidates.py", \
                version="v0.8 (by Patrick Lazarus, May 18th, 2011)", \
                description="Upload single pulse candidates from a " \
                            "beam of PALFA data analysed using the " \
                            "pipeline2.0.")
    parser.add_option('--header-id', dest='header_id', type='int', \
                        help="Header ID of this beam from the common DB.")
    parser.add_option('--versionnum', dest='versionnum', \
                        help="Version number is a combination of the PRESTO " \
                             "repository's git hash, the Pipeline2.0 " \
                             "repository's git hash, and the psrfits_utils " \
                             "repository's git hash. It has the following format " \
                             "PRESTO:githash;pipeline:githash;" \
                             "psrfits_utils:githash")
    parser.add_option('-d', '--directory', dest='directory',
                        help="Directory containing results from processing. " \
                             "Diagnostic information will be derived from the " \
                             "contents of this directory.")
    parser.add_option('--verbose', dest='verbose', action='store_true', \
                        help="Print success/failure information to screen. " \
                             "(Default: do not print).", \
                        default=False)
    parser.add_option('-n', '--dry-run', dest='dry_run', action='store_true', \
                        help="Perform a dry run. Do everything but connect to " \
                             "DB and upload candidate info. If --verbose " \
                             "is set, DB commands will be displayed on stdout. " \
                             "(Default: Connect to DB and execute commands).", \
                        default=False)
    options, args = parser.parse_args()
    main()
        
