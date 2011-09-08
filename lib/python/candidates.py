#!/usr/bin/env python

"""
A candidate uploader for the PALFA survey.

Patrick Lazarus, Jan. 10th, 2011
"""
import os.path
import sys
import glob
import tarfile
import tempfile
import optparse
import traceback
import datetime
import shutil
import types
import binascii
import time

import psr_utils
import prepfold

import debug
import database
import pipeline_utils
import upload
from formats import accelcands

# get configurations
import config.basic
import config.searching

class PeriodicityCandidate(upload.Uploadable):
    """A class to represent a PALFA periodicity candidate.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'header_id': '%d', \
              'cand_num': '%d', \
              'bary_freq': '%.12g', \
              'bary_freq': '%.12g', \
              'topo_period': '%.12g', \
              'bary_period': '%.12g', \
              'topo_f_dot': '%.12g', \
              'bary_f_dot': '%.12g', \
              'dm': '%.12g', \
              'snr': '%.12g', \
              'coherent_power': '%.12g', \
              'incoherent_power': '%.12g', \
              'num_hits': '%d', \
              'num_harmonics': '%d', \
              'institution': '%s', \
              'pipeline': '%s', \
              'versionnum': '%s', \
              'sigma': '%.12g'}

    def __init__(self, cand_num, pfd , snr, coherent_power, \
                        incoherent_power, num_hits, num_harmonics, \
                        versionnum, sigma, header_id=None):
        self.header_id = header_id # Header ID from database
        self.cand_num = cand_num # Unique identifier of candidate within beam's 
                                 # list of candidates; Candidate's position in
                                 # a list of all candidates produced in beam
                                 # ordered by decreasing sigma (where largest
                                 # sigma has cand_num=1).
        self.topo_freq, self.topo_f_dot, fdd = \
                psr_utils.p_to_f(pfd.topo_p1, pfd.topo_p2, pfd.topo_p3)
        self.bary_freq, self.bary_f_dot, baryfdd = \
                psr_utils.p_to_f(pfd.bary_p1, pfd.bary_p2, pfd.bary_p3)
        self.dm = pfd.bestdm # Dispersion measure
        self.snr = snr # signal-to-noise ratio
        self.coherent_power = coherent_power # Coherent power
        self.incoherent_power = incoherent_power # Incoherent power
        self.num_hits = num_hits # Number of dedispersed timeseries candidate was found in
        self.num_harmonics = num_harmonics # Number of harmonics candidate was 
                                           # most significant with
        self.versionnum = versionnum # Version number; a combination of PRESTO's githash
                                     # and pipeline's githash
        self.sigma = sigma # PRESTO's sigma value

        # Store a few configurations so the upload can be checked
        self.pipeline = config.basic.pipeline
        self.institution = config.basic.institution
    
        # Calculate a few more values
        self.topo_period = 1.0/self.topo_freq
        self.bary_period = 1.0/self.bary_freq

        # List of dependents (ie other uploadables that require 
        # the pdm_cand_id from this candidate)
        self.dependents = []

    def add_dependent(self, dep):
        self.dependents.append(dep)

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.
            This method will make sure any dependents have
            the pdm_cand_id and then upload them.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.header_id is None:
            raise PeriodicityCandidateError("Cannot upload candidate with " \
                    "header_id == None!")
        if debug.UPLOAD: 
            starttime = time.time()
        cand_id = super(PeriodicityCandidate, self).upload(dbname=dbname, \
                    *args, **kwargs)[0]
        
        self.compare_with_db(dbname=dbname)

        if debug.UPLOAD:
            upload.upload_timing_summary['candidates'] = \
                upload.upload_timing_summary.setdefault('candidates', 0) + \
                (time.time()-starttime)
        for dep in self.dependents:
            dep.cand_id = cand_id
            dep.upload(dbname=dbname, *args, **kwargs)
        return cand_id

    def get_upload_sproc_call(self):
        """Return the EXEC spPDMCandUploaderFindsVersion string to upload
            this candidate to the PALFA common DB.
        """
        sprocstr = "EXEC spPDMCandUploaderFindsVersion " + \
            "@header_id=%d, " % self.header_id + \
            "@cand_num=%d, " % self.cand_num + \
            "@frequency=%.12g, " % self.topo_freq + \
            "@bary_frequency=%.12g, " % self.bary_freq + \
            "@period=%.12g, " % self.topo_period + \
            "@bary_period=%.12g, " % self.bary_period + \
            "@f_dot=%.12g, " % self.topo_f_dot + \
            "@bary_f_dot=%.12g, " % self.bary_f_dot + \
            "@dm=%.12g, " % self.dm + \
            "@snr=%.12g, " % self.snr + \
            "@coherent_power=%.12g, " % self.coherent_power + \
            "@incoherent_power=%.12g, " % self.incoherent_power + \
            "@num_hits=%d, " % self.num_hits + \
            "@num_harmonics=%d, " % self.num_harmonics + \
            "@institution='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s', " % self.versionnum + \
            "@proc_date='%s', " % datetime.date.today().strftime("%Y-%m-%d") + \
            "@presto_sigma=%.12g" % self.sigma
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding candidate from DB and compare values.
            Raise a PeriodicityCandidateError if any mismatch is found.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Outputs:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT c.header_id, " \
                        "c.cand_num, " \
                        "c.frequency AS topo_freq, " \
                        "c.bary_frequency AS bary_freq, " \
                        "c.period AS topo_period, " \
                        "c.bary_period, " \
                        "c.f_dot AS topo_f_dot, " \
                        "c.bary_f_dot, " \
                        "c.dm, " \
                        "c.snr, " \
                        "c.coherent_power, " \
                        "c.incoherent_power, " \
                        "c.num_hits, " \
                        "c.num_harmonics, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number AS versionnum, " \
                        "c.presto_sigma AS sigma " \
                  "FROM pdm_candidates AS c " \
                  "LEFT JOIN versions AS v ON v.version_id=c.version_id " \
                  "WHERE c.cand_num=%d AND v.version_number='%s' AND " \
                            "c.header_id=%d " % \
                        (self.cand_num, self.versionnum, self.header_id))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(header_id: %d, cand_num: %d, version_number: %s)" % \
                                (self.header_id, self.cand_num, self.versionnum))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(header_id: %d, cand_num: %d, version_number: %s)" % \
                                (self.header_id, self.cand_num, self.versionnum))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "Candidate doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise PeriodicityCandidateError(errormsg)


class PeriodicityCandidatePlot(upload.Uploadable):
    """A class to represent the plot of a PALFA periodicity candidate.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'cand_id': '%d', \
              'plot_type': '%s', \
              'filename': '%s', \
              'datalen': '%d'}
    
    def __init__(self, plotfn, cand_id=None):
        self.cand_id = cand_id
        self.filename = os.path.split(plotfn)[-1]
        self.datalen = os.path.getsize(plotfn)
        plot = open(plotfn, 'r')
        self.filedata = plot.read()
        plot.close()

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.cand_id is None:
            raise PeriodicityCandidateError("Cannot upload plot with " \
                    "pdm_cand_id == None!")
        if debug.UPLOAD: 
            starttime = time.time()
        super(PeriodicityCandidatePlot, self).upload(dbname=dbname, \
                    *args, **kwargs)
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary[self.plot_type] = \
                upload.upload_timing_summary.setdefault(self.plot_type, 0) + \
                (time.time()-starttime)

    def get_upload_sproc_call(self):
        """Return the EXEC spPDMCandPlotUploader string to upload
            this candidate plot to the PALFA common DB.
        """
        sprocstr = "EXEC spPDMCandPlotLoader " + \
            "@pdm_cand_id=%d, " % self.cand_id + \
            "@pdm_plot_type='%s', " % self.plot_type + \
            "@filename='%s', " % os.path.split(self.filename)[-1] + \
            "@filedata=0x%s" % self.filedata.encode('hex')
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding candidate plot from DB and compare values.
            Raise a PeriodicityCandidateError if any mismatch is found.
            
            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT plt.pdm_cand_id AS cand_id, " \
                        "pltype.pdm_plot_type AS plot_type, " \
                        "plt.filename, " \
                        "DATALENGTH(plt.filedata) AS datalen " \
                   "FROM pdm_candidate_plots AS plt " \
                   "LEFT JOIN pdm_plot_types AS pltype " \
                        "ON plt.pdm_plot_type_id=pltype.pdm_plot_type_id " \
                   "WHERE plt.pdm_cand_id=%d AND pltype.pdm_plot_type='%s' " % \
                        (self.cand_id, self.plot_type))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(pdm_cand_id: %d, pdm_plot_type: %s)" % \
                                (self.cand_id, self.plot_type))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(pdm_cand_id: %d, pdm_plot_type: %s)" % \
                                (self.cand_id, self.plot_type))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "Candidate plot doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise PeriodicityCandidateError(errormsg)


class PeriodicityCandidatePNG(PeriodicityCandidatePlot):
    """A class to represent periodicity candidate PNGs.
    """
    plot_type = "prepfold plot"


class PeriodicityCandidatePFD(PeriodicityCandidatePlot):
    """A class to represent periodicity candidate PFD files.
    """
    plot_type = "pfd binary"


class PeriodicityCandidateError(upload.UploadNonFatalError):
    """Error to throw when a candidate-specific problem is encountered.
    """
    pass


def get_candidates(versionnum, directory, header_id=None):
    """Upload candidates to common DB.

        Inputs:
            versionnum: A combination of the githash values from 
                        PRESTO and from the pipeline. 
            directory: The directory containing results from the pipeline.
            header_id: header_id number for this beam, as returned by
                        spHeaderLoader/header.upload_header (default=None)

        Ouput:
            cands: List of candidates.
    """
    # find *.accelcands file    
    candlists = glob.glob(os.path.join(directory, "*.accelcands"))
                                                
    if len(candlists) != 1:
        raise PeriodicityCandidateError("Wrong number of candidate lists found (%d)!" % \
                                            len(candlists))

    # Get list of candidates from *.accelcands file
    candlist = accelcands.parse_candlist(candlists[0])
    # find the search_params.txt file
    paramfn = os.path.join(directory, 'search_params.txt')
    if os.path.exists(paramfn):
        tmp, params = {}, {}
        execfile(paramfn, tmp, params)
    else:
        raise PeriodicityCandidateError("Search parameter file doesn't exist!")
    minsigma = params['to_prepfold_sigma']
    foldedcands = [c for c in candlist \
                    if c.sigma > params['to_prepfold_sigma']]
    foldedcands = foldedcands[:params['max_cands_to_fold']]
    foldedcands.sort(reverse=True) # Sort by descending sigma

        
    # Create temporary directory
    tempdir = tempfile.mkdtemp(suffix="_tmp", prefix="PALFA_pfds_")

    if foldedcands:

        tarfns = glob.glob(os.path.join(directory, "*_pfd.tgz"))
        if len(tarfns) != 1:
            raise PeriodicityCandidateError("Wrong number (%d) of *_pfd.tgz " \
                                             "files found in %s" % (len(tarfns), \
                                                directory))
        
        tar = tarfile.open(tarfns[0])
        try:
            tar.extractall(path=tempdir)
        except IOError:
            if os.path.isdir(tempdir):
                shutil.rmtree(tempdir)
            raise PeriodicityCandidateError("Error while extracting pfd files " \
                                            "from tarball (%s)!" % tarfns[0])
        finally:
            tar.close()

    # Loop over candidates that were folded
    cands = []
    for ii, c in enumerate(foldedcands):
        basefn = "%s_ACCEL_Cand_%d" % (c.accelfile.replace("ACCEL_", "Z"), \
                                    c.candnum)
        pfdfn = os.path.join(tempdir, basefn+".pfd")
        pngfn = os.path.join(directory, basefn+".pfd.png")
        
        pfd = prepfold.pfd(pfdfn)
        
        try:
            cand = PeriodicityCandidate(ii+1, pfd, c.snr, \
                                    c.cpow, c.ipow, len(c.dmhits), \
                                    c.numharm, versionnum, c.sigma, \
                                    header_id=header_id)
        except Exception:
            raise PeriodicityCandidateError("PeriodicityCandidate could not be " \
                                            "created (%s)!" % pfdfn)


        cand.add_dependent(PeriodicityCandidatePFD(pfdfn))
        cand.add_dependent(PeriodicityCandidatePNG(pngfn))
        cands.append(cand)
        
    shutil.rmtree(tempdir)
    return cands


def main():
    db = database.Database('default', autocommit=False)
    try:
        cands = get_candidates(options.versionnum, options.directory, \
                            header_id=options.header_id)
        for cand in cands:
            cand.upload(db)
    except:
        print "Rolling back..."
        db.rollback()
        raise
    else:
        db.commit()
    finally:
        db.close()


if __name__ == '__main__':
    parser = optparse.OptionParser(prog="candidates.py", \
                version="v0.8 (by Patrick Lazarus, Jan. 12, 2011)", \
                description="Upload candidates from a beam of PALFA " \
                            "data analysed using the pipeline2.0.")
    parser.add_option('--header-id', dest='header_id', type='int', \
                        help="Header ID of this beam from the common DB.", \
                        default=None)
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
    options, args = parser.parse_args()
    main()
 

