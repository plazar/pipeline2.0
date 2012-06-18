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
import numpy as np
import scipy.special

import psr_utils
import prepfold

import debug
import database
import pipeline_utils
import upload
from formats import accelcands
import ratings2.rating_value

# get configurations
import config.basic
import config.upload
import config.searching

class PeriodicityCandidate(upload.Uploadable,upload.FTPable):
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
              'sigma': '%.12g', \
              'prepfold_sigma': '%.12g', \
              'rescaled_prepfold_sigma': '%.12g', \
              'sifting_period': '%.12g', \
              'sifting_dm': '%.12g'}

    def __init__(self, cand_num, pfd , snr, coherent_power, \
                        incoherent_power, num_hits, num_harmonics, \
                        versionnum, sigma, sifting_period, sifting_dm, \
                        header_id=None):
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

        red_chi2 = pfd.bestprof.chi_sqr #prepfold reduced chi-squared
        dof = pfd.proflen - 1 # degrees of freedom
        #prepfold sigma
        self.prepfold_sigma = \
                scipy.special.ndtri(scipy.special.chdtr(dof,dof*red_chi2)) 
        off_red_chi2 = pfd.estimate_offsignal_redchi2()
        chi2_scale = 1.0/off_red_chi2
        new_red_chi2 = chi2_scale * red_chi2
        # prepfold sigma rescaled to deal with chi-squared suppression
        # a problem when strong rfi is present
        self.rescaled_prepfold_sigma = \
                scipy.special.ndtri(scipy.special.chdtr(dof,dof*new_red_chi2))
        self.sifting_period = sifting_period # the period returned by accelsearch
                                             # (not optimized by prepfold)
        self.sifting_dm = sifting_dm # the DM returned by accelsearch
                                     # (not optimized by prepfold)

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
            dep.timestamp_mjd = self.timestamp_mjd
            dep.upload(dbname=dbname, *args, **kwargs)
        return cand_id

    def upload_FTP(self, cftp, dbname):
        for dep in self.dependents:
           if isinstance(dep,upload.FTPable):
               dep.upload_FTP(cftp,dbname=dbname)

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
            "@presto_sigma=%.12g" % self.sigma + \
            "@prepfold_sigma=%.12g" % self.prepfold_sigma + \
            "@rescaled_prepfold_sigma=%.12g" % self.rescaled_prepfold_sigma + \
            "@sifting_period=%.12g" % self.sifting_period + \
            "@sifting_dm=%.12g" %self.sifting_dm
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
                        "c.presto_sigma AS sigma, " \
                        "c.prepfold_sigma as prepfold_sigma, " \
                        "c.rescaled_prepfold_sigma as rescaled_prepfold_sigma, " \
                        "c.sifting_period as sifting_period, " \
                        "c.sifting_dm as sifting_dm " \
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

class PeriodicityCandidateBinary(upload.FTPable,upload.Uploadable):
    """A class to represent a periodicity candidate binary that
       needs to be FTPed to Cornell.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    # NEED TO UPDATE FOR BINARIES!
    to_cmp = {'cand_id': '%d', \
              'filetype': '%s', \
              'filename': '%s'}
    
    def __init__(self, filename, cand_id=None):
        self.cand_id = cand_id
        self.fullpath = filename 
        self.filename = os.path.split(filename)[-1]
        self.ftp_base = config.upload.pfd_ftp_dir

    def get_upload_sproc_call(self):
        """Return the EXEC spPFDBLAH string to upload
            this binary's info to the PALFA common DB.
        """
        sprocstr = "EXEC spPDMCandBinFSLoader " + \
            "@pdm_cand_id=%d, " % self.cand_id + \
            "@pdm_plot_type='%s', " % self.filetype + \
            "@filename='%s', " % self.filename + \
            "@file_location='%s', " % self.ftp_path + \
            "@uploaded=0 "

        return sprocstr

    def compare_with_db(self,dbname='default'):
        """Grab corresponding file info from DB and compare values.
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
        db.execute("SELECT bin.pdm_cand_id AS cand_id, " \
                        "pltype.pdm_plot_type AS filetype, " \
                        "bin.filename, " \
                        "bin.file_location AS ftp_path " \
                   "FROM PDM_Candidate_Binaries_Filesystem AS bin " \
                   "LEFT JOIN pdm_plot_types AS pltype " \
                        "ON bin.pdm_plot_type_id=pltype.pdm_plot_type_id " \
                   "WHERE bin.pdm_cand_id=%d AND pltype.pdm_plot_type='%s' " % \
                        (self.cand_id, self.filetype))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(pdm_cand_id: %d, filetype: %s)" % \
                                (self.cand_id, self.filetype))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(pdm_cand_id: %d, filetype: %s)" % \
                                (self.cand_id, self.filetype))
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
                errormsg = "Candidate binary info doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise PeriodicityCandidateError(errormsg)

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if self.cand_id is None:
            raise PeriodicityCandidateError("Cannot upload binary with " \
                    "pdm_cand_id == None!")

        mjd = int(self.timestamp_mjd)
        self.ftp_path = os.path.join(self.ftp_base,str(mjd))

        if debug.UPLOAD: 
            starttime = time.time()
        super(PeriodicityCandidateBinary, self).upload(dbname=dbname, \
                         *args, **kwargs)
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary[self.filetype + ' (db)'] = \
                upload.upload_timing_summary.setdefault(self.filetype + ' (db)', 0) + \
                (time.time()-starttime)

    def upload_FTP(self, cftp, dbname='default'): 
        """An extension to the inherited 'upload_FTP' method.
            This method FTP's the file to Cornell.

            Input:
                cftp: A CornellFTP connection.
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)

        if self.cand_id is None:
            raise PeriodicityCandidateError("Cannot FTP upload binary with " \
                    "pdm_cand_id == None!")
        if self.ftp_path is None:
            raise PeriodicityCandidateError("Cannot FTP upload binary with " \
                    "ftp_path == None!")

        if debug.UPLOAD: 
            starttime = time.time()
        try:
            ftp_fullpath = os.path.join(self.ftp_path, self.filename) 
            if not cftp.dir_exists(self.ftp_path):
                cftp.mkd(self.ftp_path)

            cftp.upload(self.fullpath, ftp_fullpath)
        except Exception, e:
            raise PeriodicityCandidateError("Error while uploading file %s via FTP:\n%s " %\
                                            (self.filename, str(e))) 
        else:
            db.execute("EXEC spPDMCandBinUploadConf " + \
                   "@pdm_plot_type='%s', " % self.filetype + \
                   "@filename='%s', " % self.filename + \
                   "@file_location='%s', " % self.ftp_path + \
                   "@uploaded=1") 
            db.commit() 

        if debug.UPLOAD:
            upload.upload_timing_summary[self.filetype + ' (ftp)'] = \
                upload.upload_timing_summary.setdefault(self.filetype + ' (ftp)', 0) + \
                (time.time()-starttime)
        

class PeriodicityCandidatePFD(PeriodicityCandidateBinary):
    """A class to represent periodicity candidate PFD files.
    """
    filetype = "pfd binary"


class PeriodicityCandidateRating(upload.Uploadable):
    """A class to represent a rating of a PALFA periodicity candidate.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'cand_id': '%d', \
              'value': '%.12g', \
              'version': '%d', \
              'name': '%s'}

    def __init__(self, ratval, cand_id=None):
        self.cand_id = cand_id
        self.ratval = ratval # A RatingValue object
        #self.instance_id = self.ratval.get_id()
        self.version = ratval.version
        self.name = ratval.name
        self.value = ratval.value
        

    def get_instance_id(self, dbname='default'):
        """Get the pdm_rating_instance_id for the rating
            from the rating name and versioni by querying
             the common DB."""
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT i.pdm_rating_instance_id FROM pdm_rating_type " + \
                   "AS r JOIN pdm_rating_instance AS i ON " + \
                   "r.pdm_rating_type_id = i.pdm_rating_type_id WHERE " + \
                   "r.name='%s' and i.version=%d" % (self.ratval.name, 
                                                     self.ratval.version))
        instance_id = db.cursor.fetchone()[0]

        if type(dbname) == types.StringType:
            db.close()

        return instance_id

    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of the database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        #if self.value is None:
        #    print self.value, type(self.value), self.name
        #if np.isnan(self.value) or np.isinf(self.value):
        #    print self.value, type(self.value), self.name
        if np.abs(self.value) < 1e-307:
            self.value = 0.0

        if self.cand_id is None:
            raise PeriodicityCandidateError("Cannot upload rating if " \
                    "pdm_cand_id is None!")
        if debug.UPLOAD: 
            starttime = time.time()

        self.instance_id = self.get_instance_id(dbname=dbname)

        #super(PeriodicityCandidateRating, self).upload(dbname=dbname, \
        #            *args, **kwargs)
        dbname.execute(self.get_upload_sproc_call())
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary["Ratings"] = \
                upload.upload_timing_summary.setdefault("Ratings", 0) + \
                (time.time()-starttime)

    def get_upload_sproc_call(self):
        """Return the SQL command to upload this candidate rating 
            to the PALFA common DB.
        """


        query = "INSERT INTO pdm_rating " + \
                "(value, pdm_rating_instance_id, pdm_cand_id, date) "

        if self.value is None or np.isnan(self.value) or np.isinf(self.value):
            query += "VALUES (NULL, %d, %d, GETDATE())" % \
                     (self.instance_id, \
                      self.cand_id)
        else:
            query += "VALUES ('%.12g', %d, %d, GETDATE())" % \
                    (self.ratval.value, \
                    self.instance_id, \
                    self.cand_id)

        return query

    def compare_with_db(self, dbname='default'):
        """Grab the rating information from the DB and compare values.
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
        db.execute("SELECT r.pdm_cand_id AS cand_id, " \
                        "r.value AS value, " \
                        "rt.name AS name, " \
                        "ri.version AS version " \
                   "FROM pdm_rating AS r " \
                   "LEFT JOIN pdm_rating_instance AS ri " \
                        "ON ri.pdm_rating_instance_id=r.pdm_rating_instance_id " \
                   "LEFT JOIN pdm_rating_type AS rt " \
                        "ON ri.pdm_rating_type_id=rt.pdm_rating_type_id " \
                   "WHERE r.pdm_cand_id=%d AND r.pdm_rating_instance_id=%d " % \
                        (self.cand_id, self.instance_id))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(pdm_cand_id: %d, " \
                                "rating name: %s, " \
                                "rating version: %d, " \
                                "fetched pdm_rating_instance_id: %d)" % \
                                (self.cand_id, self.ratval.name, \
                                self.ratval.version, self.instance_id))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(pdm_cand_id: %d, " \
                                "rating name: %s, " \
                                "rating version: %d, " \
                                "fetched pdm_rating_instance_id: %d)" % \
                                (self.cand_id, self.ratval.name, \
                                self.ratval.version, self.instance_id))
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
                errormsg = "Candidate rating doesn't match what was " \
                            "uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise PeriodicityCandidateError(errormsg)


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
            tempdir: Path of temporary directory that PFDs have been untarred,
                     returned so that it can be deleted after successful PFD upload.
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
        ratfn = os.path.join(directory, basefn+".pfd.rat")

        pfd = prepfold.pfd(pfdfn)
        
        try:
            cand = PeriodicityCandidate(ii+1, pfd, c.snr, \
                                    c.cpow, c.ipow, len(c.dmhits), \
                                    c.numharm, versionnum, c.sigma, \
                                    c.period, c.dm, header_id=header_id)
        except Exception:
            raise PeriodicityCandidateError("PeriodicityCandidate could not be " \
                                            "created (%s)!" % pfdfn)


        cand.add_dependent(PeriodicityCandidatePFD(pfdfn))
        cand.add_dependent(PeriodicityCandidatePNG(pngfn))
        for ratval in ratings2.rating_value.read_file(ratfn):
            cand.add_dependent(PeriodicityCandidateRating(ratval))
        cands.append(cand)
        
    #shutil.rmtree(tempdir)
    return cands,tempdir


def main():
    import CornellFTP
    db = database.Database('default', autocommit=False)
    try:
        cands,tempdir = get_candidates(options.versionnum, options.directory, \
                            header_id=options.header_id)
        for cand in cands:
            cand.upload(db)
    except:
        print "Rolling back..."
        db.rollback()
        raise
    else:
        db.commit()
        for cand in cands:
            # FTP ftpables 
            cftp = CornellFTP.CornellFTP()
            cand.upload_FTP(cftp,db)
            cftp.close()
        shutil.rmtree(tempdir)
        
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
 

