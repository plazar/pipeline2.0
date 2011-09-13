#!/usr/bin/env python

"""
A new beam diagnostics loader for the PALFA survey.

Patrick Lazarus, Dec. 20th, 2010
"""
import re
import sys
import glob
import os.path
import tarfile
import optparse
import types
import binascii
import time

import numpy as np

import debug
import database
import upload
import pipeline_utils
from formats import accelcands
import config.basic


class Diagnostic(upload.Uploadable):
    """An abstract class to represent PALFA diagnostics.
    """
    # Define some class attributes
    description = None
    name = None
    
    def __init__(self, obs_name, beam_id, obstype, version_number, directory):
        self.obs_name = obs_name
        self.beam_id = beam_id
        self.obstype = obstype.lower()
        self.version_number = version_number
        self.directory = directory
        # Store a few configurations so the upload can be checked
        self.pipeline = config.basic.pipeline
        self.institution = config.basic.institution

    def get_diagnostic(self):
        raise NotImplementedError("Method 'get_diagnostic(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)

    def get_upload_sproc_call(self):
        raise NotImplementedError("Method 'get_upload_sproc_call(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)

    def upload(self, dbname='default', *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if debug.UPLOAD: 
            starttime = time.time()
        super(Diagnostic, self).upload(dbname=dbname, *args, **kwargs)
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary['diagnostics'] = \
                upload.upload_timing_summary.setdefault('diagnostics', 0) + \
                (time.time()-starttime)

class FloatDiagnostic(Diagnostic):
    """An abstract class to represent float-valued PALFA diagnostics.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'obs_name': '%s', \
              'beam_id': '%d', \
              'institution': '%s', \
              'pipeline': '%s', \
              'version_number': '%s', \
              'name': '%s', \
              'description': '%s', \
              'value': '%.12g', \
              'obstype': '%s'}
    
    def __init__(self, *args, **kwargs):
        super(FloatDiagnostic, self).__init__(*args, **kwargs)
        self.value = None # The diagnostic value to upload
        self.get_diagnostic()

    def get_upload_sproc_call(self):
        sprocstr = "EXEC spDiagnosticAdder " \
            "@obs_name='%s', " % self.obs_name + \
            "@beam_id=%d, " % self.beam_id + \
            "@instit='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s', " % self.version_number + \
            "@diagnostic_type_name='%s', " % self.name + \
            "@diagnostic_type_description='%s', " % self.description + \
            "@diagnostic=%.12g, " % self.value + \
            "@obsType='%s'" % self.obstype.lower()
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding diagnostic from DB and compare values.
            Raise a DiagnosticError if any mismatch is found.
            
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
        db.execute("SELECT obs.obs_name, " \
                        "h.beam_id, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number, " \
                        "dtype.diagnostic_type_name AS name, " \
                        "dtype.diagnostic_type_description AS description, " \
                        "d.diagnostic_value AS value, " \
                        "h.obsType AS obstype " \
                   "FROM diagnostics AS d " \
                   "LEFT JOIN diagnostic_types AS dtype " \
                        "ON dtype.diagnostic_type_id=d.diagnostic_type_id " \
                   "LEFT JOIN headers AS h ON h.header_id=d.header_id " \
                   "LEFT JOIN observations AS obs ON obs.obs_id=h.obs_id " \
                   "LEFT JOIN versions AS v ON v.version_id=d.version_id " \
                   "WHERE obs.obs_name='%s' AND h.beam_id=%d " \
                        "AND v.institution='%s' AND v.version_number='%s' " \
                        "AND v.pipeline='%s' AND h.obsType='%s' " \
                        "AND dtype.diagnostic_type_name='%s' " \
                        "AND dtype.diagnostic_type_description='%s' " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            self.version_number, config.basic.pipeline, \
                            self.obstype.lower(), self.name, self.description))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_type_name: %s,\n" \
                                " diagnostic_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_type_name: %s,\n" \
                                " diagnostic_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
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
                errormsg = "Float diagnostic doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise DiagnosticError(errormsg)


class PlotDiagnostic(Diagnostic):
    """An abstract class to represent binary PALFA diagnostics.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'obs_name': '%s', \
              'beam_id': '%d', \
              'institution': '%s', \
              'pipeline': '%s', \
              'version_number': '%s', \
              'name': '%s', \
              'description': '%s', \
              'value': '%s', # The binary file's name \
              'datalen': '%d', \
              'obstype': '%s'}
    
    def __init__(self, *args, **kwargs):
        super(PlotDiagnostic, self).__init__(*args, **kwargs)
        self.value = None # The binary file's name
        self.filedata = None # The binary file's data
        self.datalen = None # The number of bytes in the binary file
        self.get_diagnostic()

    def get_upload_sproc_call(self):
        sprocstr = "EXEC spDiagnosticPlotAdder " \
            "@obs_name='%s', " % self.obs_name + \
            "@beam_id=%d, " % self.beam_id + \
            "@instit='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s', " % self.version_number + \
            "@diagnostic_plot_type_name='%s', " % self.name + \
            "@diagnostic_plot_type_description='%s', " % self.description + \
            "@filename='%s', " % os.path.split(self.value)[-1] + \
            "@diagnostic_plot=0x%s, " % self.filedata.encode('hex') + \
            "@obsType='%s'" % self.obstype
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding diagnostic plot from DB and compare values.
            Raise a DiagnosticError if any mismatch is found.
            
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
        db.execute("SELECT obs.obs_name, " \
                        "h.beam_id, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number, " \
                        "dtype.diagnostic_plot_type_name AS name, " \
                        "dtype.diagnostic_plot_type_description AS description, " \
                        "d.filename AS value, " \
                        "DATALENGTH(d.diagnostic_plot) AS datalen, " \
                        "h.obsType AS obstype " \
                   "FROM diagnostic_plots AS d " \
                   "LEFT JOIN diagnostic_plot_types AS dtype " \
                        "ON dtype.diagnostic_plot_type_id=d.diagnostic_plot_type_id " \
                   "LEFT JOIN headers AS h ON h.header_id=d.header_id " \
                   "LEFT JOIN observations AS obs ON obs.obs_id=h.obs_id " \
                   "LEFT JOIN versions AS v ON v.version_id=d.version_id " \
                   "WHERE obs.obs_name='%s' AND h.beam_id=%d " \
                        "AND v.institution='%s' AND v.version_number='%s' " \
                        "AND v.pipeline='%s' AND h.obsType='%s' " \
                        "AND dtype.diagnostic_plot_type_name='%s' " \
                        "AND dtype.diagnostic_plot_type_description='%s' " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            self.version_number, config.basic.pipeline, \
                            self.obstype.lower(), self.name, self.description))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_plot_type_name: %s,\n" \
                                " diagnostic_plot_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_plot_type_name: %s,\n" \
                                " diagnostic_plot_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
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
                errormsg = "Float diagnostic doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise DiagnosticError(errormsg)


class RFIPercentageDiagnostic(FloatDiagnostic):
    name = "RFI mask percentage"
    description = "Percentage of data masked due to RFI."
    
    maskpcnt_re = re.compile(r"Number of  bad   intervals:.*\((?P<masked>.*)%\)")

    def get_diagnostic(self):
        # find *rfifind.out file
        rfiouts = glob.glob(os.path.join(self.directory, "*rfifind.out"))

        if len(rfiouts) != 1:
            raise DiagnosticError("Wrong number of rfifind output files found (%d)!" % \
                                    len(rfiouts))
        rfifile = open(rfiouts[0], 'r')
        for line in rfifile:
            m = self.maskpcnt_re.search(line)
            if m:
                self.value = float(m.groupdict()['masked'])
                break
        rfifile.close()


class RFIPlotDiagnostic(PlotDiagnostic):
    name = "RFIfind png"
    description = "Output image produced by rfifind in png format."

    def get_diagnostic(self):
        # find *rfifind.png file
        rfipngs = glob.glob(os.path.join(self.directory, '*rfifind.png'))

        if len(rfipngs) != 1:
            raise DiagnosticError("Wrong number of rfifind pngs found (%d)!" % \
                                len(rfipngs))
        else:
            self.value = os.path.split(rfipngs[0])[-1]
            self.datalen = os.path.getsize(rfipngs[0])
            rfipng_file = open(rfipngs[0], 'rb')
            self.filedata = rfipng_file.read()
            rfipng_file.close()


class AccelCandsDiagnostic(PlotDiagnostic):
    name = "Accelcands list"
    description = "The combined and sifted list of candidates " + \
                  "produced by accelsearch. (A text file)."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        accelcandsfile = open(candlists[0], 'r')
        self.value = os.path.split(accelcandsfile.name)[-1]
        self.filedata = accelcandsfile.read()
        self.datalen = os.path.getsize(accelcandsfile.name)
        accelcandsfile.close()


class NumFoldedDiagnostic(FloatDiagnostic):
    name = "Num cands folded"
    description = "The number of candidates folded."

    def get_diagnostic(self):
        pfdpngs = glob.glob(os.path.join(self.directory, "*.pfd.png"))
        self.value = len(pfdpngs)


class NumCandsDiagnostic(FloatDiagnostic):
    name = "Num cands produced"
    description = "The total number of candidates produced, including " \
                    "those with sigma lower than the folding threshold."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        candlist = accelcands.parse_candlist(candlists[0])
        self.value = len(candlist)


class MinSigmaFoldedDiagnostic(FloatDiagnostic):
    name = "Min sigma folded"
    description = "The smallest sigma value of all folded candidates "\
                    "from this beam."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))
        pfdpngs = [os.path.split(fn)[-1] for fn in \
                    glob.glob(os.path.join(self.directory, "*.pfd.png"))]

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        candlist = accelcands.parse_candlist(candlists[0])
        sigmas = []
        for c in candlist:
            base, accel = c.accelfile.split("_ACCEL_")
            pngfn = "%s_Z%s_ACCEL_Cand_%d.pfd.png" % (base, accel, c.candnum)
            if pngfn in pfdpngs:
                sigmas.append(c.sigma)
        if len(pfdpngs) > len(sigmas):
            raise DiagnosticError("Not all *.pfd.png images were found " \
                                    "in candlist! (%d > %d)" % \
                                    (len(pfdpngs), len(sigmas)))
        elif len(pfdpngs) < len(sigmas):
            raise DiagnosticError("Some *.pfd.png image match multiple " \
                                    "entries in candlist! (%d < %d)" % \
                                    (len(pfdpngs), len(sigmas)))

        if not sigmas:
            errormsg = 'No candidates folded.'
            raise DiagnosticNonFatalError(errormsg)

        self.value = min(sigmas)


class NumAboveThreshDiagnostic(FloatDiagnostic):
    name = "Num cands above threshold"
    description = "The number of candidates produced (but not necessarily folded) " \
                    "that are above the desired sigma threshold."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        candlist = accelcands.parse_candlist(candlists[0])
        
        params = get_search_params(self.directory)
        self.value = len([c for c in candlist \
                            if c.sigma >= params['to_prepfold_sigma']])


class ZaplistUsed(PlotDiagnostic):
    name = "Zaplist used"
    description = "The list of frequencies and ranges zapped from the " \
                    "power spectrum before searching this beam. (A text file)."

    def get_diagnostic(self):
        # find the *.zaplist file
        zaps = glob.glob(os.path.join(self.directory, '*.zaplist'))

        if len(zaps) != 1:
            raise DiagnosticError("Wrong number of zaplists found (%d)!" % \
                                len(zaps))
        else:
            self.value = os.path.split(zaps[0])[-1]
            self.datalen = os.path.getsize(zaps[0])
            zap_file = open(zaps[0], 'rb')
            self.filedata = zap_file.read()
            zap_file.close()


class PercentZappedBelow1Hz(FloatDiagnostic):
    name = "Percent zapped below 1 Hz"
    description = "The percentage of the power spectrum slower than 1 Hz " \
                    "that was zapped."

    def get_diagnostic(self):
        fctr, width = get_zaplist(self.directory)
        params = get_search_params(self.directory)
        lofreqs = np.clip((fctr - 0.5*width), \
                        1.0/params['sifting_long_period'], 1.0)
        hifreqs = np.clip((fctr + 0.5*width), \
                        1.0/params['sifting_long_period'], 1.0)
        self.value = np.sum(hifreqs-lofreqs) / \
                        (1.0/params['sifting_short_period'] - 1.0)*100


class PercentZappedBelow10Hz(FloatDiagnostic):
    name = "Percent zapped below 10 Hz"
    description = "The percentage of the power spectrum slower than 10 Hz " \
                    "that was zapped."

    def get_diagnostic(self):
        fctr, width = get_zaplist(self.directory)
        params = get_search_params(self.directory)
        lofreqs = np.clip((fctr - 0.5*width), \
                        1.0/params['sifting_long_period'], 10.0)
        hifreqs = np.clip((fctr + 0.5*width), \
                        1.0/params['sifting_long_period'], 10.0)
        self.value = np.sum(hifreqs-lofreqs) / \
                        (1.0/params['sifting_short_period'] - 10.0)*100


class PercentZappedTotal(FloatDiagnostic):
    name = "Percent zapped total"
    description = "The percentage of the power spectrum that was zapped."

    def get_diagnostic(self):
        fctr, width = get_zaplist(self.directory)
        params = get_search_params(self.directory)
        lofreqs = np.clip((fctr - 0.5*width), \
                        1.0/params['sifting_long_period'], \
                        1.0/params['sifting_short_period'])
        hifreqs = np.clip((fctr + 0.5*width), \
                        1.0/params['sifting_long_period'], \
                        1.0/params['sifting_short_period'])
        self.value = np.sum(hifreqs-lofreqs) / \
                        (1.0/params['sifting_short_period'] - \
                        1.0/params['sifting_long_period'])*100


class SearchParameters(PlotDiagnostic):
    name = "Search parameters"
    description = "The search parameters used when searching data " \
                    "with the PRESTO pipeline. (A text file)."

    def get_diagnostic(self):
        # find the search_params.txt file
        paramfn = get_search_paramfn(self.directory)
        self.value = os.path.split(paramfn)[-1]
        self.datalen = os.path.getsize(paramfn)
        param_file = open(paramfn, 'rb')
        self.filedata = param_file.read()
        param_file.close()


class SigmaThreshold(FloatDiagnostic):
    name = "Sigma threshold"
    description = "The sigma threshold used for determining which " \
                    "candidates potentially get folded."

    def get_diagnostic(self):
        # find the search parameters
        params = get_search_params(self.directory)
        self.value = params['to_prepfold_sigma']


class MaxCandsToFold(FloatDiagnostic):
    name = "Max cands allowed to fold"
    description = "The maximum number of candidates that are " \
                    "allowed to be folded."
    
    def get_diagnostic(self):
        # find the search parameters
        params = get_search_params(self.directory)
        self.value = params['max_cands_to_fold']


def get_search_paramfn(dir):
    # find the search_params.txt file
    paramfn = os.path.join(dir, 'search_params.txt')
    if not os.path.exists(paramfn):
        raise DiagnosticError("Search parameter file doesn't exist!")
    return paramfn


def get_search_params(dir):
    paramfn = get_search_paramfn(dir)
    tmp, params = {}, {}
    execfile(paramfn, tmp, params)
    return params


def get_zaplistfn(dir):
    # find the *.zaplist file
    zaps = glob.glob(os.path.join(dir, '*.zaplist'))

    if len(zaps) != 1:
        raise DiagnosticError("Wrong number of zaplists found (%d)!" % \
                            len(zaps))
    return zaps[0]


def get_zaplist(dir):
    zapfn = get_zaplistfn(dir)
    fctr, width = np.loadtxt(zapfn, unpack=True)
    return fctr, width


def find_in_tarballs(dir, matchfunc):
    """Find all tarballs in the given directory and search
        for a filename (inside the tarballs) for which
        matchfunc(FILENAME) returns True.

        Inputs:
            dir: directory in which to look for tarballs.
                    (i.e. *.tar, *.tgz, *.tar.gz)
            matchfunc: function that returns True when called with 
                    the desired filename as an argument.

        Output:
            t: the tarfile object containing the matching file (already opened)
            f: matching file-like object (already opened)

        NOTE: 't' and 'f' output by this function should be closed manually.
    """
    tar_suffixes = ['*.tar.gz', '*.tgz', '*.tar']

    # Find the tarball containing a matching file
    for suffix in tar_suffixes:
        tars = glob.glob(os.path.join(dir, suffix))
        for tar in tars:
            t = tarfile.open(tar, mode='r')
            for fn in t.getnames():
                if matchfunc(fn):
                    f = t.extractfile(fn)
                    return t, f
            t.close()
    raise DiagnosticError("Could not find matching file!")


class DiagnosticError(upload.UploadNonFatalError):
    """Error to throw when a diagnostic-specific problem 
        is encountered.
    """
    pass

class DiagnosticNonFatalError(pipeline_utils.PipelineError):
    pass


def get_diagnostics(obsname, beamnum, obstype, versionnum, directory):
    """Get diagnostic to common DB.
        
        Inputs:
            obsname: Observation name in the format:
                        {Project ID}.{Source name}.{MJD}.{Sequence number}
            beamnum: ALFA beam number (an integer between 0 and 7).
            obstype: Type of data (either 'wapp' or 'mock').
            versionnum: A combination of the githash values from 
                        PRESTO and from the pipeline. 
            directory: The directory containing results from the pipeline.

        Outputs:
            diagnostics: List of diagnostic objects.
    """
    if not 0 <= beamnum <= 7:
        raise DiagnosticError("Beam number must be between 0 and 7, inclusive!")
    
    diags = []
    # Loop over diagnostics, adding missing values to the DB
    for diagnostic_type in DIAGNOSTIC_TYPES:
        try:
            d = diagnostic_type(obsname, beamnum, obstype, \
                            versionnum, directory)
        except DiagnosticNonFatalError:
            continue

        except Exception:
            raise DiagnosticError("Could not create %s object for " \
                                    "observation: %s (beam: %d)" % \
                                    (diagnostic_type.__name__, obsname, beamnum))
        diags.append(d)
    return diags

# Define a list of diagnostics to apply
DIAGNOSTIC_TYPES = [RFIPercentageDiagnostic,
                    RFIPlotDiagnostic,
                    AccelCandsDiagnostic,
                    NumFoldedDiagnostic,
                    NumCandsDiagnostic,
                    MinSigmaFoldedDiagnostic,
                    NumAboveThreshDiagnostic,
                    ZaplistUsed,
                    SearchParameters,
                    SigmaThreshold,
                    MaxCandsToFold,
                    PercentZappedTotal,
                    PercentZappedBelow10Hz,
                    PercentZappedBelow1Hz,
                   ]


def main():
    db = database.Database('default', autocommit=False)
    try:
        diags = get_diagnostics(options.obsname, options.beamnum, \
                                options.versionnum, options.directory)
        for d in diags:
            d.upload(db)
    except:
        print "Rolling back..."
        db.rollback()
        raise
    else:
        db.commit()
    finally:
        db.close()


if __name__ == '__main__':
    parser = optparse.OptionParser(prog="diagnostics.py", \
                version="v0.8 (by Patrick Lazarus, Dec. 20, 2010)", \
                description="Upload diagnostics from a beam of PALFA " \
                            "data analysed using the pipeline2.0.")
    parser.add_option('--obsname', dest='obsname', \
                        help="The observation name is a combination of " \
                             "Project ID, Source name, MJD, and sequence number " \
                             "in the following format 'projid.srcname.mjd.seqnum'.")
    parser.add_option('--beamnum', dest='beamnum', type='int', \
                        help="Beam number (0-7).")
    parser.add_option('--versionnum', dest='versionnum', \
                        help="Version number is a combination of the PRESTO " \
                             "repository's git hash and the Pipeline2.0 " \
                             "repository's git has in the following format " \
                             "PRESTO:prestohash;pipeline:prestohash")
    parser.add_option('-d', '--directory', dest='directory',
                        help="Directory containing results from processing. " \
                             "Diagnostic information will be derived from the " \
                             "contents of this directory.")
    options, args = parser.parse_args()
    main()
