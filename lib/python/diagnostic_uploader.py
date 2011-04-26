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

import database
import upload
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

    def get_diagnostic(self):
        raise NotImplementedError("Method 'get_diagnostic(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)

    def get_upload_sproc_call(self):
        raise NotImplementedError("Method 'get_upload_sproc_call(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)


class FloatDiagnostic(Diagnostic):
    """An abstract class to represent float-valued PALFA diagnostics.
    """
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

    def compare_with_db(self, dbname='common-copy'):
        """Grab corresponding diagnostic from DB and compare values.
            Return True if all values match. Return False otherwise.
            
            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'common-copy').
            Output:
                match: Boolean. True if all values match, False otherwise.
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
                        "dtype.diagnostic_type_name, " \
                        "dtype.diagnostic_type_description, " \
                        "d.diagnostic_value, " \
                        "h.obsType " \
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
            matches = [('%s' % self.obs_name == '%s' % r['obs_name']), \
                     ('%d' % self.beam_id == '%s' % r['beam_id']), \
                     ('%s' % config.basic.institution.lower() == '%s' % r['institution'].lower()), \
                     ('%s' % config.basic.pipeline.lower() == '%s' % r['pipeline'].lower()), \
                     ('%s' % self.version_number == '%s' % r['version_number']), \
                     ('%s' % self.name == '%s' % r['diagnostic_type_name']), \
                     ('%s' % self.description == '%s' % r['diagnostic_type_description']), \
                     ('%.12g' % self.value == '%.12g' % r['diagnostic_value']), \
                     ('%s' % self.obstype.lower() == '%s' % r['obsType'].lower())]
            # Match is True if _all_ matches are True
            match = all(matches)
        return match


class PlotDiagnostic(Diagnostic):
    """An abstract class to represent binary PALFA diagnostics.
    """
    def __init__(self, *args, **kwargs):
        super(PlotDiagnostic, self).__init__(*args, **kwargs)
        self.value = None # The binary file's name
        self.filedata = None # The binary file's data
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

    def compare_with_db(self, dbname='common-copy'):
        """Grab corresponding diagnostic plot from DB and compare values.
            Return True if all values match. Return False otherwise.
            
            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'common-copy').
            Output:
                match: Boolean. True if all values match, False otherwise.
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
                        "dtype.diagnostic_plot_type_name, " \
                        "dtype.diagnostic_plot_type_description AS description, " \
                        "d.filename, " \
                        "d.diagnostic_plot, " \
                        "h.obsType " \
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
            matches = [('%s' % self.obs_name == '%s' % r['obs_name']), \
                     ('%d' % self.beam_id == '%s' % r['beam_id']), \
                     ('%s' % config.basic.institution.lower() == '%s' % r['institution'].lower()), \
                     ('%s' % config.basic.pipeline.lower() == '%s' % r['pipeline'].lower()), \
                     ('%s' % self.version_number == '%s' % r['version_number']), \
                     ('%s' % self.name == '%s' % r['diagnostic_plot_type_name']), \
                     ('%s' % self.description == '%s' % r['description']), \
                     ('0x%s' % self.filedata.encode('hex') == '0x%s' % binascii.b2a_hex(r['diagnostic_plot'])), \
                     ('%s' % os.path.split(self.value)[-1] == '%s' % os.path.split(r['filename'])[-1]), \
                     ('%s' % self.obstype.lower() == '%s' % r['obsType'].lower())]
            # Match is True if _all_ matches are True
            match = all(matches)
        return match


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
        
        # find the search_params.txt file
        paramfn = os.path.join(self.directory, 'search_params.txt')
        if os.path.exists(paramfn):
            tmp, params = {}, {}
            execfile(paramfn, tmp, params)
        else:
            raise DiagnosticError("Search parameter file doesn't exist!")
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
            zap_file = open(zaps[0], 'rb')
            self.filedata = zap_file.read()
            zap_file.close()


class SearchParameters(PlotDiagnostic):
    name = "Search parameters"
    description = "The search parameters used when searching data " \
                    "with the PRESTO pipeline. (A text file)."

    def get_diagnostic(self):
        # find the search_params.txt file
        paramfn = os.path.join(self.directory, 'search_params.txt')

        if os.path.exists(paramfn):
            self.value = os.path.split(paramfn)[-1]
            param_file = open(paramfn, 'rb')
            self.filedata = param_file.read()
            param_file.close()
        else:
            raise DiagnosticError("Search parameter file doesn't exist!")


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


class DiagnosticError(Exception):
    """Error to throw when a diagnostic-specific problem 
        is encountered.
    """
    pass


def check_diagnostics(obsname, beamnum, obstype, versionnum, directory, dbname='common-copy'):
    """Check diagnostics in common DB.
        
        Inputs:
            obsname: Observation name in the format:
                        {Project ID}.{Source name}.{MJD}.{Sequence number}
            beamnum: ALFA beam number (an integer between 0 and 7).
            obstype: Type of data (either 'wapp' or 'mock').
            versionnum: A combination of the githash values from 
                        PRESTO and from the pipeline. 
            directory: The directory containing results from the pipeline.
            dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'common-copy').
        Output:
            match: Boolean value. True if all diagnostics match what
                    is in the DB, False otherwise.
    """
    if not 0 <= beamnum <= 7:
        raise DiagnosticError("Beam number must be between 0 and 7, inclusive!")
    
    matches = []
    # Loop over diagnostics, adding missing values to the DB
    for diagnostic_type in DIAGNOSTIC_TYPES:
        try:
            d = diagnostic_type(obsname, beamnum, obstype, \
                            versionnum, directory)
        except Exception:
            raise DiagnosticError("Could not create %s object for " \
                                    "observation: %s (beam: %d)" % \
                                    (diagnostic_type.__name__, obsname, beamnum))
        matches.append(d.compare_with_db(dbname))
        if not matches[-1]:
            break
    return all(matches)


def upload_diagnostics(obsname, beamnum, obstype, versionnum, directory, \
                        verbose=False, dry_run=False, *args, **kwargs):
    """Upload diagnostic to common DB.
        
        Inputs:
            obsname: Observation name in the format:
                        {Project ID}.{Source name}.{MJD}.{Sequence number}
            beamnum: ALFA beam number (an integer between 0 and 7).
            obstype: Type of data (either 'wapp' or 'mock').
            versionnum: A combination of the githash values from 
                        PRESTO and from the pipeline. 
            directory: The directory containing results from the pipeline.
            verbose: An optional boolean value that determines if information 
                        is printed to stdout.
            dry_run: An optional boolean value. If True no connection to DB
                        will be made and DB command will not be executed.
                        (If verbose is True DB command will be printed 
                        to stdout.)

            *** NOTE: Additional arguments are passed to the uploader function.

        Outputs:
            diagnostic_ids: List of diagnostic IDs corresponding to these 
                        diagnostics in the common DB. (Or a list of None values 
                        if dry_run is True).
    """
    if not 0 <= beamnum <= 7:
        raise DiagnosticError("Beam number must be between 0 and 7, inclusive!")
    
    results = []
    # Loop over diagnostics, adding missing values to the DB
    for diagnostic_type in DIAGNOSTIC_TYPES:
    	if verbose:
        	print "Working on %s" % diagnostic_type.name
        try:
            d = diagnostic_type(obsname, beamnum, obstype, \
                            versionnum, directory)
        except Exception:
            raise DiagnosticError("Could not create %s object for " \
                                    "observation: %s (beam: %d)" % \
                                    (diagnostic_type.__name__, obsname, beamnum))
        if dry_run:
            d.get_upload_sproc_call()
            if verbose:
                print d
            results.append(None)
        else:
            result = d.upload(*args, **kwargs)
            if result < 0:
                raise DiagnosticError("An error was encountered! " \
                                        "(Error code: %d)" % result)
            if verbose:
                print "\tSuccess! (Return value: %d)" % result
            
            results.append(result)
    return results

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
                   ]


def main():
    try:
        upload_diagnostics(options.obsname, options.beamnum, \
                            options.versionnum, options.directory, \
                            options.verbose, options.dry_run)
    except upload.UploadError, e:
        traceback.print_exception(*sys.exc_info())
        sys.stderr.write("\nOriginal exception thrown:\n")
        traceback.print_exception(*e.orig_exc)


if __name__ == '__main__':
    parser = optparse.OptionParser(prog="diagnostic_uploader.py", \
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
    parser.add_option('--verbose', dest='verbose', action='store_true', \
                        help="Print success/failure information to screen. " \
                             "(Default: do not print).", \
                        default=False)
    parser.add_option('-n', '--dry-run', dest='dry_run', action='store_true', \
                        help="Perform a dry run. Do everything but connect to " \
                             "DB and upload diagnostics info. If --verbose " \
                             "is set, DB commands will be displayed on stdout. " \
                             "(Default: Connect to DB and execute commands).", \
                        default=False)
    options, args = parser.parse_args()
    main()
