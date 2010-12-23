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
import datetime

import database

from formats import accelcands

# get configurations from config file
import config


class Diagnostic(object):
    """An abstract class to represent PALFA diagnostics.
    """
    # Define some class attributes
    description = None
    name = None
    
    def __init__(self, obs_name, beam_id, version_number, directory):
        self.obs_name = obs_name
        self.beam_id = beam_id
        self.version_number = version_number
        self.directory = directory

    def get_diagnostic(self):
        raise NotImplementedError("Method 'get_diagnostic(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)

    def upload(self):
        raise NotImplementedError("Method 'upload(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)


class FloatDiagnostic(Diagnostic):
    """An abstract class to represent float-valued PALFA diagnostics.
    """
    def __init__(self, *args, **kwargs):
        super(FloatDiagnostic, self).__init__(*args, **kwargs)
        self.value = None # The diagnostic value to upload
        self.get_diagnostic()

    def upload(self):
        db.cursor.execute("EXEC spDiagnosticAdder " \
                          "SET obs_name='%s', " % self.obs_name + \
                          "    beam_id=%d, " % self.beam_id + \
                          "    instit='%s', " % config.institution + \
                          "    pipeline='%s', % config.pipeline + \
                          "    version_number='%s', " % self.version_number + \
                          "    diagnostic_type_name='%s', " % self.name + \
                          "    diagnostic_type_description='%s', " % self.description + \
                          "    diagnostic_value=%.12g;" % self.value)


class PlotDiagnostic(Diagnostic):
    """An abstract class to represent binary PALFA diagnostics.
    """
    def __init__(self, *args, **kwargs):
        super(PlotDiagnostic, self).__init__(*args, **kwargs)
        self.value = None # The binary file's name
        self.filedata = None # The binary file's data
        self.get_diagnostic()

    def upload(self):
        db.cursor.execute("EXEC spDiagnosticPlotAdder " \
                          "SET obs_name='%s', " % self.obs_name + \
                          "    beam_id=%d, " % self.beam_id + \
                          "    instit='%s', " % config.institution + \
                          "    pipeline='%s', % config.pipeline + \
                          "    version_number='%s', " % self.version_number + \
                          "    diagnostic_plot_type_name='%s', " % self.name + \
                          "    diagnostic_plot_type_description='%s', " % self.description + \
                          "    filename='%s', " % os.path.split(self.value)[-1] + \
                          "    diagnostic_plot=0x%s;" % self.filedata.encode('hex'))


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
        raise NotImplementedError("Need to cross-check png files with accelcands list.")
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))
        pfdpngs = glob.glob(os.path.join(self.directory, "*.pfd.png"))

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
        self.value = len([c for c in candlist if c.sigma >= config.to_prepfold_sigma])


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


def main():
    # Connect to the database
    global db
    warnings.warn("Connecting to common-copy DB at Cornell for testing...")
    db = database.Database('common-copy')

    # Define a list of diagnostics to apply
    diagnostic_types = [RFIPercentageDiagnostic,
                        RFIPlotDiagnostic,
                        AccelCandsDiagnostic,
                        NumFoldedDiagnostic,
                        NumCandsDiagnostic,
                        MinSigmaFoldedDiagnostic,
                        NumAboveThreshDiagnostic,
                       ]

    # Loop over diagnostics, adding missing values to the DB
    for diagnostic_type in diagnostic_types:
        print "Working on %s" % diagnostic_type.name
        try:
            d = diagnostic_type(options.obsname, options.beamnum, \
                                options.versionnum, options.directory)
        except DiagnosticError, e:
            print "Error caught. %s. Skipping..." % e
        except KeyboardInterrupt:
            print "\nMoving along..."
            break
        else:
            d.upload()
            print "Uploaded (%s)" % d.value

    # Close connection to the database
    db.close()


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
    (options, sys.argv) = parser.parse_args()
    main()
