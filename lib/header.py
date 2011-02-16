#!/usr/bin/env python

"""
Header object for uploading datafile information to commonDB from 
    a given PALFA data file.

Patrick Lazarus, Jan. 5, 2011
"""

import os.path
import sys
import re
import warnings
import types
import optparse

import upload
import datafile

# Raise warnings produced by invalid coord strings as exceptions
warnings.filterwarnings("error", message="Input is not a valid sexigesimal string: .*")

class Header(upload.Uploadable):
    """PALFA Header object. 
    """
    def __init__(self, datafns, *args, **kwargs):
        if isinstance(datafns, datafile.Data):
            self.data = datafns
        else:
            self.data = datafile.autogen_dataobj(datafns, *args, **kwargs)

    def __getattr__(self, key):
        # Allow Header object to return Data object's attributes
        return getattr(self.data, key)

    def get_upload_sproc_call(self):
        """Return the EXEC spHeaderLoader string to upload
            this header to the PALFA common DB.
        """
        sprocstr = "EXEC spHeaderLoader " + \
            "@obs_name='%s', " % self.obs_name + \
            "@beam_id=%d, " % self.beam_id + \
            "@original_wapp_file='%s', " % self.original_file + \
            "@sample_time=%f, " % self.sample_time + \
            "@observation_time=%f, " % self.observation_time + \
            "@timestamp_mjd=%.15f, " % self.timestamp_mjd + \
            "@num_samples_per_record=%d, " % self.num_samples_per_record + \
            "@center_freq=%f, " % self.center_freq + \
            "@channel_bandwidth=%f, " % self.channel_bandwidth + \
            "@num_channels_per_record=%d, " % self.num_channels_per_record + \
            "@num_ifs=%d, " % self.num_ifs + \
            "@orig_right_ascension=%.4f, " % self.orig_right_ascension + \
            "@orig_declination=%.4f, " % self.orig_declination + \
            "@orig_galactic_longitude=%.8f, " % self.orig_galactic_longitude + \
            "@orig_galactic_latitude=%.8f, " % self.orig_galactic_latitude + \
            "@source_name='%s', " % self.source_name + \
            "@sum_id=%d, " % self.sum_id + \
            "@orig_start_az=%.4f, " % self.orig_start_az + \
            "@orig_start_za=%.4f, " % self.orig_start_za + \
            "@start_ast=%.8f, " % self.start_ast + \
            "@start_lst=%.8f, " % self.start_lst + \
            "@project_id='%s', " % self.project_id + \
            "@observers='%s', " % self.observers + \
            "@file_size=%d, " % self.file_size + \
            "@data_size=%d, " % self.data_size + \
            "@num_samples=%d, " % self.num_samples + \
            "@orig_ra_deg=%.8f, " % self.orig_ra_deg + \
            "@orig_dec_deg=%.8f, " % self.orig_dec_deg + \
            "@right_ascension=%.4f, " % self.right_ascension + \
            "@declination=%.4f, " % self.declination + \
            "@galactic_longitude=%.8f, " % self.galactic_longitude + \
            "@galactic_latitude=%.8f, " % self.galactic_latitude + \
            "@ra_deg=%.8f, " % self.ra_deg + \
            "@dec_deg=%.8f" % self.dec_deg
        return sprocstr
    

class HeaderError(Exception):
    """Error to throw when a header-specific problem is encountered.
    """
    pass


def upload_header(fns, beamnum=None, verbose=False, dry_run=False):
    """Upload header to commonDB.

        Inputs:
            fns: list of filenames (include paths) of data to parse.
            beamnum: ALFA beam number (an integer between 0 and 7).
                        This is only required for multiplexed WAPP data files.
            verbose: An optional boolean value that determines if information 
                        is printed to stdout. (Default: don't print to stdout).
            dry_run: An optional boolean value. If True no connection to DB
                        will be made and DB command will not be executed.
                        (If verbose is True DB command will be be printed 
                        to stdout.)
        Output:
            header_id: The header ID corresponding to this beam's entry
                        in the common DB. (Or None if dry_run is True).
    """
    if beamnum is not None:
        if not 0 <= beamnum <= 7:
            raise HeaderError("Beam number must be between 0 and 7, inclusive!")
        header = Header(fns, beamnum=beamnum)
    else:
        header = Header(fns)
    # header.upload('common', verbose=verbose)
    if dry_run:
        header.get_upload_sproc_call()
        if verbose:
            print header
        result = None
    else:
        result = header.upload()
        if result < 0:
            raise HeaderError("An error was encountered! " \
                                "(Error code: %d)" % result)

        if verbose:
            print "Success! (Return value: %d)" % result
    return result


def main():
    try:
        upload_header(args, options.beamnum, options.verbose, options.dry_run)
    except upload.UploadError, e:
        traceback.print_exception(*sys.exc_info())
        sys.stderr.write("\nOriginal exception thrown:\n")
        traceback.print_exception(*e.orig_exc)
    

if __name__=='__main__':
    parser = optparse.OptionParser(usage="%prog [OPTIONS] file1 [file2 ...]")
    parser.add_option('-b', '--beamnum', dest='beamnum', type='int', \
                        help="Beam number is required for mulitplexed WAPP " \
                             "data. Beam number must be between 0 and 7, " \
                             "inclusive.", \
                        default=None)
    parser.add_option('--verbose', dest='verbose', action='store_true', \
                        help="Print success/failure information to screen. " \
                             "(Default: do not print).", \
                        default=False)
    parser.add_option('-n', '--dry-run', dest='dry_run', action='store_true', \
                        help="Perform a dry run. Do everything but connect to " \
                             "DB and upload header info. If --verbose " \
                             "is set, DB commands will be displayed on stdout. " \
                             "(Default: Connect to DB and execute commands).", \
                        default=False)
    options, args = parser.parse_args()
    main()
