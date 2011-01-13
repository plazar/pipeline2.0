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
import atexit
import optparse

import database
import datafile

# A global dictionary to keep track of database connections
db_connections = {}


@atexit.register # register this function to be executed at exit time
def close_db_connections():
    """A function to close database connections at exit time.
    """
    for db in db_connections.values():
        db.close()


class Header(object):
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
    
    def __str__(self):
        s = self.get_upload_sproc_call()
        return s.replace('@', '\n    @')
   
    def upload(self, dbname='common', verbose=False):
        if dbname not in db_connections:
            db_connections[dbname] = database.Database(dbname)
        db = db_connections[dbname]
        query = self.get_upload_sproc_call()
        db.cursor.execute(query)
        result = db.cursor.fetchone()[0]
        return result


class HeaderUploadError(Exception):
    """Error to throw when a diagnostic-specific problem 
        is encountered.
    """
    pass


def upload_header(fns, beamnum=None, verbose=False):
    if beamnum is not None:
        header = Header(fns, beamnum=beamnum)
    else:
        header = Header(fns)
    # header.upload('common', verbose=verbose)
    warnings.warn("Database is set to 'common-copy' for debugging.")
    result = header.upload('common-copy')

    if verbose:
        # Check to see if upload worked
        if result < 0:
            print "An error was encountered! (Error code: %d)" % result
        else:
            print "Success! (Return value: %d)" % result
    return result


def main():
    upload_header(args, options.beamnum, options.verbose)
    

if __name__=='__main__':
    parser = optparse.OptionParser(usage="%(prog) [OPTIONS] file1 [file2 ...]")
    parser.add_option('-b', '--beamnum', dest='beamnum', type='int', \
                        help="Beam number is required for mulitplexed WAPP " \
                             "data. Beam number must be between 0 and 7, " \
                             "inclusive.", \
                        default=None)
    parser.add_option('--verbose', dest='verbose', action='store_true', \
                        help="Print success/failure information to screen. " \
                             "(Default: do not print).", \
                        default=False)
    options, args = parser.parse_args()
    if options.beamnum is not None:
        if not 0 <= options.beamnum <= 7:
            raise ValueError("Beam number must be between 0 and 7, inclusive!")
    main()
