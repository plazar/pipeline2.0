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
import time

import debug
import upload
import datafile
import database
import pipeline_utils

# Raise warnings produced by invalid coord strings as exceptions
warnings.filterwarnings("error", message="Input is not a valid sexigesimal string: .*")

class Header(upload.Uploadable):
    """PALFA Header object. 
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'obs_name': '%s', \
              'beam_id': '%d', \
              'original_file': '%s', \
              'sample_time': '%f', \
              'observation_time': '%f', \
              'timestamp_mjd': '%.10f', \
              'num_samples_per_record': '%d', \
              'center_freq': '%f', \
              'channel_bandwidth': '%f', \
              'num_channels_per_record': '%d', \
              'num_ifs': '%d', \
              'orig_right_ascension': '%.4f', \
              'orig_declination': '%.4f', \
              'orig_galactic_longitude': '%.8f', \
              'orig_galactic_latitude': '%.8f', \
              'source_name': '%s', \
              'start_ast': '%.8f', \
              'start_lst': '%.8f', \
              'project_id': '%s', \
              'observers': '%s', \
              'file_size': '%d', \
              'data_size': '%d', \
              'num_samples': '%d', \
              'orig_ra_deg': '%.8f', \
              'orig_dec_deg': '%.8f', \
              'right_ascension': '%.4f', \
              'declination': '%.4f', \
              'galactic_longitude': '%.8f', \
              'galactic_latitude': '%.8f', \
              'ra_deg': '%.8f', \
              'dec_deg': '%.8f', \
              'obstype': '%s'}
    
    def __init__(self, datafns, *args, **kwargs):
        if isinstance(datafns, datafile.Data):
            self.data = datafns
        else:
            self.data = datafile.autogen_dataobj(datafns, *args, **kwargs)
       

        # List of dependents (ie other uploadables that require 
        # the header_id from this header)
        self.dependents = []

    def add_dependent(self, dep):
        self.dependents.append(dep)
    
    def upload(self, dbname, *args, **kwargs):
        """An extension to the inherited 'upload' method.
            This method will make sure any dependents have
            the header_id and then upload them.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if debug.UPLOAD: 
            starttime = time.time()
        header_id = super(Header, self).upload(dbname=dbname, *args, **kwargs)[0]
        
        self.compare_with_db(dbname=dbname)
 
        if debug.UPLOAD:
            upload.upload_timing_summary['header'] = \
                upload.upload_timing_summary.setdefault('header', 0) + \
                (time.time()-starttime)
        
        for dep in self.dependents:
            dep.header_id = header_id
            dep.upload(dbname=dbname, *args, **kwargs)
        return header_id

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
            "@timestamp_mjd=%.10f, " % self.timestamp_mjd + \
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
            "@dec_deg=%.8f, " % self.dec_deg + \
            "@obsType='%s'" % self.obstype
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding header from DB and compare values.
            Raise a HeaderError if any mismatch is found.

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
                          "h.original_wapp_file AS original_file, " \
                          "h.sample_time, " \
                          "h.observation_time, " \
                          "h.timestamp_mjd, " \
                          "h.num_samples_per_record, " \
                          "h.center_freq, " \
                          "h.channel_bandwidth, " \
                          "h.num_channels_per_record, " \
                          "h.num_ifs, " \
                          "h.orig_right_ascension, " \
                          "h.orig_declination, " \
                          "h.orig_galactic_longitude, " \
                          "h.orig_galactic_latitude, " \
                          "h.source_name, " \
                          "h.sum_id, " \
                          "h.orig_start_az, " \
                          "h.orig_start_za, " \
                          "h.start_ast, " \
                          "h.start_lst, " \
                          "h.project_id, " \
                          "h.observers, " \
                          "h.file_size, " \
                          "h.data_size, " \
                          "h.num_samples, " \
                          "h.orig_ra_deg, " \
                          "h.orig_dec_deg, " \
                          "h.right_ascension, " \
                          "h.declination, " \
                          "h.galactic_longitude, " \
                          "h.galactic_latitude, " \
                          "h.ra_deg, " \
                          "h.dec_deg, " \
                          "h.obsType  AS obstype " \
                   "FROM headers AS h " \
                   "LEFT JOIN observations AS obs ON obs.obs_id=h.obs_id " \
                   "WHERE obs.obs_name='%s' AND h.beam_id=%d " % \
                        (self.obs_name, self.beam_id))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(obs_name: %s, beam_id: %d)" % \
                                (self.obs_name, self.beam_id))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(obs_name: %s, beam_id: %d)" % \
                                (self.obs_name, self.beam_id))
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
                errormsg = "Header doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise HeaderError(errormsg)


class HeaderError(upload.UploadNonFatalError):
    """Error to throw when a header-specific problem is encountered.
    """
    pass


def get_header(fns, beamnum=None):
    """Get header.

        Inputs:
            fns: list of filenames (include paths) of data to parse.
            beamnum: ALFA beam number (an integer between 0 and 7).
                        This is only required for multiplexed WAPP data files.

        Output:
            header: The header object.
    """
    if beamnum is not None:
        if not 0 <= beamnum <= 7:
            raise HeaderError("Beam number must be between 0 and 7, inclusive!")
        try:
            header = Header(fns, beamnum=beamnum)
        except Exception:
            raise HeaderError("Couldn't create Header object for files (%s)!" % \
                                    fns) 
    else:
        try:
            header = Header(fns)
        except Exception:
            raise HeaderError("Couldn't create Header object for files (%s)!" % \
                                    fns) 
    return header


def main():
    db = database.Database('default', autocommit=False)
    try:
        header = get_header(args, options.beamnum)
        header.upload(db)
    except:
        print "Rolling back..."
        db.rollback()
        raise
    else:
        db.commit()
    finally:
        db.close()


if __name__=='__main__':
    parser = optparse.OptionParser(usage="%prog [OPTIONS] file1 [file2 ...]")
    parser.add_option('-b', '--beamnum', dest='beamnum', type='int', \
                        help="Beam number is required for mulitplexed WAPP " \
                             "data. Beam number must be between 0 and 7, " \
                             "inclusive.", \
                        default=None)
    options, args = parser.parse_args()
    main()
