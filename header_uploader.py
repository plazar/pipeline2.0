#!/usr/bin/env python

"""
Script to upload header information to commonDB given a WAPP file.

Argument 1: WAPP file name
Argument 2: PALFA beam number (0-7; beam 7 is a copy of beam 6)
Other arguments are ignored.

*** Values are different on different machines (32/64-bit issue?) ***
    *** Needs investigation! ***

Patrick Lazarus, Sept. 10, 2010
"""

import os.path
import sys
import re

import numpy as np
import database
from pypulsar.utils.astro import sextant
from pypulsar.utils.astro import protractor
from pypulsar.utils.astro import calendar
from pypulsar.formats import wapp

COORDS_TABLE = "/homes/borgii/alfa/svn/workingcopy_PL/PALFA/miscellaneous/" + \
                "PALFA_coords_table.txt"

date_re = re.compile(r'^(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})$')
time_re = re.compile(r'^(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})$')

class Header:
    """PALFA Header object. 
        Defines observation information relevant to the common DB.
    """
    def __init__(self, wappfns, beamnum):
        """Header object constructor.
        """
        # Open wapp files, sort by offset since start of observation
        cmp_offset = lambda w1,w2: cmp(w1.header['timeoff'], w2.header['timeoff'])
        self.wapps = sorted([wapp.wapp(fn) for fn in wappfns], cmp=cmp_offset)
        w0 = self.wapps[0]
        
        # Check WAPP files are from the same observation
        if False in [w0.header['src_name'] == w.header['src_name'] \
                        for w in self.wapps]:
            raise ValueError("Source name is not consistent in all files.")
        if False in [w0.header['obs_date'] == w.header['obs_date'] \
                        for w in self.wapps]:
            raise ValueError("Observation date is not consistent in all files.")
        if False in [w0.header['start_time'] == w.header['start_time'] \
                        for w in self.wapps]:
            raise ValueError("Start time is not consistent in all files.")
        # Divide number of samples by 2 because beams are multiplexed
        # First entry is 0 because first file is start of observation
        sampoffset = np.cumsum([0]+[w.number_of_samples/2 for w in self.wapps])
        if False in [w.header['timeoff']==samps for (w,samps) in \
                        zip(self.wapps, sampoffset)]:
            raise ValueError("Offset since start of observation not consistent.")
        
        self.original_wapp_file = w0.filename
        self.beam_id = beamnum
        self.project_id = w0.header['project_id']
        self.observers = w0.header['observers']
        self.start_ast = w0.header['start_ast']
        self.start_lst = w0.header['start_lst']
        self.source_name = w0.header['src_name']
        self.center_freq = w0.header['cent_freq']
        self.num_channels_per_record = w0.header['num_lags']
        # ALFA band is inverted
        self.channel_bandwidth = -abs(w0.header['bandwidth'] / \
                                    float(self.num_channels_per_record))
        self.num_ifs = w0.header['nifs']
        self.sample_time = w0.header['samp_time'] # in micro seconds
        self.sum_id = w0.header['sum']
        
        # Compute timestamp_mjd
        date = date_re.match(w0.header['obs_date']).groupdict()
        time = time_re.match(w0.header['start_time']).groupdict()
        dayfrac = (int(time['hour']) + \
                    (int(time['min']) + \
                    (int(time['sec']) / 60.0)) / 60.0) / 24.0
        day = calendar.date_to_MJD(int(date['year']), int(date['month']), \
                                    int(date['day']))
        self.timestamp_mjd = day + dayfrac

        # Combine obs_name
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    self.original_wapp_file.split('.')[-1]])
        
        # Get beam positions 
        self.beam_id = beamnum
        if beamnum == 7:
            b = 6
        else:
            b = beamnum
        self.orig_start_az = w0.header['alfa_az'][b]
        if w0.header['start_az'] > 360.0 and self.orig_start_az < 360.0:
            self.orig_start_az += 360.0
        self.orig_start_za = w0.header['alfa_za'][b]
        self.orig_ra_deg = w0.header['alfa_raj'][b]*15.0
        self.orig_dec_deg = w0.header['alfa_decj'][b]
        self.orig_right_ascension = float(protractor.convert(self.orig_ra_deg, \
                                        'deg', 'hmsstr')[0].replace(':', ''))
        self.orig_declination = float(protractor.convert(self.orig_dec_deg, \
                                        'deg', 'dmsstr')[0].replace(':', ''))
        l, b = sextant.equatorial_to_galactic(self.orig_ra_deg, self.orig_dec_deg, \
                                            'deg', 'deg', J2000=True)
        self.orig_galactic_longitude = float(l)
        self.orig_galactic_latitude = float(b)
        # Get corrected beam positions
        matches = [line for line in open(COORDS_TABLE, 'r') if \
                        line.startswith(w0.filename)]
        if len(matches) == 0 and self.timestamp_mjd > 54651:
            # No corrected coords found, but coordinate problem is fixed,
            # so use header values.
            # MJD=54651 is July 4th 2008, it is a recent date by which
            # the coord problem is definitely corrected. (The problem
            # was discovered and fixed in ~2005).
            self.right_ascension = self.orig_right_ascension
            self.declination = self.orig_declination
            self.ra_deg = self.orig_ra_deg
            self.dec_deg = self.orig_dec_deg
            self.galactic_longitude = self.orig_galactic_longitude
            self.galactic_latitude = self.orig_galactic_latitude
        elif len(matches) == 1:
            # Use values from coords table
            if beamnum % 2:
                # Even beam number. Use columns 2 and 3.
                ra, decl = matches[0].split()[1:3]
            else:
                ra, decl = matches[0].split()[3:5]
            self.right_ascension = float(ra.replace(':', ''))
            self.declination = float(decl.replace(':', ''))
            self.ra_deg = float(protractor.convert(ra, 'hmsstr', 'deg')[0])
            self.dec_deg = float(protractor.convert(decl, 'dmsstr', 'deg')[0])
            l, b = sextant.equatorial_to_galactic(ra, decl, \
                                    'sexigesimal', 'deg', J2000=True)
            self.galactic_longitude = float(l[0])
            self.galactic_latitude = float(b[0])
        else:
            raise ValueError("Bad number of matches (%d) in coords table!" % len(matches))

        # Multiple files
        # Factors of 2 is because two beams are multiplexed
        self.data_size = int(sum([w.data_size/2.0 for w in self.wapps]))
        self.file_size = int(sum([w.file_size for w in self.wapps]))
        self.observation_time = sum([w.obs_time for w in self.wapps])
        self.num_samples = sum([w.number_of_samples for w in self.wapps])

        # Still not sure exactly what self.num_samples_per_record is supposed to be
        self.num_samples_per_record = self.num_samples

    def get_upload_sproc_call(self):
        """Return the EXEC spHeaderLoader string to upload
            this header to the PALFA common DB.
        """
        sprocstr = "EXEC spHeaderLoader " + \
            "@obs_name='%s', " % self.obs_name + \
            "@beam_id=%d, " % self.beam_id + \
            "@original_wapp_file='%s', " % self.original_wapp_file + \
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
            "@right_ascention=%.4f, " % self.right_ascension + \
            "@declination=%.4f, " % self.declination + \
            "@galactic_longitude=%.8f, " % self.galactic_longitude + \
            "@galactic_latitude=%.8f, " % self.galactic_latitude + \
            "@ra_deg=%.8f, " % self.ra_deg + \
            "@dec_deg=%.8f" % self.dec_deg
        return sprocstr


def print_usage():
    print "header_uploader.py beam_num WAPP_file1 [WAPP_file2 ...]"

if __name__=='__main__':
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(1)
    beamnum = int(sys.argv[1])
    wappfns = sys.argv[2:]
    header = Header(wappfns, beamnum)

    # Get query to upload
    query = header.get_upload_sproc_call()
   
    ### FOR TESTING
    print query
    sys.stderr.write("EXITING BEFORE ACCESSING DATABASE (for testing purposes).\n")
    sys.exit(2)
    ###############

    # Connect to DB
    db = database.Database('common')
    db.cursor.execute(query)
    db.cursor.commit()
    
    # Check to see if upload worked
    result = db.cursor.fetchone()
    if result < 0:
        print "An error was encountered! (Error code: %d)" % result
        sys.exit(1)
    else:
        print "Success! (Return value: %d)" % result
