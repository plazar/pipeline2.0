#!/usr/bin/env python

"""
Script to upload header information to commonDB given a PALFA data file.

Argument 1: PALFA beam number (0-7; beam 7 is a copy of beam 6)
Other arguments: data file names

*** Values are different on different machines (32/64-bit issue?) ***
    *** Needs investigation! ***

Patrick Lazarus, Sept. 10, 2010
"""

import os.path
import sys
import re
import warnings
import types

import numpy as np
#import database
from astro_utils import sextant
from astro_utils import protractor
from astro_utils import calendar
from formats import wapp
from formats import psrfits

COORDS_TABLE = "/homes/borgii/alfa/svn/workingcopy_PL/PALFA/miscellaneous/" + \
                "PALFA_coords_table.txt"

date_re = re.compile(r'^(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})$')
time_re = re.compile(r'^(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})$')

class Header(object):
    """PALFA Header object. 
        Defines observation information relevant to the common DB.
    """
    # An impossible to match string:
    # The end-of-line mark is before the start-of-line mark
    # This variable should be overridden by subclasses of Header
    filename_re = re.compile('$x^')

    def __init__(self):
        raise NotImplementedError("Constructor not implemented for abstract class Header.")

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
    
    def get_correct_positions(self):
        """Reconstruct original wapp filename and check
            for correct beam positions from the coordinates
            table.

            Returns nothing, updates object in place.
        """
        wappfn = '.'.join([self.project_id, self.source_name, \
                            "wapp%d" % (self.beam_id/2+1), \
                            "%5d" % int(self.timestamp_mjd), \
                            self.fnmatch(self.original_file).groupdict()['scan']])
        # Get corrected beam positions
        matches = [line for line in open(COORDS_TABLE, 'r') if \
                        line.startswith(wappfn)]
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

    def __str__(self):
        s = self.get_upload_sproc_call()
        return s.replace('@', '\n    @')
    
    # These are class methods.
    # They don't need to be called with an instance.
    @classmethod
    def fnmatch(cls, filename):
        """Match filename with regular expression.
        """
        fn = os.path.split(filename)[-1]
        return cls.filename_re.match(fn)
    
    @classmethod
    def is_correct_filetype(cls, filenames):
        """Check if the header class accurately describes the data
            in the files listed in filenames.
        """
        result = True
        for fn in filenames:
            if cls.fnmatch(fn) is None:
                result = False
                break
        return result

    @classmethod
    def autogen_header(cls, fns, beamnum):
        """Automatically generate a Header object.
            More specifically: Given a list of filenames
            find out which subclass of Header is appropriate
            and instantiate and return the object.
        """

        for objname in globals():
            obj = eval(objname)
            if type(obj)==types.TypeType and issubclass(obj, Header):
                if obj.is_correct_filetype(fns):
                    print "Using %s" % objname
                    header = obj(fns, beamnum)
                    break
        if 'header' not in dir():
            raise ValueError("Cannot determine datafile's type.")
        return header


class WappHeader(Header):
    """PALFA WAPP Header object.
    """
    def __init__(self, wappfns, beamnum):
        """WAPP Header object constructor.
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
        
        self.original_file = os.path.split(w0.filename)[-1]
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
        scan = self.fnmatch(self.original_file).groupdict()['scan']
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    scan])
        
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
        self.get_correct_positions()

class MultiplexedWappHeader(WappHeader):
    """WAPP Headers of multiplexed PALFA data.
    """
    filename_re = re.compile(r'^(?P<projid>[Pp]\d{4})\.(?P<source>.*)\.' \
                                r'wapp(?P<wapp>\d)\.(?P<mjd>\d{5})\.' \
                                r'(?P<scan>\d{4})$')

    def __init__(self, wappfns, beamnum):
        """Constructor for MultiplexedWappHeader objects.
        """
        super(MultiplexedWappHeader, self).__init__(wappfns, beamnum)
        # Multiple files
        # Factors of 2 is because two beams are multiplexed
        self.data_size = int(sum([w.data_size/2.0 for w in self.wapps]))
        self.file_size = int(sum([w.file_size for w in self.wapps]))
        self.observation_time = sum([w.obs_time/2.0 for w in self.wapps])
        self.num_samples = sum([w.number_of_samples/2.0 for w in self.wapps])
        # Still not sure exactly what self.num_samples_per_record is supposed to be
        self.num_samples_per_record = self.num_samples


class DumpOfWappHeader(WappHeader):
    """Dump of PALFA WAPP Headers.
        These dumps are produced when converting from WAPP to PSR fits.
    """
    filename_re = re.compile(r'^(?P<projid>[Pp]\d{4})_(?P<mjd>\d{5})_' \
                                r'(?P<sec>\d{5})_(?P<scan>\d{4})_' \
                                r'(?P<source>.*)_(?P<beam>\d)\.w4bit\.wapp_hdr$')

    def __init__(self, fns, beamnum):
        """Dumpy of PALFA WAPP Header constructor.
        """
        super(DumpOfWappHeader, self).__init__(fns, beamnum)
        # The file provided has no data, thus we cannot determine sizes
        self.data_size = -1
        self.file_size = -1

        self.observation_time = self.wapps[0].header['obs_time']
        self.num_samples = self.observation_time/(self.sample_time*1e-6)
        # Still not sure exactly what self.num_samples_per_record is supposed to be
        self.num_samples_per_record = self.num_samples


class PsrfitsHeader(Header):
    """PSR fits Header object.
    """
    def __init__(self, fitsfns, beamnum):
        """PSR fits Header object constructor.
        """
        # Read information from files
        self.specinfo = psrfits.SpectraInfo(fitsfns)
        self.original_file = os.path.split(sorted(self.specinfo.filenames)[0])[-1]
        self.beam_id = self.specinfo.beam_id
        if self.beam_id is None:
            raise ValueError("Beam number not encoded in PSR fits header.")
        self.project_id = self.specinfo.project_id
        self.observers = self.specinfo.observer
        self.source_name = self.specinfo.source
        self.center_freq = self.specinfo.fctr
        self.num_channels_per_record = self.specinfo.num_channels
        self.channel_bandwidth = self.specinfo.df
        self.sample_time = self.specinfo.dt*1e6 # In microseconds
        self.sum_id = int(self.specinfo.summed_polns)
        self.timestamp_mjd = self.specinfo.start_MJD[0]
        self.start_lst = self.specinfo.start_lst 
        self.orig_start_az = self.specinfo.azimuth
        self.orig_start_za = self.specinfo.zenith_ang
        self.orig_ra_deg = self.specinfo.ra2000
        self.orig_dec_deg = self.specinfo.dec2000
        self.orig_right_ascension = float(protractor.convert(self.orig_ra_deg, \
                                        'deg', 'hmsstr')[0].replace(':', ''))
        self.orig_declination = float(protractor.convert(self.orig_dec_deg, \
                                        'deg', 'dmsstr')[0].replace(':', ''))
        l, b = sextant.equatorial_to_galactic(self.orig_ra_deg, self.orig_dec_deg, \
                                            'deg', 'deg', J2000=True)
        self.orig_galactic_longitude = float(l)
        self.orig_galactic_latitude = float(b)
        self.get_correct_positions()

        self.file_size = int(sum([os.path.getsize(fn) for fn in fitsfns]))
        self.observation_time = self.specinfo.T
        self.num_samples = self.specinfo.N
        self.data_size = self.num_samples * \
                            self.specinfo.bits_per_sample/8.0 * \
                            self.num_channels_per_record
        self.num_samples_per_record = self.specinfo.spectra_per_subint


class WappPsrfitsHeader(PsrfitsHeader):
    """PSR fits Header object for WAPP data.
    """
    filename_re = re.compile(r'^(?P<projid>[Pp]\d{4})_(?P<mjd>\d{5})_' \
                                r'(?P<sec>\d{5})_(?P<scan>\d{4})_' \
                                r'(?P<source>.*)_(?P<beam>\d)\.w4bit\.fits$')

    def __init__(self, fitsfns, beamnum):
        super(WappPsrfitsHeader, self).__init__(fitsfns, beamnum)
        # Note Puerto Rico doesn't observe daylight savings time
        # so it is 4 hours behind UTC all year
        dayfrac = calendar.MJD_to_date(self.timestamp_mjd)[-1]%1
        self.start_ast = int((dayfrac*24-4)*3600)
        self.start_ast %= 24*3600
        self.num_ifs = 1 # Hardcoded as 1
        # Parse filename to get the scan number
        m = self.fnmatch(fitsfns[0])
        self.scan_num = m.groupdict()['scan']
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    str(self.scan_num)])


class MockPsrfitsHeader(PsrfitsHeader):
    """PSR fits Header object for MockSpec data.
    """
    filename_re = re.compile(r'^(?P<projid>[Pp]\d{4})\.(?P<date>\d{8})\.' \
                                r'(?P<source>.*)\.b(?P<beam>[0-7])' \
                                r's(?P<subband>[01])g0_4b.(?P<scan>\d{5})\.' \
                                r'(?P=scan)\.fits')

    def __init__(self, fitsfns, beamnum):
        super(MockPsrfitsHeader, self).__init__(fitsfns, beamnum)
        # Note Puerto Rico doesn't observe daylight savings time
        # so it is 4 hours behind UTC all year
        dayfrac = calendar.MJD_to_date(self.timestamp_mjd)[-1]%1
        self.start_ast = int((dayfrac*24-4)*3600)
        self.start_ast %= 24*3600
        self.num_ifs = self.specinfo.hdus[1].header['NUMIFS']
        # Parse filename to get the scan number
        m = self.fnmatch(fitsfns[0])
        self.scan_num = m.groupdict()['scan']
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    str(self.scan_num)])



def print_usage():
    print "header_uploader.py beam_num file1 [file2 ...]"


if __name__=='__main__':
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(1)
    beamnum = int(sys.argv[1])
    fns = sys.argv[2:]
    header = Header.autogen_header(fns, beamnum)

    # Get query to upload
    query = header.get_upload_sproc_call()
   
    ### FOR TESTING
    print header
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

    db.close()
