#!/usr/bin/env python

"""
Data object definitions to represent PALFA data files.

Patrick Lazarus, Jan. 5, 2011
"""

import os.path
import sys
import re
import warnings
import types

import pyfits
import numpy as np

from astro_utils import sextant
from astro_utils import protractor
from astro_utils import calendar
from formats import psrfits

COORDS_TABLE = "/homes/borgii/alfa/svn/workingcopy_PL/PALFA/miscellaneous/" + \
                "PALFA_coords_table.txt"

date_re = re.compile(r'^(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})$')
time_re = re.compile(r'^(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})$')


def autogen_dataobj(fns, verbose=False, *args, **kwargs):
    """Automatically generate a Data object.
        More specifically: Given a list of filenames
        find out which subclass of Data is appropriate
        and instantiate and return the object.
    """
    for objname in globals():
        obj = eval(objname)
        if type(obj)==types.TypeType and issubclass(obj, Data):
            if obj.is_correct_filetype(fns):
                if verbose:
                    print "Using %s" % objname
                data = obj(fns, *args, **kwargs)
                break
    if 'data' not in dir():
        raise ValueError("Cannot determine datafile's type.")
    return data


class Data(object):
    """PALFA Data object. 
        Collects observation information.
    """
    # An impossible to match string:
    # The end-of-line mark is before the start-of-line mark
    # This variable should be overridden by subclasses of Header
    filename_re = re.compile('$x^')

    def __init__(self, fns):
        self.fns = fns
        self.posn_corrected = False # Have RA/Dec been corrected in file header

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
            self.posn_corrected = True
            if self.beam_id % 2:
                # Even beam number. Use columns 2 and 3.
                self.correct_ra, self.correct_decl = matches[0].split()[1:3]
            else:
                self.correct_ra, self.correct_decl = matches[0].split()[3:5]
            self.right_ascension = float(self.correct_ra.replace(':', ''))
            self.declination = float(self.correct_decl.replace(':', ''))
            self.ra_deg = float(protractor.convert(self.correct_ra, 'hmsstr', 'deg')[0])
            self.dec_deg = float(protractor.convert(self.correct_decl, 'dmsstr', 'deg')[0])
            l, b = sextant.equatorial_to_galactic(self.correct_ra, self.correct_decl, \
                                    'sexigesimal', 'deg', J2000=True)
            self.galactic_longitude = float(l[0])
            self.galactic_latitude = float(b[0])
        else:
            raise ValueError("Bad number of matches (%d) in coords table!" % len(matches))

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
        """Check if the Data class accurately describes the
            datafiles listed in filenames.
        """
        result = True
        for fn in filenames:
            if cls.fnmatch(fn) is None:
                result = False
                break
        return result


class PsrfitsData(Data):
    """PSRFITS Data object.
    """
    def __init__(self, fitsfns):
        """PSR fits Header object constructor.
        """
        super(PsrfitsData, self).__init__(fitsfns)
        # Read information from files
        self.specinfo = psrfits.SpectraInfo(self.fns)
        self.original_file = os.path.split(sorted(self.specinfo.filenames)[0])[-1]
        self.project_id = self.specinfo.project_id
        self.observers = self.specinfo.observer
        self.source_name = self.specinfo.source
        self.center_freq = self.specinfo.fctr
        self.num_channels_per_record = self.specinfo.num_channels
        self.channel_bandwidth = self.specinfo.df*1000.0 # In kHz
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

        self.file_size = int(sum([os.path.getsize(fn) for fn in fitsfns]))
        self.observation_time = self.specinfo.T
        self.num_samples = self.specinfo.N
        self.data_size = self.num_samples * \
                            self.specinfo.bits_per_sample/8.0 * \
                            self.num_channels_per_record
        self.num_samples_per_record = self.specinfo.spectra_per_subint


class WappPsrfitsData(PsrfitsData):
    """PSRFITS Data object for WAPP data.
    """
    filename_re = re.compile(r'^(?P<projid>[Pp]\d{4})_(?P<mjd>\d{5})_' \
                                r'(?P<sec>\d{5})_(?P<scan>\d{4})_' \
                                r'(?P<source>.*)_(?P<beam>\d)\.w4bit\.fits$')

    def __init__(self, fitsfns):
        super(WappPsrfitsData, self).__init__(fitsfns)
        self.beam_id = self.specinfo.beam_id
        if self.beam_id is None:
            raise ValueError("Beam number not encoded in PSR fits header.")
        self.get_correct_positions()
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

    def update_positions(self):
        """Update positions in raw data file's header.
            
            Note: This cannot be undone!
        """
        if self.posn_corrected:
            for fn in self.fns:
                hdus = pyfits.open(fn, mode='update')
                primary = hdus['PRIMARY'].header
                primary['RA'] = self.correct_ra 
                primary['DEC'] = self.correct_decl
                hdus.close() # hdus are updated at close-time


class MockPsrfitsData(PsrfitsData):
    """PSR fits Data object for MockSpec data.
    """
    filename_re = re.compile(r'^4bit-(?P<projid>[Pp]\d{4})\.(?P<date>\d{8})\.' \
                                r'(?P<source>.*)\.b(?P<beam>[0-7])' \
                                r's(?P<subband>[01])g0.(?P<scan>\d{5})\.fits')

    def __init__(self, fitsfns):
        super(MockPsrfitsData, self).__init__(fitsfns)
        self.beam_id = self.specinfo.beam_id
        if self.beam_id is None:
            raise ValueError("Beam number not encoded in PSR fits header.")
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


class MergedMockPsrfitsData(PsrfitsData):
    """PSRFITS Data object for merged MockSpec data.
    """
    filename_re = re.compile(r'^4bit-(?P<projid>[Pp]\d{4})\.(?P<date>\d{8})\.' \
                                r'(?P<source>.*)\.b(?P<beam>[0-7])' \
                                r'g0\.merged\.(?P<scan>\d{5})_(?P<filenum>\d{4})' \
                                r'\.fits')

    def __init__(self, fitsfns):
        super(MergedMockPsrfitsData, self).__init__(fitsfns)
        # Note Puerto Rico doesn't observe daylight savings time
        # so it is 4 hours behind UTC all year
        dayfrac = calendar.MJD_to_date(self.timestamp_mjd)[-1]%1
        self.start_ast = int((dayfrac*24-4)*3600)
        self.start_ast %= 24*3600
        self.num_ifs = 2
        # Parse filename to get the scan number
        m = self.fnmatch(fitsfns[0])
        self.beam_id = int(m.groupdict()['beam'])
        self.get_correct_positions()
        self.scan_num = m.groupdict()['scan']
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    str(self.scan_num)])



def main():
    data = autogen_dataobj(sys.argv[1:])
    print data.__dict__


if __name__=='__main__':
    main()
