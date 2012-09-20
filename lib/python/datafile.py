#!/usr/bin/env python

"""
Data object definitions to represent PALFA data files.

Patrick Lazarus, Jan. 5, 2011
"""
import os
import os.path
import sys
import re
import warnings
import types

import numpy as np

# Import PSRFITS-specific modules

from astro_utils import sextant
from astro_utils import protractor
from astro_utils import calendar
import pipeline_utils

date_re = re.compile(r'^(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})$')
time_re = re.compile(r'^(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})$')


def autogen_dataobj(fns, verbose=False, *args, **kwargs):
    """Automatically generate a Data object.
        More specifically: Given a list of filenames
        find out which subclass of Data is appropriate
        and instantiate and return the object.
    """
    datafile_type = get_datafile_type(fns)
    if verbose:
        print "Using %s" % datafile_type.__name__
    data = datafile_type(fns, *args, **kwargs)
    return data


def get_datafile_type(fns):
    """Find the type of data file corresponds to the given file names.

        Input:
            fns: A list of file names.

        Output:
            datafile_type: The subclass of Data that corresponds to 'fns'.
    """
    datafile_type = None
    for objname in globals():
        obj = eval(objname)
        if type(obj)==types.TypeType and issubclass(obj, Data):
            if obj.is_correct_filetype(fns):
                datafile_type = obj
                break
    if datafile_type is None:
        raise DataFileError("Cannot determine datafile's type (%s)." % fns)
    return datafile_type


def are_grouped(fn1, fn2):
    """Return True if file names fn1 and fn2 represent data files
        that should grouped (ie belong to the same observation).
        Returns False if files are of different types or don't
        belong together.

        Note: fn1 and fn2 cannot have the same file name (path is ignored).

        Inputs:
            fn1: A data file name.
            fn2: A data file name.

        Output:
            grpd: Boolean value. True if files belong together.
    """
    datatype1 = get_datafile_type([fn1])
    datatype2 = get_datafile_type([fn2])
    grpd = False
    if (os.path.split(fn1)[-1] != os.path.split(fn2)[-1]) and \
            (datatype1 == datatype2):
        grpd = datatype1.are_grouped(fn1, fn2)
    return grpd


def is_complete(fns):
    """Return True if the list of file names, 'fns' is complete.
        
        Inputs:
            fns: A list of file names.

        Output:
            complete: Boolean value. True if list of file names
                is a group that is complete.
    """
    """
    if not fns:
        return False
    datatypes = [get_datafile_type([fn]) for fn in fns]
    for t in datatypes[1:]:
        if datatypes[0] != t:
            return False
    return datatypes[0].is_complete(fns)
    """
    if len(fns)==8:
        return True
    else:
        return False


def group_files(fns):
    """Given a list of file names form groups of files
        that belong to the same observation.

        Intput:
            fns: A list of file names.

        Output:
            groups: A list of groups (each group is a list of filenames).
    """
    groups = []
    for ii, fn in enumerate(fns):
        group = [fn]
        for jj in range(len(fns)-1, ii, -1):
            other = fns[jj]
            if are_grouped(fn, other):
                group.append(fns.pop(jj))
        groups.append(group)
    return groups

def match_observation(fn1, fn2):
    sfn1 = os.path.split(fn1)[-1]
    sfn2 = os.path.split(fn2)[-1]

    try:
        s1 = '_'.join(sfn1.split('_')[0:4])
        s2 = '_'.join(sfn2.split('_')[0:4])
    except:
        return False
    if s1==s2:
        return True
    else:
        return False


def simple_group_files(fns):
    """Given a list of file names form groups of files
        that belong to the same observation (only based on file names).

        Intput:
            fns: A list of file names.

        Output:
            groups: A list of groups (each group is a list of filenames).
    """
    groups = []
    for ii, fn in enumerate(fns):
        group = [fn]
        for jj in range(len(fns)-1, ii, -1):
            other = fns[jj]
            if match_observation(fn, other):
                group.append(fns.pop(jj))
        groups.append(group)
    return groups

def preprocess(fns):
    """Given a list of filenames apply any preprocessing steps
        required. Return a list of file names of the output
        files.

        Input:
            fns: List of names of files to preprocess.

        Output:
            outfns: List of names of preprocessed files.
    """
    datafile_type = get_datafile_type(fns)
    return datafile_type.preprocess(fns)

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
        import config.basic
        wappfn = '.'.join([self.project_id, self.source_name, \
                            "wapp%d" % (self.beam_id/2+1), \
                            "%5d" % int(self.timestamp_mjd), \
                            self.fnmatch(self.original_file).groupdict()['scan']])
        # Get corrected beam positions
        matches = [line for line in open(config.basic.coords_table, 'r') if \
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
            raise DataFileError("Bad number of matches (%d) in coords table! " \
                             "(Files: %s)" % (len(matches), ", ".join(self.fns)))

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

    @classmethod
    def are_grouped(cls, fn1, fn2):
        """Return True if file names fn1 and fn2 represent data files
            that should grouped (ie belong to the same observation).
            Returns False if files are of different types or don't
            belong together.

            Note: fn1 and fn2 cannot have the same file name (path is ignored).
 
            Inputs:
                fn1: A data file name.
                fn2: A data file name.
 
            Output:
                grpd: Boolean value. True if files belong together.
        """
        raise NotImplementedError("The classmethod 'are_grouped(...)' " \
                                    "must be overridden!")

    @classmethod
    def is_complete(cls, fns):
        """Return True if the list of file names, 'fns' is complete.
            
            Inputs:
                fns: a list of filenames.

            Output:
                complete: Boolean value. True if list of file names
                    is a group that is complete.
        """
        raise NotImplementedError("The classmethod 'is_complete(...)' " \
                                    "must be overridden!")

    @classmethod
    def preprocess(cls, fns):
        """Given a list of filenames apply any preprocessing steps
            required. Return a list of file names of the output
            files.

            Input:
                fns: List of names of files to preprocess.

            Output:
                outfns: List of names of preprocessed files.
        """
        # By default do nothing.
        return fns

class PsrfitsData(Data):
    """PSRFITS Data object.
    """
    def __init__(self, fitsfns):
        """PSR fits Header object constructor.
        """
        from formats import psrfits        
        
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
        self.header_version = float(self.specinfo.header_version)


class WappPsrfitsData(PsrfitsData):
    """PSRFITS Data object for WAPP data.
    """
    filename_re = re.compile(r'^(?P<projid>[Pp]\d{4})_(?P<mjd>\d{5})_' \
                                r'(?P<sec>\d{5})_(?P<scan>\d{4})_' \
                                r'(?P<source>.*)_(?P<beam>\d)\.w4bit\.fits$')

    def __init__(self, fitsfns):
        super(WappPsrfitsData, self).__init__(fitsfns)
        self.obstype = 'WAPP'
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
        self.project_id = m.groupdict()['projid']
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    str(self.scan_num)])

    def update_positions(self):
        """Update positions in raw data file's header.
            
            Note: This cannot be undone!
        """
        import pyfits
        if self.posn_corrected:
            for fn in self.fns:
                hdus = pyfits.open(fn, mode='update')
                primary = hdus['PRIMARY'].header
                primary['RA'] = self.correct_ra 
                primary['DEC'] = self.correct_decl
                hdus.close() # hdus are updated at close-time

    @classmethod
    def are_grouped(cls, fn1, fn2):
        """Return True if file names fn1 and fn2 represent data files
            that should grouped (ie belong to the same observation).
            Returns False if files are of different types or don't
            belong together.

            Note: fn1 and fn2 cannot have the same file name (path is ignored).
 
            *** Note: WAPP files are not supposed to be grouped. This
            ***     function always returns False.

            Inputs:
                fn1: A data file name.
                fn2: A data file name.
 
            Output:
                grpd: Boolean value. True if files belong together.
        """
        return False

    @classmethod
    def is_complete(cls, fns):
        """Return True if the list of file names, 'fns' is complete.
            
            Inputs:
                fns: a list of filenames.

            Output:
                complete: Boolean value. True if list of file names
                    is a group that is complete.
        """
        if len(fns) == 1:
            complete = cls.is_correct_filetype(fns) 
        elif len(fns) > 1:
            warnings.warn("List of PSRFITS WAPP files has " \
                            "too many files (%d)!" % len(fns))
            complete = False
        else:
            complete = False
        return complete


class NuppiPsrfitsData(PsrfitsData):
    """PSR fits Data object for NUPPI data.
    """
    filename_re = re.compile(r'nuppi_(?P<mjd>\d{5})_(?P<source>SRV\d{6})\_' \
                                r'(?P<scan>\d{6})_(?P<fileno>\d{4}).fits')

class MockPsrfitsData(PsrfitsData):
    """PSR fits Data object for MockSpec data.
    """
    filename_re = re.compile(r'^4bit-(?P<projid>[Pp]\d{4})\.(?P<date>\d{8})\.' \
                                r'(?P<source>.*)\.b(?P<beam>[0-7])' \
                                r's(?P<subband>[01])g0.(?P<scan>\d{5})\.fits')

    def __init__(self, fitsfns):
        super(MockPsrfitsData, self).__init__(fitsfns)
        self.obstype = 'Mock'
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
        self.project_id = m.groupdict()['projid']
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    str(self.scan_num)])

    @classmethod
    def are_grouped(cls, fn1, fn2):
        """Return True if file names fn1 and fn2 represent data files
            that should grouped (ie belong to the same observation).
            Returns False if files are of different types or don't
            belong together.

            Note: fn1 and fn2 cannot have the same file name (path is ignored).
 
            Inputs:
                fn1: A data file name.
                fn2: A data file name.
 
            Output:
                grpd: Boolean value. True if files belong together.
        """
        fnmatch1 = cls.fnmatch(fn1)
        fnmatch2 = cls.fnmatch(fn2)

        if fnmatch1 is None or fnmatch2 is None:
            grpd = False
        else:
            mdict1 = fnmatch1.groupdict()
            mdict2 = fnmatch2.groupdict()
            sub1 = mdict1.pop('subband')
            sub2 = mdict2.pop('subband')
            if (sub1 == '0' and sub2 == '1') or (sub1 == '1' and sub2 == '0'):
                grpd = (mdict1 == mdict2) # all other values must be identical
            else:
                grpd = False
        return grpd
           
    @classmethod
    def is_complete(cls, fns):
        """Return True if the list of file names, 'fns' is complete.
            
            Inputs:
                fns: a list of filenames.

            Output:
                complete: Boolean value. True if list of file names
                    is a group that is complete.
        """
        if len(fns) == 2:
            complete = cls.are_grouped(*fns)
        elif len(fns) > 2:
            warnings.warn("List of Mock files has " \
                            "too many files (%d)!" % len(fns))
            complete = False
        else:
            complete = False
        return complete

    @classmethod
    def preprocess(cls, fns):
        """Given a list of filenames apply any preprocessing steps
            required. Return a list of file names of the output
            files.

            Input:
                fns: List of names of files to preprocess.

            Output:
                outfns: List of names of preprocessed files.
        """
        infiles = " ".join(fns)
        fnmatchdict = cls.fnmatch(fns[0]).groupdict()
        obsdata = cls(fns)
        outbasenm = "%(projid)s.%(date)s.%(source)s.b%(beam)s.%(scan)s" % \
                        fnmatchdict
        
        outfile = outbasenm + "_0001.fits" # '0001' added is the filenumber

        # Clobber output file
        #if os.path.exists(outfile):
        #    os.remove(outfile)
        
        # Merge mock subbands
        mergecmd = "combine_mocks %s -o %s" % (infiles, outbasenm)
        pipeline_utils.execute(mergecmd, stdout=outbasenm+"_merge.out")
        
        # Rename file to remove the '_0001' that was added
        mergedfn = outbasenm+'.fits'
        os.rename(outfile, mergedfn)
        
        merged = autogen_dataobj([mergedfn])
        if not isinstance(merged, MergedMockPsrfitsData):
            raise ValueError("Preprocessing of Mock data has not produced " \
                                "a recognized merged file!")
        subints_with_cal = merged.get_subints_with_cal()
        num_subints = merged.num_samples/merged.num_samples_per_record
        subints_with_cal = [isub for isub in subints_with_cal \
                                if isub >=0 and isub < num_subints]
        if len(subints_with_cal):
            rowdelcmds = []
            startrow = subints_with_cal[0]
            numrows = 1
            numdelrows = 0 # Number of rows deleted so far
            # Add infinity to the list of subints with cal to 
            # make sure we remove the last cal-block.
            for isub in subints_with_cal[1:]+[np.inf]:
                isub -= numdelrows # Adjust based on the number of missing rows
                if isub == startrow+numrows:
                    numrows+=1
                else:
                    if startrow+1 < 0.1*num_subints:
                        print "Cal-affected region is within 10%% of start of obs " \
                                "remove all rows before cal. (cal start: %d; " \
                                "total num rows: %d)" % (startrow, num_subints)
                        numrows += startrow
                        startrow = 0
                    elif startrow+numrows > 0.9*num_subints:
                        print "Cal-affected region is within 10%% of end of obs " \
                                "remove all rows after cal. (cal start: %d; " \
                                "total num rows: %d)" % (startrow, num_subints)
                        numrows = num_subints - startrow
                    numdelrows += numrows # Keep track of number of rows deleted
                    print "Will delete %d rows starting at %d" % (numrows, startrow+1)
                    rowdelcmds.append("fitsdelrow %s[SUBINT] %d %d" % \
                                    (mergedfn, startrow+1, numrows))
                    startrow = isub-numrows # NOTE: only delete numrows because
                                            # numdelrows has already been deleted
                                            # from isub
                    num_subints -= numrows # Adjust number of subints in the file
                    print "resetting startrow to", startrow
                    numrows = 1
            for rowdelcmd in rowdelcmds:
                pipeline_utils.execute(rowdelcmd)

        # Make dat file
        prepdatacmd = "prepdata -noclip -nobary -dm 0 -o %s_post_DM0.00 %s" % (outbasenm, mergedfn)
        pipeline_utils.execute(prepdatacmd, stdout=outbasenm+"_post_prepdata.out")

        return [mergedfn]


class MergedMockPsrfitsData(PsrfitsData):
    """PSRFITS Data object for merged MockSpec data.
    """
    filename_re = re.compile(r'^(?P<projid>[Pp]\d{4})\.(?P<date>\d{8})\.' \
                                r'(?P<source>.*)\.b(?P<beam>[0-7])' \
                                r'\.(?P<scan>\d{5})\.fits')

    def __init__(self, fitsfns):
        super(MergedMockPsrfitsData, self).__init__(fitsfns)
        self.obstype = 'Mock'
        # Note Puerto Rico doesn't observe daylight savings time
        # so it is 4 hours behind UTC all year
        dayfrac = calendar.MJD_to_date(self.timestamp_mjd)[-1]%1
        self.start_ast = int((dayfrac*24-4)*3600)
        self.start_ast %= 24*3600
        self.num_ifs = 2
        # Parse filename to get the scan number
        m = self.fnmatch(fitsfns[0])
        self.beam_id = int(m.groupdict()['beam'])
        self.get_correct_positions() # This sets self.right_ascension, etc.
        self.scan_num = m.groupdict()['scan']
        self.project_id = m.groupdict()['projid']
        self.obs_name = '.'.join([self.project_id, self.source_name, \
                                    str(int(self.timestamp_mjd)), \
                                    str(self.scan_num)])

    @classmethod
    def are_grouped(cls, fn1, fn2):
        """Return True if file names fn1 and fn2 represent data files
            that should grouped (ie belong to the same observation).
            Returns False if files are of different types or don't
            belong together.

            Note: fn1 and fn2 cannot have the same file name (path is ignored).
 
            *** Note: Merged Mock files don't need to be grouped 
            ***     (they're already merged!) This function always 
            ***     returns False.

            Inputs:
                fn1: A data file name.
                fn2: A data file name.
 
            Output:
                grpd: Boolean value. True if files belong together.
        """
        return False

    @classmethod
    def is_complete(cls, fns):
        """Return True if the list of file names, 'fns' is complete.
            
            Inputs:
                fns: a list of filenames.

            Output:
                complete: Boolean value. True if list of file names
                    is a group that is complete.
        """
        if len(fns) == 1:
            complete = cls.is_correct_filetype(fns) 
        elif len(fns) > 1:
            warnings.warn("List of merged Mock files has " \
                            "too many files (%d)!" % len(fns))
            complete = False
        else:
            complete = False
        return complete

    def get_subints_with_cal(self, nsigma=15, margin_of_error=1):
        """Return a list of subint numbers with the cal turned on.
 
            Input:
                nsigma: The number of sigma above the median a
                    subint needs to be in order to be flagged as
                    having the cal on. (Default: 15)

                    NOTE: The median absolute deviation is used in
                        place of the standard deviation here.
                margin_of_error: For each subint with the cal on 
                    also flag 'margin_of_error' subints on either
                    side. (Default: 1)
 
            Output:
                subints_with_cal: A sorted list of subints with the
                    cal on.
        """
        fn = self.fns[0]
        if not fn.endswith(".fits"):
            raise ValueError("Filename doesn't end with '.fits'!")
        basenm = fn[:-5] # Chop off '.fits'
        # Make dat file
        prepdatacmd = "prepdata -noclip -nobary -dm 0 -o %s_pre_DM0.00 %s" % (basenm, fn)
        pipeline_utils.execute(prepdatacmd, stdout=basenm+"_pre_prepdata.out")
        datfn = basenm+"_pre_DM0.00.dat"
        samp_per_rec = self.num_samples_per_record
        dat = np.memmap(datfn, mode='r', dtype='float32')
        dat = dat[:dat.size/samp_per_rec*samp_per_rec]
        dat.shape = (dat.size/samp_per_rec, samp_per_rec)
        meds = np.median(dat, axis=1)
        med_of_meds = np.median(meds)
        mad_of_meds = np.median(np.abs(meds-med_of_meds))
        print "Median of medians:", med_of_meds
        print "MAD of medians:", mad_of_meds
        for ii, (med, nsig) in enumerate(zip(meds, (meds-med_of_meds)/mad_of_meds)):
            print "%d: %g (%g)" % (ii, med, nsig)
        has_cal = (meds-med_of_meds)/mad_of_meds > nsigma
 
        subints_with_cal = set()
        print "Subints with cal: %s" % sorted(list(subints_with_cal))
        for isub in np.flatnonzero(has_cal):
            subints_with_cal.add(isub)
            for x in range(1,margin_of_error+1):
                subints_with_cal.add(isub-x)
                subints_with_cal.add(isub+x)
 
        print "Conservative list of subints to remove: %s" % sorted(list(subints_with_cal))

        # Remove dat file created
        os.remove(datfn)
        
        return sorted(list(subints_with_cal))


class DataFileError(pipeline_utils.PipelineError):
    pass


def main():
    if sys.argv[1]=='preprocess':
        # Preprocess data files
        preprocess(sys.argv[2:])
    elif sys.argv[1]=='filetype':
        # Print file type
        filetype = get_datafile_type(sys.argv[2:])
        print filetype.__name__
    elif sys.argv[1]=='group+preprocess':
        # Group files
        groups = group_files(sys.argv[2:])
        # Preprocess each group
        for group in groups:
            if is_complete(group):
                preprocess(group)
    else:
        # Print datafile's header information
        data = autogen_dataobj(sys.argv[1:])
        print data.__dict__


if __name__=='__main__':
    main()
