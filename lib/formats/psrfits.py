#!/usr/bin/env python

"""
Collect PSR FITS information, emulating behaviour of PRESTO.

Patrick Lazarus, May 11, 2010
"""
import re
import os
import warnings
import sys

import slalib
import pyfits
import numpy as np
import psr_utils
from astro_utils import protractor

# Regular expression for parsing DATE-OBS card's format.
date_obs_re = re.compile(r"^(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-" \
                            "(?P<day>[0-9]{2})T(?P<hour>[0-9]{2}):" \
                            "(?P<min>[0-9]{2}):(?P<sec>[0-9]{2}" \
                            "(?:\.[0-9]+)?)$")

class SpectraInfo:
    def __init__(self, filenames, verbose=False):
        self.filenames = filenames
        self.num_files = len(filenames)
        self.N = 0
        self.user_poln = 0 
        self.default_poln = 0

        # Initialise a few arrays
        self.start_MJD = np.empty(self.num_files)
        self.num_subint = np.empty(self.num_files)
        self.start_subint = np.empty(self.num_files)
        self.start_spec = np.empty(self.num_files)
        self.num_pad = np.empty(self.num_files)
        self.num_spec = np.empty(self.num_files)

        # The following should default to False
        self.need_scale = False
        self.need_offset = False
        self.need_weight = False
        self.need_flipband = False

        for ii, fn in enumerate(filenames):
            if verbose:
                print "Reading '%s'" % fn
            if not is_PSRFITS(fn):
                raise ValueError("File '%s' does not appear to be PSRFITS!" % fn)
            
            # Open the PSRFITS file
            hdus = pyfits.open(fn, mode='readonly', checksum=False)
            self.hdus = hdus

            if ii==0:
                self.hdu_names = [hdu.name for hdu in hdus]

            primary = hdus['PRIMARY'].header

            if primary.has_key('IBEAM'):
                self.beam_id = primary['IBEAM']
            elif hdus[1].header.has_key('BEAM'):
                self.beam_id = hdus[1].header['BEAM']
            else:
                self.beam_id = None

            if primary.has_key('TELESCOP'):
                telescope = primary['TELESCOP']
                # Quick fix for MockSpec data...
                if telescope == "ARECIBO 305m":
                    telescope = "Arecibo"
            else:
                telescope = ""
            if ii == 0:
                self.telescope = telescope
            else:
                if telescope != self.telescope[0]:
                    warnings.warn("'TELESCOP' values don't match for files 0 and %d!" % ii)

            self.observer = primary['OBSERVER']
            self.source = primary['SRC_NAME']
            self.frontend = primary['FRONTEND']
            self.backend = primary['BACKEND']
            self.project_id = primary['PROJID']
            self.date_obs = primary['DATE-OBS']
            self.poln_type = primary['FD_POLN']
            self.ra_str = primary['RA']
            self.dec_str = primary['DEC']
            self.fctr = primary['OBSFREQ']
            self.orig_num_chan = primary['OBSNCHAN']
            self.orig_df = primary['OBSBW']
            self.beam_FWHM = primary['BMIN']

            # CHAN_DM card is not in earlier versions of PSRFITS
            if primary.has_key('CHAN_DM'):
                self.chan_dm = primary['CHAN_DM']
            else:
                self.chan_dm = 0.0

            self.start_MJD[ii] = primary['STT_IMJD'] + (primary['STT_SMJD'] + \
                                    primary['STT_OFFS'])/psr_utils.SECPERDAY
            
            # Are we tracking
            track = (primary['TRK_MODE'] == "TRACK")
            if ii==0:
                self.tracking = track
            else:
                if track != self.tracking:
                    warnings.warn("'TRK_MODE' values don't match for files 0 and %d" % ii)

            # Now switch to the subint HDU header
            subint = hdus['SUBINT'].header
            
            self.dt = subint['TBIN']
            self.num_channels = subint['NCHAN']
            self.num_polns = subint['NPOL']
            
            # PRESTO's 'psrfits.c' has some settings based on environ variables
            envval = os.getenv("PSRFITS_POLN")
            if envval is not None:
                ival = int(envval)
                if ((ival > -1) and (ival < self.num_polns)):
                    print "Using polarisation %d (from 0-%d) from PSRFITS_POLN." % \
                                (ival, self.num_polns-1)
                    self.default_poln = ival
                    self.user_poln = 1

            self.poln_order = subint['POL_TYPE']
            if subint['NCHNOFFS'] > 0:
                warnings.warn("first freq channel is not 0 in file %d" % ii)
            self.spectra_per_subint = subint['NSBLK']
            self.bits_per_sample = subint['NBITS']
            self.num_subint[ii] = subint['NAXIS2']
            self.start_subint[ii] = subint['NSUBOFFS']
            self.time_per_subint = self.dt * self.spectra_per_subint

            # Now pull stuff from the columns
            subint_hdu = hdus['SUBINT']
            # The following is a hack to read in only the first row 
            # from the fits file
            subint_hdu.columns._shape = 1

            # Identify the OFFS_SUB column number
            if 'OFFS_SUB' not in subint_hdu.columns.names:
                warnings.warn("Can't find the 'OFFS_SUB' column!")
            else:
                colnum = subint_hdu.columns.names.index('OFFS_SUB')
                if ii==0:
                    self.offs_sub_col = colnum 
                elif self.offs_sub_col != colnum:
                    warnings.warn("'OFFS_SUB' column changes between files 0 and %d!" % ii)

                # Read the OFFS_SUB column value for the 1st row
                offs_sub = subint_hdu.header[colnum]
                numrows = int((offs_sub - 0.5 * self.time_per_subint) / \
                                self.time_per_subint + 1e-7)
                # Check to see if any rows have been deleted or are missing
                if numrows > self.start_subint[ii]:
                    warnings.warn("Warning: NSUBOFFS reports %d previous rows\n" \
                                  "         but OFFS_SUB implies %s. Using OFFS_SUB.\n" \
                                  "         Will likely be able to correct for this.\n" % \
                                    (self.start_subint[ii], numrows))
                self.start_subint[ii] = numrows

            # This is the MJD offset based on the starting subint number
            MJDf = (self.time_per_subint * self.start_subint[ii])/psr_utils.SECPERDAY
            # The start_MJD values should always be correct
            self.start_MJD[ii] += MJDf

            # Compute the starting spectra from the times
            MJDf = self.start_MJD[ii] - self.start_MJD[0]
            if MJDf < 0.0:
                raise ValueError("File %d seems to be from before file 0!" % ii)

            self.start_spec[ii] = (MJDf * psr_utils.SECPERDAY / self.dt + 0.5)

            # Identify the data column and the data type
            if 'DATA' not in subint_hdu.columns.names:
                warnings.warn("Can't find the 'DATA' column!")
                self.FITS_typecode = 0
                self.data_col = 0
            else:
                colnum = subint_hdu.columns.names.index('DATA')
                if ii==0:
                    self.data_col = colnum
                    self.FITS_typecode = subint_hdu.columns[self.data_col].format[-1]
                elif self.data_col != colnum:
                    warnings.warn("'DATA' column changes between files 0 and %d!" % ii)

            # Telescope azimuth
            if 'TEL_AZ' not in subint_hdu.columns.names:
                self.azimuth = 0.0
            else:
                colnum = subint_hdu.columns.names.index('TEL_AZ')
                if ii==0:
                    self.tel_az_col = colnum
                    self.azimuth = subint_hdu.data[0]['TEL_AZ']

            # Telescope zenith angle
            if 'TEL_ZEN' not in subint_hdu.columns.names:
                self.zenith_ang = 0.0
            else:
                colnum = subint_hdu.columns.names.index('TEL_ZEN')
                if ii==0:
                    self.tel_zen_col = colnum
                    self.zenith_ang = subint_hdu.data[0]['TEL_ZEN']

            # Observing frequencies
            if 'DAT_FREQ' not in subint_hdu.columns.names:
                warnings.warn("Can't find the channel freq column, 'DAT_FREQ'!")
            else:
                colnum = subint_hdu.columns.names.index('DAT_FREQ')
                freqs = subint_hdu.data[0]['DAT_FREQ']
                if ii==0:
                    self.freqs_col = colnum
                    self.df = freqs[1]-freqs[0]
                    self.lo_freq = freqs[0]
                    self.hi_freq = freqs[-1]
                    # Now check that the channel spacing is the same throughout
                    ftmp = freqs[1:] - freqs[:-1]
                    if np.any((ftmp - self.df)) > 1e-7:
                        warnings.warn("Channel spacing changes in file %d!" % ii)
                else:
                    ftmp = np.abs(self.df - (freqs[1]-freqs[0]))
                    if ftmp > 1e-7:
                        warnings.warn("Channel spacing between files 0 and %d!" % ii)
                    ftmp = np.abs(self.lo_freq-freqs[0])
                    if ftmp > 1e-7:
                        warnings.warn("Low channel changes between files 0 and %d!" % ii)
                    ftmp = np.abs(self.hi_freq-freqs[-1])
                    if ftmp > 1e-7:
                        warnings.warn("High channel changes between files 0 and %d!" % ii)

            # Data weights
            if 'DAT_WTS' not in subint_hdu.columns.names:
                warnings.warn("Can't find the channel weights column, 'DAT_WTS'!")
            else:
                colnum = subint_hdu.columns.names.index('DAT_WTS')
                if ii==0:
                    self.dat_wts_col = colnum
                elif self.dat_wts_col != colnum:
                    warnings.warn("'DAT_WTS column changes between files 0 and %d!" % ii)
                if np.any(subint_hdu.data[0]['DAT_WTS'] != 1.0):
                    self.need_weight = True
                
            # Data offsets
            if 'DAT_OFFS' not in subint_hdu.columns.names:
                warnings.warn("Can't find the channel offsets column, 'DAT_OFFS'!")
            else:
                colnum = subint_hdu.columns.names.index('DAT_OFFS')
                if ii==0:
                    self.dat_offs_col = colnum
                elif self.dat_offs_col != colnum:
                    warnings.warn("'DAT_OFFS column changes between files 0 and %d!" % ii)
                if np.any(subint_hdu.data[0]['DAT_OFFS'] != 0.0):
                    self.need_offset = True

            # Data scalings
            if 'DAT_SCL' not in subint_hdu.columns.names:
                warnings.warn("Can't find the channel scalings column, 'DAT_SCL'!")
            else:
                colnum = subint_hdu.columns.names.index('DAT_SCL')
                if ii==0:
                    self.dat_scl_col = colnum
                elif self.dat_scl_col != colnum:
                    warnings.warn("'DAT_SCL' column changes between files 0 and %d!" % ii)
                if np.any(subint_hdu.data[0]['DAT_SCL'] != 1.0):
                    self.need_scale = True

            # Comute the samples per file and the amount of padding
            # that the _previous_ file has
            self.num_pad[ii] = 0
            self.num_spec[ii] = self.spectra_per_subint * self.num_subint[ii]
            if ii>0:
                if self.start_spec[ii] > self.N: # Need padding
                    self.num_pad[ii-1] = self.start_spec[ii] - self.N
                    self.N += self.num_pad[ii-1]
            self.N += self.num_spec[ii]

        # Finished looping through PSRFITS files. Finalise a few things.
        # Convert the position strings into degrees        
        self.ra2000 = protractor.convert(self.ra_str, 'hmsstr', 'deg')
        self.dec2000 = protractor.convert(self.dec_str, 'dmsstr', 'deg')
        
        # Are the polarisations summed?
        if (self.poln_order=="AA+BB") or (self.poln_order=="INTEN"):
            self.summed_polns = True
        else:
            self.summed_polns = False

        # Calculate some others
        self.T = self.N * self.dt
        self.orig_df /= float(self.orig_num_chan)
        self.samples_per_spectra = self.num_polns * self.num_channels
        # Note: the following is the number of bytes that will be in
        #       the returned array.
        if self.bits_per_sample < 8:
            self.bytes_per_spectra = self.samples_per_spectra
        else:
            self.bytes_per_spectra = (self.bits_per_sample * self.samples_per_spectra)/8
        self.samples_per_subint = self.samples_per_spectra * self.spectra_per_subint
        self.bytes_per_subint = self.bytes_per_spectra * self.spectra_per_subint

        # Flip the band?
        if self.hi_freq < self.lo_freq:
            tmp = self.hi_freq
            self.hi_freq = self.lo_freq
            self.lo_freq = tmp
            self.df *= -1.0
            self.need_flipband = True
        # Compute the bandwidth
        self.BW = self.num_channels * self.df

        # A few extras
        self.start_lst = primary['STT_LST']

        # Close the psrfits file
        hdus.close()

    def __str__(self):
        """Format spectra_info's information into a easy to
            read string and return it.
        """
        result = [] # list of strings. Will be concatenated with newlines (\n).
        result.append("From the PSRFITS file '%s':" % self.filenames[0])
        result.append("                       HDUs = %s" % ', '.join(self.hdu_names))
        result.append("                  Telescope = %s" % self.telescope)
        result.append("                   Observer = %s" % self.observer)
        result.append("                Source Name = %s" % self.source)
        result.append("                   Frontend = %s" % self.frontend)
        result.append("                    Backend = %s" % self.backend)
        result.append("                 Project ID = %s" % self.project_id)
        # result.append("                Scan Number = %s" % self.scan_number)
        result.append("            Obs Date String = %s" % self.date_obs)
        imjd, fmjd = DATEOBS_to_MJD(self.date_obs)
        mjdtmp = "%.14f" % fmjd
        result.append("  MJD start time (DATE-OBS) = %5d.%14s" % (imjd, mjdtmp[2:]))
        result.append("     MJD start time (STT_*) = %19.14f" % self.start_MJD[0])
        result.append("                   RA J2000 = %s" % self.ra_str)
        result.append("             RA J2000 (deg) = %-17.15g" % self.ra2000)
        result.append("                  Dec J2000 = %s" % self.dec_str)
        result.append("            Dec J2000 (deg) = %-17.15g" % self.dec2000)
        result.append("                  Tracking? = %s" % self.tracking)
        result.append("              Azimuth (deg) = %-.7g" % self.azimuth)
        result.append("           Zenith Ang (deg) = %-.7g" % self.zenith_ang)
        result.append("          Polarisation type = %s" % self.poln_type)
        if (self.num_polns>=2) and (not self.summed_polns):
            numpolns = "%d" % self.num_polns
        elif self.summed_polns:
            numpolns = "2 (summed)"
        else:
            numpolns = "1"
        result.append("            Number of polns = %s" % numpolns)
        result.append("          Polarisation oder = %s" % self.poln_order)
        result.append("           Sample time (us) = %-17.15g" % (self.dt * 1e6))
        result.append("         Central freq (MHz) = %-17.15g" % self.fctr)
        result.append("          Low channel (MHz) = %-17.15g" % self.lo_freq)
        result.append("         High channel (MHz) = %-17.15g" % self.hi_freq)
        result.append("        Channel width (MHz) = %-17.15g" % self.df)
        result.append("         Number of channels = %d" % self.num_channels)
        if self.chan_dm != 0.0:
            result.append("   Orig Channel width (MHz) = %-17.15g" % self.orig_df)
            result.append("    Orig Number of channels = %d" % self.orig_num_chan)
            result.append("    DM used for chan dedisp = %-17.15g" % self.chan_dm)
        result.append("      Total Bandwidth (MHz) = %-17.15g" % self.BW)
        result.append("         Spectra per subint = %d" % self.spectra_per_subint)
        result.append("            Starting subint = %d" % self.start_subint[0])
        result.append("           Subints per file = %d" % self.num_subint[0])
        result.append("           Spectra per file = %d" % self.num_spec[0])
        result.append("        Time per file (sec) = %-.12g" % (self.num_spec[0]*self.dt))
        result.append("              FITS typecode = %s" % self.FITS_typecode)
        if debug:
            result.append("                DATA column = %d" % self.data_col)
            result.append("            bits per sample = %d" % self.bits_per_sample)
            if self.bits_per_sample < 8:
                spectmp = (self.bytes_per_spectra * self.bits_per_sample) / 8
                subtmp = (self.bytes_per_subint * self.bits_per_sample) / 8
            else:
                spectmp = self.bytes_per_spectra
                subtmp = self.bytes_per_subint
            result.append("          bytes per spectra = %d" % spectmp)
            result.append("        samples per spectra = %d" % self.samples_per_spectra)
            result.append("           bytes per subint = %d" % subtmp)
            result.append("         samples per subint = %d" % self.samples_per_subint)
            result.append("              Need scaling? = %s" % self.need_scale)
            result.append("              Need offsets? = %s" % self.need_offset)
            result.append("              Need weights? = %s" % self.need_weight)
            result.append("        Need band inverted? = %s" % self.need_flipband)

        return '\n'.join(result)


def DATEOBS_to_MJD(dateobs):
    """Convert DATE-OBS string from PSRFITS primary HDU to a MJD.
        Returns a 2-tuple:
            (integer part of MJD, fractional part of MJD)
    """
    # Parse string using regular expression defined at top of file
    m = date_obs_re.match(dateobs)
    mjd_fracday = (float(m.group("hour")) + (float(m.group("min")) + \
                    (float(m.group("sec")) / 60.0)) / 60.0) / 24.0
    mjd_day, err = slalib.sla_cldj(float(m.group("year")), \
                        float(m.group("month")), float(m.group("day")))
    return mjd_day, mjd_fracday


def is_PSRFITS(filename):
    """Return True if filename appears to be PSRFITS format.
        Return False otherwise.
    """
    hdus = pyfits.open(filename, mode='readonly')
    primary = hdus['PRIMARY'].header

    try:
        isPSRFITS = ((primary['FITSTYPE'] == "PSRFITS") and \
                        (primary['OBS_MODE'] == "SEARCH"))
    except KeyError:
        isPSRFITS = False
    
    hdus.close() 
    return isPSRFITS


def debug_mode(mode=None):
    """Set debugging mode.
        If 'mode' is None return current debug mode.
    """
    global debug
    if mode is None:
        return debug
    else:
        debug = bool(mode)


def main():
    infiles = sys.argv[1:] # input files
    specinf = SpectraInfo(infiles)
    print specinf


if __name__=='__main__':
    debug_mode(1)
    main()
