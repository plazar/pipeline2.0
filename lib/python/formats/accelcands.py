"""
Interface to parse *.accelcands files, combined and 
sifted candidates produced by accelsearch for survey 
pointings.

Patrick Lazarus, Dec. 8, 2010
"""

import os.path
import sys
import re
import types


dmhit_re = re.compile(r'^ *DM= *(?P<dm>[^ ]*) *SNR= *(?P<snr>[^ ]*) *\** *$')
candinfo_re = re.compile(r'^(?P<accelfile>.*):(?P<candnum>\d*) *(?P<dm>[^ ]*)' \
                         r' *(?P<snr>[^ ]*) *(?P<sigma>[^ ]*) *(?P<numharm>[^ ]*)' \
                         r' *(?P<ipow>[^ ]*) *(?P<cpow>[^ ]*) *(?P<period>[^ ]*)' \
                         r' *(?P<r>[^ ]*) *(?P<z>[^ ]*) *\((?P<numhits>\d*)\)$')


class AccelCand(object):
    """Object to represent candidates as they are listed
        in *.accelcands files.
    """
    def __init__(self, accelfile, candnum, dm, snr, sigma, numharm, \
                        ipow, cpow, period, r, z, *args, **kwargs):
        self.accelfile = accelfile
        self.candnum = int(candnum)
        self.dm = float(dm)
        self.snr = float(snr)
        self.sigma = float(sigma)
        self.numharm = int(numharm)
        self.ipow = float(ipow)
        self.cpow = float(cpow)
        self.period = float(period)
        self.r = float(r)
        self.z = float(z)
        self.dmhits = []

    def add_dmhit(self, dm, snr):
        self.dmhits.append(DMHit(dm, snr))

    def __str__(self):
        cand = self.accelfile + ':' + `self.candnum`
        result = "%-65s   %7.2f  %6.2f  %6.2f  %s   %7.1f  " \
                 "%7.1f  %12.6f  %10.2f  %8.2f  (%d)\n" % \
            (cand, self.dm, self.snr, self.sigma, \
                "%2d".center(7) % self.numharm, self.ipow, \
                self.cpow, self.period*1000.0, self.r, self.z, \
                len(self.dmhits))
        for dmhit in self.dmhits:
            result += str(dmhit)
        return result

    def __cmp__(self, other):
        """By default candidates are sorted by increasing sigma.
        """
        return cmp(self.sigma, other.sigma)


class AccelCandlist(list):
    def __init__(self, *args, **kwargs):
        super(AccelCandlist, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        return np.array([getattr(c, key) for c in self])

    def write_candlist(self, fn=sys.stdout):
        """Write AccelCandlist to a file with filename fn.
 
            Inputs:
                fn - path of output candlist, or an open file object
                    (Default: standard output stream)
            NOTE: if fn is an already-opened file-object it will not be
                    closed by this function.
        """
        if type(fn) == types.StringType:
            toclose = True
            file = open(fn, 'w')
        else:
            # fn is actually a file-object
            toclose = False
            file = fn
 
        # Print column headers
        file.write("#" + "file:candnum".center(66) + "DM".center(9) + \
                   "SNR".center(8) + "sigma".center(8) + "numharm".center(9) + \
                   "ipow".center(9) + "cpow".center(9) +  "P(ms)".center(14) + \
                   "r".center(12) + "z".center(8) + "numhits".center(9) + "\n")

        self.sort(reverse=True) # Sort cands by decreasing simga
        for cand in self:
            cand.dmhits.sort()
            file.write(str(cand))
        if toclose:
            file.close()


class DMHit(object):
    """Object to represent a DM hit of an accelcands candidate.
    """
    def __init__(self, dm, snr):
        self.dm = float(dm)
        self.snr = float(snr)

    def __str__(self):
        result = "  DM=%6.2f SNR=%5.2f" % (self.dm, self.snr)
        result += "   " + int(self.snr/3.0)*'*' + '\n'
        return result

    def __cmp__(self, other):
        """By default DM hits are sorted by DM.
        """
        return cmp(self.dm, other.dm)


class AccelcandsError(Exception):
    """An error to throw when a line in a *.accelcands file
        has an unrecognized format.
    """
    pass


def parse_candlist(candlistfn):
    """Parse candidate list and return a list of AccelCand objects.
        
        Inputs:
            candlistfn - path of candlist, or an open file object
    
        Outputs:
            An AccelCandlist object
    """
    if type(candlistfn) == types.StringType or type(candlistfn) == types.UnicodeType:
        candlist = open(candlistfn, 'r')
        toclose = True
    else:
        # candlistfn is actually a file-object
        candlist = candlistfn
        toclose = False
    cands = AccelCandlist()
    for line in candlist:
        if not line.partition("#")[0].strip():
            # Ignore lines with no content
            continue
        candinfo_match = candinfo_re.match(line)
        if candinfo_match:
            cdict = candinfo_match.groupdict()
            cdict['period'] = float(cdict['period'])/1000.0 # convert ms to s
            cands.append(AccelCand(**cdict))
        else:
            dmhit_match = dmhit_re.match(line)
            if dmhit_match:
                cands[-1].add_dmhit(**dmhit_match.groupdict())
            else:
                raise AccelcandsError("Line has unrecognized format!\n(%s)\n" % line)
    if toclose:
        candlist.close()
    return cands
