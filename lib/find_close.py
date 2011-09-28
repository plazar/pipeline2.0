#!/usr/bin/env python

import sys
import numpy as np
from pypulsar.utils import astro

ra0 = sys.argv[1]
dec0 = sys.argv[2]
if len(sys.argv) > 3:
    thresh = float(sys.argv[3]) # Degrees
else:
    thresh = 0.5 # Degrees

mockcoords = np.loadtxt("PALFA_mock_coords_table.txt", usecols=(0,1,2), \
                        dtype=([("fn", np.str_, 255), ("ra_deg", np.float, 1), \
                                ("dec_deg", np.float, 1)]))
mockseps = np.empty(len(mockcoords))

for ii, (fn, ra_deg, dec_deg) in enumerate(mockcoords):
    mockseps[ii] = astro.sextant.angsep(ra_deg, dec_deg, ra0, dec0, \
                                    input=("deg", "sexigesimal"), output="deg")

iiclose = np.argwhere(mockseps<thresh)
print "Close Mock Beams:"
for ii in iiclose:
    print "    %s (%f arcmin away)" % (mockcoords[ii]['fn'], mockseps[ii]*60)


wappcoords = np.loadtxt("PALFA_wapp_coords_table.txt", \
                        dtype=([("fn", np.str_, 255), ("raA", np.str_, 20), \
                                ("decA", np.str_, 20), ("raB", np.str_, 20), \
                                ("decB", np.str_, 20)]))
wappseps = np.empty(2*len(wappcoords))
for ii, (fn, raA, decA, raB, decB) in enumerate(wappcoords):
    wappseps[2*ii] = astro.sextant.angsep(raA, decA, ra0, dec0, \
                                    input="sexigesimal", output="deg")
    wappseps[2*ii+1] = astro.sextant.angsep(raB, decB, ra0, dec0, \
                                    input="sexigesimal", output="deg")

iiclose = np.argwhere(wappseps<thresh)
iisorted = iiclose[np.argsort(wappseps[iiclose])]
print "\nClose WAPP Beams:"
for ii in iiclose:
    if ii%2==0:
        # First beam listed
        print "    %s - A (%f arcmin away)" % (wappcoords[ii/2]['fn'], wappseps[ii]*60)
    else:
        # Second beam listed
        print "    %s - B (%f arcmin away)" % (wappcoords[ii/2]['fn'], wappseps[ii]*60)
