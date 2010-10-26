#!/usr/bin/env python

"""
Script to upload candidate information to commonDB.

Argument 1: directory
Other arguments are ignored.

Patrick Lazarus, Sept. 12, 2010
"""

import os.path
import sys
import re
# import database

dmhit_re = re.compile(r'^ *DM= *(?P<dm>[^ ]*) *SNR= *(?P<snr>[^ ]*) *\** *$')
candinfo_re = re.compile(r'^(?P<accelfile>.*):(?P<candnum>\d*) *(?P<dm>[^ ]*)' \
                         r' *(?P<snr>[^ ]*) *(?P<sigma>[^ ]*) *(?P<numharm>[^ ]*)' \
                         r' *(?P<ipow>[^ ]*) *(?P<cpow>[^ ]*) *(?P<period>[^ ]*)' \
                         r' *(?P<r>[^ ]*) *(?P<z>[^ ]*) *\((?P<numhits>\d*)\)$')

def parse_candlist(candlistfn):
    """Parse candidate list and return a list of Candidate objects.
        
        Inputs:
            candlistfn - path of candlist
    
        Outputs:
            list of Candidates objects
    """
    candlist = open(candlistfn, 'r')
    cands = []
    for line in candlist:
        if not line.partition("#")[0].strip():
            # Ignore lines with no content
            continue
        dmhit_match = dmhit_re.match(line)
        candinfo_match = candinfo_re.match(line)
        if dmhit_match is not None:
            print dmhit_match.groupdict()
        elif candinfo_match is not None:
            print candinfo_match.groupdict()
        else:
            sys.stderr.write("Line has unrecognized format!\n(%s)\n" % line)
            sys.exit(1)

if __name__=='__main__':
    parse_candlist(sys.argv[1])
