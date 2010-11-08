"""
protractor.py

A suite of useful angle related functions.

Patrick Lazarus, Jan 22nd, 2010
"""
import types
import re
import sys
import warnings
import numpy as np

DEGTORAD = np.pi/180.0
RADTODEG = 180.0/np.pi
HOURTORAD = np.pi/12.0
RADTOHOUR = 12.0/np.pi

hms_re = re.compile(r'^(?P<sign>[-+])?(?P<hour>\d{2}):(?P<min>\d{2})' \
                     r'(?::(?P<sec>\d{2}(?:.\d+)?))?$')
dms_re = re.compile(r'^(?P<sign>[-+])?(?P<deg>\d{2}):(?P<min>\d{2})' \
                     r'(?::(?P<sec>\d{2}(?:.\d+)?))?$')

def hmsstr_to_rad(hmsstr):
    """Convert HH:MM:SS.SS sexigesimal string to radians.
    """
    hmsstr = np.atleast_1d(hmsstr)
    hours = np.zeros(hmsstr.size)

    for i,s in enumerate(hmsstr):
        # parse string using regular expressions
        match = hms_re.match(s)
        if match is None:
            warnings.warn("Input is not a valid sexigesimal string: %s" % s)
            hours[i] = np.nan
            continue
        d = match.groupdict(0) # default value is 0

        # Check sign of hms string
        if d['sign'] == '-':
            sign = -1
        else:
            sign = 1
        
        hour = float(d['hour']) + \
                float(d['min'])/60.0 + \
                float(d['sec'])/3600.0

        hours[i] = sign*hour

    return hour_to_rad(hours)


def dmsstr_to_rad(dmsstr):
    """Convert DD:MM:SS.SS sexigesimal string to radians.
    """
    dmsstr = np.atleast_1d(dmsstr)
    degs = np.zeros(dmsstr.size)

    for i,s in enumerate(dmsstr):
        # parse string using regular expressions
        match = dms_re.match(s)
        if match is None:
            warnings.warn("Input is not a valid sexigesimal string: %s" % s)
            degs[i] = np.nan
            continue
        d = match.groupdict(0) # default value is 0

        # Check sign of dms string
        if d['sign'] == '-':
            sign = -1
        else:
            sign = 1
        
        deg = float(d['deg']) + \
                float(d['min'])/60.0 + \
                float(d['sec'])/3600.0

        degs[i] = deg

    degs = sign*degs
    return deg_to_rad(degs)


def rad_to_hmsstr(rads):
    """Convert radians to HH:MM:SS.SS sexigesimal string.
    """
    signs = np.atleast_1d(np.sign(rads))
    hours = np.atleast_1d(rad_to_hour(np.abs(rads)))
    strs = []
    for sign, hour in zip(signs, hours):
        # Add small value so results isn't affected by machine precision.
        hour += 1e-12 
        h = int(hour)
        min = (hour-h)*60.0
        m = int(min)
        s = (min-m)*60.0
        if sign == -1:
            sign = "-"
        else:
            sign = ""
        if (s >= 9.9995):
            strs.append("%s%.2d:%.2d:%.4f" % (sign, h, m, s))
        else:
            strs.append("%s%.2d:%.2d:0%.4f" % (sign, h, m, s))
    return strs
        

def rad_to_dmsstr(rads):
    """Convert radians to DD:MM:SS.SS sexigesimal string.
    """
    signs = np.atleast_1d(np.sign(rads))
    degs = np.atleast_1d(rad_to_deg(np.abs(rads)))
    strs = []
    for sign, deg in zip(signs, degs):
        # Add small value so results isn't affected by machine precision.
        deg += 1e-12 
        d = int(deg)
        min = (deg-d)*60.0
        m = int(min)
        s = (min-m)*60.0
        if sign == -1:
            sign = "-"
        else:
            sign = ""
        if (s >= 9.9995):
            strs.append("%s%.2d:%.2d:%.4f" % (sign, d, m, s))
        else:
            strs.append("%s%.2d:%.2d:0%.4f" % (sign, d, m, s))
    return strs
        

def hour_to_rad(hours):
    """Convert hours to radians.
    """
    hours = np.array(hours)
    return hours*HOURTORAD


def rad_to_hour(rads):
    """Convert hours to radians.
    """
    rads = np.array(rads)
    return rads*RADTOHOUR


def deg_to_rad(degs):
    """Convert degrees to radians.
    """
    degs = np.array(degs)
    return degs*DEGTORAD


def rad_to_deg(rads):
    """Convert radians to degrees.
    """
    rads = np.array(rads)
    return rads*RADTODEG


def rad_to_rad(rads):
    """Trivial do-nothing function.
        (This function is needed so convert function doesn't break.)
    """
    return rads


def convert(values, intype, outtype):
    """Convert values from 'intype' to 'outtype'.
    """
    # request two functions: 
    #    Conversion from 'in' to rad
    #    Conversion from rad to 'out'
    conv_in_to_rad = getfunction('%s_to_rad' % intype)
    conv_rad_to_out = getfunction('rad_to_%s' % outtype)
    
    return conv_rad_to_out(conv_in_to_rad(values))
    

def getfunction(reqfunc_name):
    """Request a function object by name.
        Return the function.
    """
    # check if requested function exists
    if reqfunc_name in globals():
        reqfunc = globals()[reqfunc_name]
        if (type(reqfunc) is types.FunctionType):
            pass
        else:
            raise ValueError("Requested conversion (%s) doesn't correspond " \
                                "to a function! type is %s." % \
                                (reqfunc_name, type(reqfunc)))
    else:
        raise ValueError("Requested conversion (%s) doesn't exist!" % reqfunc_name)

    return reqfunc
