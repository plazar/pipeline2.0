"""
clock.py

A suite of useful clock/time related functions.

Patrick Lazarus, Nov 20th, 2009
"""
import types
import numpy as np

import calendar

def JD_to_mstUT_deg(JD):
    """Given Julian Day (JD) return mean sidereal time (UT)
        in degrees.
    """
    JD = np.array(JD)
    T = (JD - np.array(2451545.0))/np.array(36525.0)
    print T
    mst_deg = np.array(280.46061837) + \
                np.array(360.98564736629)*(JD - np.array(2451545.0)) + \
                np.array(0.000387933)*np.power(T,2) - \
                np.power(T,3)/np.array(38710000.0)
    return mst_deg


def MJD_to_mstUT_deg(MJD):
    """Given Modified Julian Day (MJD) return mean sidereal time (UT)
        in degrees.
    """
    JD = MJD_to_JD(MJD)
    return JD_to_mstUT_deg(JD)
