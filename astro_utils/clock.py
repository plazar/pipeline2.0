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
    JD = calendar.MJD_to_JD(MJD)
    return JD_to_mstUT_deg(JD)


def JD_to_GMST(JD):
    """Given Julian Day (JD) return mean sidereal time at Greenwich
        in hours.

        Uses equations from http://www.usno.navy.mil/USNO/astronomical-applications/astronomical-information-center/approx-sider-time.
    """
    JD = np.asarray(JD)
    JD0 = (JD + 0.5).astype('int') - 0.5 # JD at previous midnight (0h)
    D = JD - 2451545.0
    D0 = JD0 - 2451545.0
    H = (JD - JD0)*24
    T = D/36525.0
    GMST = 6.697374558 + 0.06570982441908*D0 + 1.00273790935*H + 0.000026*T**2
    return GMST.astype('float') % 24.0


def MJD_to_GMST(MJD):
    """Given Modified Julian Day (MJD) return mean sidereal time at Greenwich
        in hours.
    """
    JD = calendar.MJD_to_JD(MJD)
    return JD_to_GMST(JD)


def JD_lon_to_LST(JD, lon):
    """Given Julian Day (JD) and longitude of observatory (in degrees) return
        the local mean sidereal time at the observatory in hours.

        Note: Positions West of Greenwich should be negative. 
              Positions East of Greenwish should be positive.
    """
    GMST = JD_to_GMST(JD)
    lon_hours = np.asarray(lon)/15.0

    lst = GMST + lon_hours
    lst = lst % 24.0
    return lst


def MJD_lon_to_LST(MJD, lon):
    """Given Modified Julian Day (MJD) and longitude of observatory 
        (in degrees) return the local mean sidereal time at the 
        observatory in hours.

        Note: Positions West of Greenwich should be negative. 
              Positions East of Greenwish should be positive.
    """
    JD = calendar.MJD_to_JD(MJD)
    return JD_lon_to_LST(JD, lon)
