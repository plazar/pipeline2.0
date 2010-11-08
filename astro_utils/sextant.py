"""
sextant.py

A suite of useful coordinate related functions.

Patrick Lazarus, Jan. 31, 2010
"""
import warnings
import types
import numpy as np

import protractor
import calendar

def ha_from_lst(lst, ra):
    """Given local sidereal time (lst) and right ascension (ra),
        both in same units, (degrees, radians, or decimal hours)
        calculate the local hour angle.

        Return value is in same units as input arguments.
    """
    hourangle = lst - ra
    return hourangle


def ha_from_mjdlon(mjd, lon, ra):
    """Given Modified Julian Day (mjd), longitude (lon), right ascension (ra)
        where longitude, measured positive west and negative east from
        Greenwich, is given in degrees and right ascension is also given
        in degrees.

        Return value is in degrees.
    """
    mst_deg = clock.MJD_to_mstUT_deg(mjd)
    hourangle = mst_deg - lon - ra
    return hourangle


def equatorial_to_ecliptic(ra, decl, input="sexigesimal", output="deg", \
                                J2000=True):
    """Given right ascension and declination (provided in units of 'input')
        return ecliptic coordinates (longitude, latitude) in units of 'output'.

        If J2000 is true, assume input coords are in J2000 equinox,
            otherwise assume input coords are in B1950 equinox.
    
        Possible values for input and output are "sexigesimal", "deg" and "rad".
    """
    if J2000:
        obliquity = 0.409092804 # radians
    else:
        obliquity = 0.409206212 # radians

    # Convert equatorial coords to radians
    if input == "sexigesimal":
        ra = protractor.convert(ra, "hmsstr", "rad")
        decl = protractor.convert(decl, "dmsstr", "rad")
    else:
        ra = protractor.convert(ra, input, "rad")
        decl = protractor.convert(decl, input, "rad")

    # Do the conversion
    lon = np.arctan2(np.sin(ra)*np.cos(obliquity) + \
                        np.tan(decl)*np.sin(obliquity),
                        np.cos(ra))
    lat = np.arcsin(np.sin(decl)*np.cos(obliquity) - \
                        np.cos(decl)*np.sin(obliquity)*np.sin(ra))
    
    # Ensure radian values are between 0 and 2pi
    lon = np.mod(lon, np.pi*2)
    lat = np.mod(lat, np.pi*2)
    
    if output == "sexigesimal":
        output = "dmsstr"
    lon = protractor.convert(lon, "rad", output)
    lat = protractor.convert(lat, "rad", output)

    return (lon, lat)


def ecliptic_to_equatorial(lon, lat, input="deg", output="sexigesimal", \
                                J2000=True):
    """Given ecliptic longitude and latitude (provided in units of 'input')
        return equatorial coordinates (right ascension, declination)
        in units of 'output'.

        If J2000 is true, assume input coords are in J2000 equinox,
            otherwise assume input coords are in B1950 equinox.
    
        Possible values for input and output are "sexigesimal", "deg" and "rad".
    """
    if J2000:
        obliquity = 0.409092804 # radians
    else:
        obliquity = 0.409206212 # radians

    # Convert ecliptic coords to radians
    if input == "sexigesimal":
        input = "dmsstr"
    lon = protractor.convert(lon, input, "rad")
    lat = protractor.convert(lat, input, "rad")

    # Do the conversion
    ra = np.arctan2(np.sin(lon)*np.cos(obliquity) - \
                    np.tan(lat)*np.sin(obliquity), \
                    np.cos(lon))
    decl = np.arcsin(np.sin(lat)*np.cos(obliquity) + \
                    np.cos(lat)*np.sin(obliquity)*np.sin(lon))
   
    # Ensure radian values are between 0 and 2pi
    ra = np.mod(ra, np.pi*2)
    decl = np.mod(decl, np.pi*2)
    
    if output == "sexigesimal":
        ra = protractor.convert(ra, "rad", "hmsstr")
        decl = protractor.convert(decl, "rad", "dmsstr")
    else:
        ra = protractor.convert(ra, "rad", output)
        decl = protractor.convert(decl, "rad", output)

    return (ra, decl)


def hadec_to_altaz(ha, decl, obslat, input="sexigesimal", output="deg"):
    """Given hour angle, declination (provided in units of 'input') 
        and observer latitude (in degrees) return local horizontal coordinates
        (altitude and azimuth, in units of 'output').
    
        Possible values for input and output are "sexigesimal", "deg" and "rad".
    """
    # Convert equatorial coords to radians
    if input == "sexigesimal":
        ha = protractor.convert(ha, "hmsstr", "rad")
        decl = protractor.convert(decl, "dmsstr", "rad")
    else:
        ha = protractor.convert(ha, input, "rad")
        decl = protractor.convert(decl, input, "rad")

    # Do the conversion
    az = np.arctan2(np.sin(ha), np.cos(ha)*np.sin(obslat) - \
                        np.tan(decl)*np.cos(obslat))
    alt = np.arcsin(np.sin(obslat)*np.sin(decl) + \
                        np.cos(obslat)*np.cos(decl)*np.cos(ha))

    # Ensure radian values are between 0 and 2pi
    az = np.mod(az, np.pi*2)
    alt = np.mod(alt, np.pi*2)

    # Convert output values to desired units
    if output == "sexigesimal":
        output = "dmsstr"
    az = protractor.convert(az, "rad", output)
    alt = protractor.convert(alt, "rad", output)

    return (alt, az)


def altaz_to_hadec(alt, az, obslat, input="deg", output="sexigesimal"):
    """Given altitude, azimuth angle (provided in units of 'input')
        and observer latitude (in degrees) return hour angle and 
        declination (in units of 'output').

        Possible values for input and output are "sexigesimal", "deg" and "rad".
    """
    # Convert input args to radians
    if input == "sexigesimal":
        input = "dmsstr"
    alt = protractor.convert(alt, input, "rad")
    az = protractor.convert(az, input, "rad")
    
    # Do the conversion
    ha = np.arctan2(np.sin(az), np.cos(az)*np.sin(obslat) + \
                    np.tan(alt)*np.cos(obslat))
    decl = np.arcsin(np.sin(obslat)*np.sin(alt) - \
                    np.cos(obslat)*np.cos(alt)*np.cos(az))

    # Ensure radian values are between 0 and 2pi
    ha = np.mod(ha, np.pi*2)
    decl = np.mod(decl, np.pi*2)

    # Convert output values to desired units
    if output == "sexigesimal":
        ha = protractor.convert(ha, "rad", "hmsstr")
        decl = protractor.convert(decl, "rad", "dmsstr")
    else:
        ha = protractor.convert(ha, "rad", output)
        decl = protractor.convert(decl, "rad", output)

    return (ha, decl)


def equatorial_to_galactic(ra, decl, input="sexigesimal", output="deg", \
                            J2000=True):
    """Given right ascension and declination (in units of 'input') convert
        to galactic longitude and latitude (returned in units of 'output').

        Possible values for input and output are "sexigesimal", "deg" and "rad".
        If "J2000" is True then input equinox is J2000, otherwise input equinox is
        B1950.
    """
    # Convert equatorial coords to radians
    if input == "sexigesimal":
        ra = protractor.convert(ra, "hmsstr", "rad")
        decl = protractor.convert(decl, "dmsstr", "rad")
    else:
        ra = protractor.convert(ra, input, "rad")
        decl = protractor.convert(decl, input, "rad")

    # Conversion formula expects equatorial coords in B1950 equinox
    if J2000:
        ra, decl = precess_J2000_to_B1950(ra, decl, input='rad', output='rad')

    # Define galactic north pole
    ra_north = 3.35539549 # radians
    decl_north = 0.478220215 # radians
    
    # Do the conversion
    x = np.arctan2(np.sin(ra_north-ra), np.cos(ra_north-ra)*np.sin(decl_north) - \
                    np.tan(decl)*np.cos(decl_north))
    l = 5.28834763 - x # 303 deg = 5.28834763 rad (origin of galactic coords)
    b = np.arcsin(np.sin(decl)*np.sin(decl_north) + \
                    np.cos(decl)*np.cos(decl_north)*np.cos(ra_north-ra))

    # Ensure radian values are between 0 and 2pi
    l = np.mod(l, np.pi*2)
    b = np.mod(b, np.pi*2)

    if b > np.pi:
        b -= np.pi*2

    # Convert output values to desired units
    if output == "sexigesimal":
        output = "dmsstr"
    l = protractor.convert(l, "rad", output)
    b = protractor.convert(b, "rad", output)

    return (l, b)


def precess_B1950_to_J2000(ra, decl, input="sexigesimal", output="sexigesimal"):
    """Given right ascension and declination (in units of 'input') in B1950
        equinox, precess to J2000 equinox (returned in units of 'output').

        Possible values for input and output are "sexigesimal", "deg" and "rad".

        NOTE: Followed http://www.stargazing.net/kepler/b1950.html
    """
    # Convert equatorial coords to radians
    if input == "sexigesimal":
        ra = protractor.convert(ra, "hmsstr", "rad")
        decl = protractor.convert(decl, "dmsstr", "rad")
    else:
        ra = protractor.convert(ra, input, "rad")
        decl = protractor.convert(decl, input, "rad")

    # Convert to rectangular coords
    x = np.cos(ra) * np.cos(decl)
    y = np.sin(ra) * np.cos(decl)
    z = np.sin(decl)

    # Rotate vector
    x2 = 0.9999257080*x - 0.0111789372*y - 0.0048590035*z
    y2 = 0.0111789372*x + 0.9999375134*y - 0.0000271626*z
    z2 = 0.0048590036*x - 0.0000271579*y + 0.9999881946*z
   
    # Convert to equatorial
    ra2000 = np.arctan2(y2,x2)
    decl2000 = np.arcsin(z2)

    # Ensure radian values are between 0 and 2pi
    ra2000 = np.mod(ra2000, np.pi*2)
    decl2000 = np.mod(decl2000, np.pi*2)
   
    # Convert to desired units
    if output == "sexigesimal":
        ra2000 = protractor.convert(ra2000, "rad", "hmsstr")
        decl2000 = protractor.convert(decl2000, "rad", "dmsstr")
    else:
        ra2000 = protractor.convert(ra2000, "rad", output)
        decl2000 = protractor.convert(decl2000, "rad", output)

    return (ra2000, decl2000)


def precess_J2000_to_B1950(ra, decl, input="sexigesimal", output="sexigesimal"):
    """Given right ascension and declination (in units of 'input') in J2000
        equinox, precess to B1950 equinox (returned in units of 'output').

        Possible values for input and output are "sexigesimal", "deg" and "rad".
    """
    # Convert equatorial coords to radians
    if input == "sexigesimal":
        ra = protractor.convert(ra, "hmsstr", "rad")
        decl = protractor.convert(decl, "dmsstr", "rad")
    else:
        ra = protractor.convert(ra, input, "rad")
        decl = protractor.convert(decl, input, "rad")

    # Convert to rectangular coords
    x = np.cos(ra) * np.cos(decl)
    y = np.sin(ra) * np.cos(decl)
    z = np.sin(decl)

    # Rotate vector
    x2 =  0.9999257080*x + 0.0111789372*y + 0.0048590036*z
    y2 = -0.0111789372*x + 0.9999375134*y - 0.0000271579*z
    z2 = -0.0048590035*x - 0.0000271626*y + 0.9999881946*z
   
    # Convert to equatorial
    ra1950 = np.arctan2(y2,x2)
    decl1950 = np.arcsin(z2)

    # Ensure radian values are between 0 and 2pi
    ra1950 = np.mod(ra1950, np.pi*2)
    decl1950 = np.mod(decl1950, np.pi*2)
   
    # Convert to desired units
    if output == "sexigesimal":
        ra1950 = protractor.convert(ra1950, "rad", "hmsstr")
        decl1950 = protractor.convert(decl1950, "rad", "dmsstr")
    else:
        ra1950 = protractor.convert(ra1950, "rad", output)
        decl1950 = protractor.convert(decl1950, "rad", output)

    return (ra1950, decl1950)


def precess(ra, decl, inequinox, outequinox, \
                input="sexigesimal", output="sexigesimal"):
    """Given right ascension and declination (in unitls of 'input') in 'inequinox'
        equinox, precess to 'outequinox' equinox (returned in units of 'output').

        Possible values for input and output are "sexigesimal", "deg" and "rad".
        (Follow Jean Meeus' Astronomical Formulae For Calculators, 4th Ed., Ch 14
            Rigorous Method.)
    """
    warnings.warn("Results not exactly correct...")
    # Convert equatorial coords to radians
    if input == "sexigesimal":
        ra = protractor.convert(ra, "hmsstr", "rad")
        decl = protractor.convert(decl, "dmsstr", "rad")
    else:
        ra = protractor.convert(ra, input, "rad")
        decl = protractor.convert(decl, input, "rad")

    inJD = calendar.date_to_JD(inequinox, 0, 0, gregorian=True)
    outJD = calendar.date_to_JD(outequinox, 0, 0, gregorian=True)

    print inJD, outJD

    intau = (inJD - 2415020.313)/36524.2199
    outtau = (outJD - inJD)/36524.2199

    print intau, outtau

    # The following 3 variables are in arcseconds
    zeta = (2304.250 + 1.396*intau)*outtau + 0.302*outtau**2 + 0.018*outtau**3
    z = zeta + 0.791*outtau**2 + 0.001*outtau**3
    theta = (2004.682 - 0.853*intau)*outtau - 0.426*outtau**2 - 0.042*outtau**3

    print zeta, z, theta

    # Convert to radians
    zeta = zeta/3600*protractor.DEGTORAD
    z = z/3600*protractor.DEGTORAD
    theta = theta/3600*protractor.DEGTORAD

    A = np.cos(decl)*np.sin(ra+zeta)
    B = np.cos(theta)*np.cos(decl)*np.cos(ra+zeta)-np.sin(theta)*np.sin(decl)
    C = np.sin(theta)*np.cos(decl)*np.cos(ra+zeta)+np.cos(theta)*np.sin(decl)

    print A, B, C

    outra = np.arctan2(A, B)+z
    outdecl = np.arcsin(C)

    # Ensure radian values are between 0 and 2pi
    outra = np.mod(outra, np.pi*2)
    outdecl = np.mod(outdecl, np.pi*2)
   
    # Convert to desired units
    if output == "sexigesimal":
        outra = protractor.convert(outra, "rad", "hmsstr")
        outdecl = protractor.convert(outdecl, "rad", "dmsstr")
    else:
        outra = protractor.convert(outra, "rad", output)
        outdecl = protractor.convert(outdecl, "rad", output)

    return (outra, outdecl)
