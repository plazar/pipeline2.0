"""
calendar.py

A suite of useful calendar/date/day related functions.

Patrick Lazarus, Nov 4th, 2009
"""
import types
import numpy as np


MONTH_TO_NUM = {'January': 1,
                'February': 2,
                'March': 3,
                'April': 4,
                'May': 5,
                'June': 6,
                'July': 7,
                'August': 8,
                'September': 9,
                'October': 10,
                'November': 11,
                'December': 12}

NUM_TO_MONTH = {1: 'January',
                2: 'February',
                3: 'March',
                4: 'April',
                5: 'May',
                6: 'June',
                7: 'July',
                8: 'August',
                9: 'September',
                10: 'October',
                11: 'November',
                12: 'December'}

DAY_TO_NUM = {'Sunday': 0,
              'Monday': 1,
              'Tuesday': 2,
              'Wednesday': 3,
              'Thursday': 4,
              'Friday': 5,
              'Saturday': 6}

NUM_TO_DAY = {0: 'Sunday',
              1: 'Monday',
              2: 'Tuesday',
              3: 'Wednesday',
              4: 'Thursday', 
              5: 'Friday',
              6: 'Saturday'}


def JD_to_MJD(JD):
    """Convert Julian Day (JD) to Modified Julian Day (MJD).
    """
    return JD - 2400000.5


def MJD_to_JD(MJD):
    """Convert Modified Julian Day (MJD) to Julian Day (JD).
    """
    return MJD + 2400000.5


def date_to_MJD(*args, **kwargs):
    """Convert calendar date to Modified Julian Day (JD).
        
        All arguments are passed directly to "date_to_JD(...)",
        so consult its documentation.
    """
    return JD_to_MJD(date_to_JD(*args, **kwargs))


def date_to_JD(year, month, day, gregorian=True):
    """Convert calendar date to Julian Day (JD).

        Inputs:
            year: integer
            month:  integer or a string
            day: float
            gregorian:  - True for Gregorian calendar (Default)
                        - False for Julian calendar
    
        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    if type(month) == types.StringType:
        month = month_to_num(month)
    
    year = np.atleast_1d(year)
    month = np.atleast_1d(month)
    day = np.atleast_1d(day)
    
    year[month<=2] -= 1
    month[month<=2] += 12
    
    A = np.floor(year/100.0)
    
    if gregorian:
        B = 2 - A + np.floor(A/4.0)
    else:
        B = 0

    JD = np.floor(365.25*(year+4716)) + np.floor(30.6001*(month+1)) + \
            day + B - 1524.5

    if np.any(JD<0.0):
        raise ValueError("This function does not apply for JD < 0.")
    
    return JD.squeeze()


def julian_to_JD(year, month, day):
    """Convert Julian date to Julian Day (JD).

        Inputs:
            year: integer
            month:  integer or a string
            day: float
    
        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    return date_to_JD(year, month, day, gregorian=False)


def gregorian_to_JD(year, month, day):
    """Convert Gregorian date to Julian Day (JD).

        Inputs:
            year: integer
            month:  integer or a string
            day: float
    
        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    return date_to_JD(year, month, day, gregorian=True)


def gregorian_to_MJD(year, month, day):
    """Convert Gregorian date to Modified Julian Day (MJD).

        Inputs:
            year: integer
            month:  integer or a string
            day: float
    
        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    JD = date_to_JD(year, month, day, gregorian=True)
    return JD_to_MJD(JD)
    

def julian_to_MJD(year, month, day):
    """Convert Julian date to Modified Julian Day (MJD).

        Inputs:
            year: integer
            month:  integer or a string
            day: float
    
        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    JD = date_to_JD(year, month, day, gregorian=False)
    return JD_to_MJD(JD)


def JD_to_date(JD):
    """Convert Julian Day (JD) to a date.
        
        Input:
            JD: Julian day

        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    JD = np.atleast_1d(JD)
    
    if np.any(JD<0.0):
        raise ValueError("This function does not apply for JD < 0.")

    JD += 0.5

    # Z is integer part of JD
    Z = np.floor(JD)
    # F is fractional part of JD
    F = np.mod(JD, 1)

    A = np.copy(Z)
    alpha = np.floor((Z-1867216.25)/36524.25)
    A[Z>=2299161] = Z + 1 + alpha - np.floor(0.25*alpha)

    B = A + 1524
    C = np.floor((B-122.1)/365.25)
    D = np.floor(365.25*C)
    E = np.floor((B-D)/30.6001)

    day = B - D - np.floor(30.6001*E) + F
    month = E - 1
    month[(E==14.0) | (E==15.0)] = E - 13
    year = C - 4716
    year[(month==1.0) | (month==2.0)] = C - 4715

    return (year.astype('int').squeeze(), month.astype('int').squeeze(), \
                day.squeeze())


def MJD_to_date(MJD):
    """Convert Modified Julian Day (MJD) to a date.
        
        Input:
            MJD: Modified Julian day

        (Follow Jean Meeus' Astronomical Algorithms, 2nd Ed., Ch. 7)
    """
    JD = MJD_to_JD(MJD)
    return JD_to_date(JD)


def is_leap_year(year, gregorian=True):
    """Return True if year is a leap year.
        
        Inputs:
            year: integer
            gregorian:  - True for Gregorian calendar (Default)
                        - False for Julian calendar
    """
    year = np.atleast_1d(year)
    leap = (year%4)==0
    if gregorian:
        leap = np.bitwise_and(leap, np.bitwise_or((year%400)==0, (year%100)!=0))
    return leap.squeeze()


def is_gregorian_leap_year(year):
    """Return True if year is a leap year.
        
        Inputs:
            year: integer
    """
    return is_leap_year(year, gregorian=True)


def is_julian_leap_year(year):
    """Return True if year is a leap year.
        
        Inputs:
            year: integer
    """
    return is_leap_year(year, gregorian=False)


def first_of_year_JD(year):
    """Return Julian Day (JD) corresponding to January 0.0 of year.
        (This is the same as December 31.0 of year-1.)

        Inputs:
            year: integer
    """
    Y = np.floor(year)-1
    A = np.floor(0.01*Y)
    JD_0 = np.floor(365.25*Y) - A + np.floor(0.25*A) + 1721424.5

    return JD_0.squeeze()


def first_of_year_MJD(year):
    """Return Modified Julian Day (MJD) corresponding to January 0.0 of year.
        (This is the same as December 31.0 of year-1.)

        Inputs:
            year: integer
    """
    JD_0 = first_of_year_JD(year)
    return JD_to_MJD(JD_0)


def day_of_year(year, month, day, gregorian=True):
    """Return day of year given month and day.
        
        Inputs:
            year: integer
            month: integer
            day: float
            gregorian:  - True for Gregorian calendar (Default)
                        - False for Julian calendar
        
        Notes:
            'year' and 'gregorian' are used to determine if it is
            a leap year.
    """
    year = np.atleast_1d(year)
    month = np.atleast_1d(month)
    day = np.atleast_1d(day)
   
    leaps = np.atleast_1d(is_leap_year(year, gregorian))
   
    K = 2*np.ones_like(year)
    K[leaps] = 1

    N = np.floor(275.0*month/9.0) - K * np.floor((month+9)/12.0) + day - 30
    return N.squeeze()


def day_of_week(year, month, day):
    """Return day of week given a date.

        Input:
            year: integer
            month: integer
            day: float

        Note:
            'gregorian' is not needed because day of week was not
            changed when adopting Gregorian Calendar.
    """
    JD = gregorian_to_JD(year, month, np.floor(day)) + 1.5
    return np.mod(JD, 7)
    
    
def month_to_num(month):
    """Return month number given the month name, a string.
    """
    if not hasattr(month, '__iter__'):
        month = [month]
    nums = []
    for m in month:
        if type(m) != types.StringType:
            raise TypeError("month must be of type string. type(month): %s" % \
                                type(m))
        if m not in MONTH_TO_NUM:
            raise ValueError("Unrecognized month name: %s" % m)
        nums.append(MONTH_TO_NUM[month])
    if len(nums) == 1:
        nums = nums[0]
    return nums


def num_to_month(month):
    """Return month name, a string, given the month number.
    """
    if not hasattr(month, '__iter__'):
        month = [month]
    strings = []
    for m in month:
        if type(m) != types.IntType and type(m) != np.int32:
            raise TypeError("month must be of type integer. type(month): %s" % \
                                type(m))
        if m not in NUM_TO_MONTH:
            raise ValueError("Unrecognized month number: %d" % m)
        strings.append(NUM_TO_MONTH[m])
    if len(strings) == 1:
        strings = strings[0]
    return strings


def date_to_string(year, month, day):
    """Return a string of the date given.
        
        Inputs:
            year: integer
            month: integer
            day: integer

        Output format: 
            Month day, year

        Notes:
            Ignore fractional part of day.
    """
    year = np.atleast_1d(year)
    month = np.atleast_1d(month)
    day = np.atleast_1d(day)
   
    month = num_to_month(month)
    if not hasattr(month, '__iter__'):
        month = [month]
   
    date_strings = []
    for y, m, d in zip(year, month, day):
        date_strings.append("%s %d, %d" % (m, d, y))
    
    if len(date_strings) == 1:
        date_strings = date_strings[0]
    return date_strings


def interval_in_days(year1, month1, day1, year2, month2, day2, gregorian=True):
    """Compute difference between two dates in days.
        Return date2 - date1

        Inputs:
            year1: integer
            month1: integer
            day1: float
            year2: integer
            month2: integer
            day2: float
            gregorian:  - True for Gregorian calendar (Default)
                        - False for Julian calendar
    """
    JD1 = date_to_JD(year1, month1, day1, gregorian)
    JD2 = date_to_JD(year2, month2, day2, gregorian)

    diff = JD2 - JD1
    return diff.squeeze()
   

def fraction_of_year(year, month, day, gregorian=True):
    """Compute fraction of year elapsed.
        
        Inputs:
            year: integer
            month: integer
            day: float
            gregorian:  - True for Gregorian calendar (Default)
                        - False for Julian calendar
    """
    JD_0 = first_of_year_JD(year) + 1
    JD = date_to_JD(year, month, day, gregorian)
    diff = JD - JD_0
    numdays = np.atleast_1d(365*np.ones_like(diff))
    leaps = np.atleast_1d(is_leap_year(year, gregorian))
    numdays[leaps] += 1.0
    frac = diff/numdays
    return frac.squeeze()

def MJD_to_year(MJD):
    """Given a MJD return the corresponding (fractional) year.

        Inputs:
            MJD
    """
    year, month, day = MJD_to_date(MJD) 
    return year + fraction_of_year(year, month, day)


def MJD_to_datestring(MJD):
    """Given a MJD return the date formatted as a string.
        
        Inputs:
            MJD
    """
    return date_to_string(*MJD_to_date(MJD))
