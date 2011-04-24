"""
This script will test-import many modules used by the pipeline.

Error messages will be output for modules that are not imported properly.
"""

BUILTIN = ["datetime", \
           "os", \
           "os.path", \
           "sys", \
           "optparse", \
           "shutil", \
           "socket", \
           "subprocess", \
           "tempfile", \
           "curses", \
           "time", \
           "traceback", \
           "warnings", \
           "types", \
           "re", \
           "glob", \
           "tarfile", \
           "threading", \
           "sqlite3", \
           "email", \
           "smtplib", \
           "logging", \
           "socket", \
           "struct", \
           "atexit", \
           "random", \
           "urllib", \
           "xml.dom.minidom"]

THIRDPARTY = [("presto", "Part of PRESTO - Available at: https://github.com/scottransom/presto"), \
              ("PBSQuery", "Only needed if using PBS queue manager - Available at: http://subtrac.sara.nl/oss/pbs_python/wiki/TorqueInstallation"), \
              ("matplotlib", "Available at: http://matplotlib.sourceforge.net/"), \
              ("matplotlib.pyplot", "Part of matplotlib - Available at: http://matplotlib.sourceforge.net/"), \
              ("numpy", "Available at: http://numpy.scipy.org/"), \
              ("pyodbc", "Available at: http://code.google.com/p/pyodbc/"), \
              ("pyfits", "Available at: http://www.stsci.edu/resources/software_hardware/pyfits"), \
              ("prepfold", "Part of PRESTO - Available at: https://github.com/scottransom/presto"), \
              ("psr_utils", "Part of PRESTO - Available at: https://github.com/scottransom/presto"), \
              ("M2Crypto", "Available at: http://chandlerproject.org/Projects/MeTooCrypto"), \
              ("prettytable", "Available at: http://code.google.com/p/prettytable/"), \
              ("sifting", "Part of PRESTO - Available at: https://github.com/scottransom/presto")]

errors = []

print "Checking python built-ins:"
for m in BUILTIN:
    print "    %s" % m
    try:
        __import__(m)
    except Exception, e:
        errors.append("Error importing %s - %s: %s" \
                "\n    The module '%s' is a python built-in. " \
                "Check your version and/or install." % (m, type(e), str(e), m))

print "\nChecking third-party modules:"
for m, note in THIRDPARTY:
    print "    %s" % m
    try:
        __import__(m)
    except Exception, e:
        errors.append("Error importing %s - %s: %s\n    %s" % \
                        (m, type(e), str(e), note))


if errors:
    print "\nNumber of errors: %d" % len(errors)
    print "============== ERRORS =============="
    for err in errors:
        print err
else:
    print "\nAll imports successful."
