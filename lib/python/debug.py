modes = [('jobtracker', 'Print SQL statements executed on job-tracker DB.'), \
            ('upload', 'Print timing summary for each successful upload.'), \
            ('download', 'Print extra information in downloader.'), \
            ('syscalls', 'Print commands being executed as system calls.'), \
            ('qmanager', 'Print extra information in queue manager.'), \
            ('commondb', 'Print SQL statements executed on common DB.')]
modes.sort()

# By default set all debug modes to False
for ii, (m, desc) in enumerate(modes):
    exec("%s = False" % m.upper())


def set_mode_on(*modes):
    for m in modes:
        exec "%s = True" % m.upper() in globals() 


def set_allmodes_on():
    for m, desc in modes:
        exec "%s = True" % m.upper() in globals() 


def set_allmodes_off():
    for m, desc in modes:
        exec "%s = False" % m.upper() in globals() 


def set_mode_off(*modes):
    for m in modes:
        exec "%s = False" % m.upper() in globals() 


def get_on_modes():
    on_modes = []
    for m, desc in modes:
        if eval('%s' % m.upper()):
            on_modes.append('debug.%s' % m.upper())
    return on_modes


def print_debug_status():
    on_modes = get_on_modes()
    print "The following debugging modes are turned on:"
    for m in on_modes:
        print "    %s" % m
