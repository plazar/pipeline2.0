MODE = 0

modes = [('jobtracker', 'Print SQL statements executed on job-tracker DB.'), \
            ('upload', 'Print timing summary for each successful upload.'), \
            ('commondb', 'Debug connections to the common DB.')]
modes.sort()

ALL = 0
for ii, (m, desc) in enumerate(modes):
    exec("%s = %d" % (m.upper(), 2**ii))
    ALL |= 2**ii

modes = [("all", "Turn on all debugging modes. (Same as -d/--debug).")] + modes


def set_mode_on(*modes):
    global MODE
    for m in modes:
        MODE |= m


def set_mode_off(*modes):
    global MODE
    for m in modes:
        MODE ^= m


def get_on_modes():
    global MODE
    on_modes = []
    for m, desc in modes:
        if m == 'all':
            continue
        if is_mode_on(eval('%s' % m.upper())):
            on_modes.append('debug.%s' % m.upper())
    return on_modes


def print_debug_status():
    on_modes = get_on_modes()
    print "The following debugging modes are turned on:"
    for m in on_modes:
        print "    %s" % m


def is_mode_on(m):
    global MODE
    return bool(m & MODE)
