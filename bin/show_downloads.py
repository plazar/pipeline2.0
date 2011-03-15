import os.path
import curses
import time

import jobtracker


def show_status(scr):
    active_downloads = jobtracker.query("SELECT * FROM files " \
                                        "WHERE status IN ('downloading', " \
                                                         "'unverified', " \
                                                         "'new') " \
                                        "ORDER BY created_at ASC")
    scr.clear()
    maxy, maxx = scr.getmaxyx()
    scr.addstr(0,0, "Number of active downloads: %d" % len(active_downloads), \
                curses.A_BOLD | curses.A_UNDERLINE)

    for ii, file in enumerate(active_downloads[:(maxy-2)/2-1]):
        fn = os.path.split(file['filename'])[-1]
        scr.addstr(2+ii*2, 0, fn, curses.A_BOLD)
        scr.addstr(2+ii*2, len(fn), " - %s" % file['status'])
        if os.path.exists(file['filename']):
            currsize = os.path.getsize(file['filename'])
        else:
            currsize = 0
        pcnt_complete = float(currsize)/file['size']
        scr.addstr(3+ii*2, 0, "[")
        scr.addstr(3+ii*2, 1, "="*int(pcnt_complete*(maxx-12))+">")
        scr.addstr(3+ii*2, maxx-10, "]")
        scr.addstr(3+ii*2, maxx-8, "(%5.1f%%)" % (pcnt_complete*100))

    scr.refresh()


def loop(scr):
    curses.curs_set(0)
    curses.use_default_colors()
    scr.scrollok(True)
    while True:
        show_status(scr)
        time.sleep(5)


def main():
    curses.wrapper(loop)


if __name__=='__main__':
    main()
