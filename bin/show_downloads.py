import os.path
import curses
import time

import jobtracker


class File:
    def __init__(self, name, size, status, update_time):
        self.name = name
        self.size = size
        self.status = status
        self.currsize = 0
        self.currtime = time.time()
        self.update()
        self.oldsize = self.currsize
        self.oldtime = self.currtime
        if self.status == 'downloading':
            t = time.strptime(update_time, '%Y-%m-%d %H:%M:%S')
            self.starttime = time.mktime(t)
        else:
            self.starttime = None

    def update(self):
        if os.path.exists(self.name):
            newsize = os.path.getsize(self.name)
        else:
            newsize = 0
        if newsize != self.currsize:
            self.oldsize = self.currsize
            self.oldtime = self.currtime
            self.currsize = newsize
        self.currtime = time.time()

    def get_progressbar(self, numchars):
        pcnt_complete = float(self.currsize)/self.size
        progbar = "[" + "="*int(round(pcnt_complete*(numchars-12)))+">" + \
                    " "*int(round((1-pcnt_complete)*(numchars-12))) + "]" + \
                    " (%5.1f%%)" % (pcnt_complete*100)
        return progbar
    
    def get_download_info(self, numchars):
        info = "File Size: %d MB" % (self.size/1024.0**2)
        if self.status == 'downloading':
            info += " - Amt. Downloaded: %d MB" % (self.currsize/1024.0**2) # in MB
            delta_size = (self.currsize - self.oldsize)/1024.0**2 # in MB
            delta_time = self.currtime - self.oldtime # in seconds
            if delta_time == 0:
                info += " - Rate: ?? MB/s"
            else:
                rate = delta_size/delta_time
                info += " - Rate: %.2f MB/s" % rate
        elif self.status == 'unverified':
            if self.starttime is None:
                info += " - Avg. Rate: ?? MB/s - Total Time: ?? s"
            else:
                rate = self.size/(self.oldtime - self.starttime)/1024.0**2 # in MB/s
                info += " - Avg. Rate: %.2f MB/s - Total Time: %d s" % \
                        (rate, round(self.oldtime-self.starttime))
        return info
    
    def __cmp__(self, other):
        status_mapping = {'downloading': 1, 'unverified': 2, 'new':0}
        return cmp(status_mapping[self.status], status_mapping[other.status])


class FileList(list):
    def __init__(self):
        super(FileList, self).__init__()

    def update(self):
        active_downloads = jobtracker.query("SELECT * FROM files " \
                                            "WHERE status IN ('downloading', " \
                                                             "'unverified', " \
                                                             "'new') " \
                                            "ORDER BY created_at ASC")
        for dl in active_downloads:
            found = False
            for f in self:
                if dl['filename'] == f.name:
                    f.status=dl['status']
                    found = True
            if not found:
                self.append(File(dl['filename'], dl['size'], dl['status'], dl['updated_at']))

        for ii, f in enumerate(self):
            found = False
            for dl in active_downloads:
                if dl['filename'] == f.name:
                    found = True
                    if dl['status'] == 'downloading':
                        t = time.strptime(dl['updated_at'], '%Y-%m-%d %H:%M:%S')
                        f.starttime = time.mktime(t)
            if not found:
                self.pop(ii)
            else:
                f.update()
       

def show_status(scr):
    scr.clear()
    maxy, maxx = scr.getmaxyx()
    scr.addstr(0,0, "Number of active downloads: %d" % len(files), \
                curses.A_BOLD | curses.A_UNDERLINE)

    for ii, file in enumerate(files[:(maxy-2)/3-1]):
        fn = os.path.split(file.name)[-1]
        scr.addstr(2+ii*3, 0, fn, curses.A_BOLD)
        scr.addstr(2+ii*3, len(fn), " - %s" % file.status)
        scr.addstr(3+ii*3, 0, file.get_download_info(maxx))
        scr.addstr(4+ii*3, 0, file.get_progressbar(maxx))
    scr.refresh()


def loop(scr):
    curses.curs_set(0)
    curses.use_default_colors()
    scr.scrollok(True)
    while True:
        files.update()
        files.sort(reverse=True)
        show_status(scr)
        time.sleep(1)


def main():
    global files
    files = FileList()
    curses.wrapper(loop)


if __name__=='__main__':
    main()
