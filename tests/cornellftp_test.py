#!/usr/bin/env python

import os
import os.path
import sys
import time

import CornellFTP
import config.basic

C_FTP = CornellFTP.CornellFTP()

print "Attempting to connect, log in and list files on the Cornell FTP server"
print "Getting a list of files and sizes"
ftpfiles = C_FTP.get_files('ftpTest')
for fn, size in ftpfiles:
    print "%s (%d bytes)" % (fn, size)

ftpfn, ftpsize = ftpfiles[0]

print "Downloading a file"
start = time.time()
localfn = C_FTP.download(os.path.join("ftpTest", ftpfn))
dltime = time.time()-start

print "Checking file size"
localsize = os.path.getsize(localfn)
if localsize != ftpsize:
    raise ValueError("The download failed.\n" \
                     "The file sizes didn't match " \
                     "(local: %d, ftp: %d)" % (localsize, ftpsize))
else:
    dlrate = localsize/1024.0**2/dltime
    print "File downloaded successfully! Download rate %.2f MB/s" % dlrate

print "Uploading a file"
ftp_path = "singlePulse/ftpTest/%s_%d.test" % \
            (config.basic.institution, time.time())
start = time.time()
C_FTP.upload(localfn, ftp_path)
uptime = time.time()-start

uprate = localsize/1024.0**2/uptime
print "File uploaded successfully! Upload rate %.2f MB/s" % uprate

C_FTP.delete(ftp_path)

os.remove(localfn)
