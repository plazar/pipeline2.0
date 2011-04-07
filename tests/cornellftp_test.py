import os
import os.path
import sys

import CornellFTP

C_FTP = CornellFTP.CornellFTP()

print "Attempting to connect, log in and list files on the Cornell FTP server"
print "Getting a list of files and sizes"
ftpfiles = C_FTP.get_files('ftpTest')
for fn, size in ftpfiles:
    print "%s (%d bytes)" % (fn, size)

ftpfn, ftpsize = ftpfiles[0]
print "Downloading a file"
localfn = C_FTP.download(os.path.join("ftpTest", ftpfn))

print "Checking file size"
localsize = os.path.getsize(localfn)
os.remove(localfn)
if localsize != ftpsize:
    raise ValueError("The download failed.\n" \
                     "The file sizes didn't match " \
                     "(local: %d, ftp: %d)" % (localsize, ftpsize))
else:
    print "File downloaded successfully!"
