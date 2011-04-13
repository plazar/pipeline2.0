import suds.client
import sys
sys.path.append('../lib/python/')
import config.download
from ftplib import FTP

myFtp = FTP('ftp.mozilla.org')
response = myFtp.login()
print "Login Response: %s" % response

del(myFtp)

myFtp = FTP('ftp.mozilla.org')
response = myFtp.login()
print "Login Response: %s" % response

print config.download.api_service_url
web_service =  suds.client.Client(config.download.api_service_url).service
response = web_service.Location(username=config.download.api_username, \
                                pw=config.download.api_password, \
                                guid='ftpTest4BitMock')
print "Web service Response: %s" % response

del(myFtp)

myFtp = FTP('ftp.mozilla.org')
response = myFtp.login()
print "Login Response: %s" % response

