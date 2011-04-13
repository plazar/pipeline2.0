import M2Crypto
import sys
sys.path.append('../lib/python/')
import config.download
import urllib

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect(config.download.ftp_host, config.download.ftp_port)
myFtp.auth_tls()
myFtp.set_pasv(1)
response = myFtp.login(config.download.ftp_username, config.download.ftp_password)
print "Login Response: %s" % response

del(myFtp)

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect(config.download.ftp_host, config.download.ftp_port)
myFtp.auth_tls()
myFtp.set_pasv(1)
response = myFtp.login(config.download.ftp_username, config.download.ftp_password)
print "Login Response: %s" % response


params =  urllib.urlencode({'username':config.download.api_username,'pw':config.download.api_password,'guid':'ftpTest4BitMock'})
response = urllib.urlopen("http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx/Location?%s" % params )
print response.read()


del(myFtp)

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect(config.download.ftp_host, config.download.ftp_port)
myFtp.auth_tls()
myFtp.set_pasv(1)
response = myFtp.login(config.download.ftp_username, config.download.ftp_password)
print "Login Response: %s" % response
