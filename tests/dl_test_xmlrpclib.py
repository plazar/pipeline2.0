import M2Crypto
import xmlrpclib
import sys
sys.path.append('../lib/python/')
import config.download

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

web_service =  xmlrpclib.ServerProxy(config.download.api_service_url)
multicall  = xmlrpclib.MultiCall(web_service)
print web_service.system

response = multicall.Location(username=config.download.api_username, \
                                pw=config.download.api_password, \
                                guid='ftpTest4BitMock')


print "XMLRPCLIB: Web service Response: %s" % response

del(myFtp)

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect(config.download.ftp_host, config.download.ftp_port)
myFtp.auth_tls()
myFtp.set_pasv(1)
response = myFtp.login(config.download.ftp_username, config.download.ftp_password)
print "Login Response: %s" % response
