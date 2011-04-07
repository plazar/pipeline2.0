import M2Crypto
import suds.client

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

web_service =  suds.client.Client(config.download.api_service_url).service
response = web_service.Location(username=config.download.api_username, \
                                pw=config.download.api_password, \
                                guid='ftpTest4BitMock')
print "Web service Response: %s" % response

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect(config.download.ftp_host, config.download.ftp_port)
myFtp.auth_tls()
myFtp.set_pasv(1)
response = myFtp.login(config.download.ftp_username, config.download.ftp_password)
print "Login Response: %s" % response
