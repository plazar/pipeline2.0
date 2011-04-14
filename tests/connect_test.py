import M2Crypto
import config.download
import CornellWebservice

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


web_service = CornellWebservice.Client()
guid = web_service.RestoreTest(username=config.download.api_username, \
                                pw=config.download.api_password, \
                                number=1, bits=config.download.request_numbits, \
                                fileType=config.download.request_datatype)


del(myFtp)

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect(config.download.ftp_host, config.download.ftp_port)
myFtp.auth_tls()
myFtp.set_pasv(1)
response = myFtp.login(config.download.ftp_username, config.download.ftp_password)
print "Login Response: %s" % response
