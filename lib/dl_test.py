import M2Crypto
import suds.client
from time import sleep

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect('arecibo.tc.cornell.edu', 31001)
myFtp.auth_tls()
myFtp.set_pasv(1)
print "Login Response: %s" % myFtp.login('palfadata', 'NAIC305m')

del(myFtp)

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect('arecibo.tc.cornell.edu', 31001)
myFtp.auth_tls()
myFtp.set_pasv(1)
print "Login Response: %s" %  myFtp.login('palfadata', 'NAIC305m')

WebService =  suds.client.Client("http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx?WSDL").service
print "WebService Response: %s" %  WebService.Location(username="mcgill",pw="palfa@Mc61!!", guid='e96ea139361740eca91f0f82ed4d889f')

myFtp = M2Crypto.ftpslib.FTP_TLS()
myFtp.connect('arecibo.tc.cornell.edu', 31001)
myFtp.auth_tls()
myFtp.set_pasv(1)
print "Login Response: %s" %  myFtp.login('palfadata', 'NAIC305m')