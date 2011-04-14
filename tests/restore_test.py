import CornellWebservice
import config.download

print "Connecting to web service."
web_service = CornellWebservice.Client()

# Define test cases. 
# Each tuple contains arguments for (bits, fileType) options.
TESTCASES = [(4, 'wapp'), \
         (16, 'wapp'), \
         (4, 'mock'), \
         (16, 'mock')]

for bits, ftype in TESTCASES:
    print "Getting test GUID using 'RestoreTest' " \
            "with bits=%d and fileType='%s'." % (bits, ftype)
    guid = web_service.RestoreTest(username=config.download.api_username, \
                                    pw=config.download.api_password, \
                                    number=1, bits=bits, fileType=ftype)
    print "GUID: %s" % guid

    print "Checking status of restore using 'LocationTest'."
    response = web_service.LocationTest(guid=guid,
                                    username=config.download.api_username, \
                                    pw=config.download.api_password)
    print "Response: %s\n" % response
