import suds
import config.download

print "Connecting to web service."
web_service = suds.client.Client(config.download.api_service_url, cache=None).service

print "Getting test GUID using 'RestoreTest'."
guid = web_service.RestoreTest(username=config.download.api_username, \
                                pw=config.download.api_password, \
                                number=1, bits=4, fileType='wapp')
print "GUID: %s" % guid

print "Checking status of restore using 'LocationTest'."
response = web_service.LocationTest(guid=guid,
                                username=config.download.api_username, \
                                pw=config.download.api_password)
print "Response: %s" % response
