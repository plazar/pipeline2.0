import urllib
import sys
sys.path.append('../lib/python/')
import config.download
from xml.dom.minidom import parseString

class Client():
    def __init__(self, test = False):
        self.username = config.download.api_username
        self.password = config.download.api_password
        self.bits = config.download.request_numbits
        self.fileType = config.download.request_datatype
        if test:
            self.restore_get_url = 'http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx/RestoreTest'
            self.location_get_url = 'http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx/LocationTest'
        else:
            self.restore_get_url = 'http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx/Restore'
            self.location_get_url = 'http://arecibo.tc.cornell.edu/palfadataapi/dataflow.asmx/Location'
        
    def Restore(self,num_beams):
        params = urllib.urlencode({'username':self.username,'pw':self.password,'number':num_beams,'bits': self.bits,'fileType':self.fileType})
        url_open = urllib.urlopen("%s?%s" % (self.restore_get_url, params))
        return self.parse_response(url_open.read())
    
    def Location(self,guid):
        params = urllib.urlencode({'username':self.username,'pw':self.password,'guid':guid})
        url_open = urllib.urlopen("%s?%s" % (self.location_get_url,params))
        return self.parse_response(url_open.read())
        
    def parse_response(self, data):
        dom = parseString(data)
        response = dom.getElementsByTagName('string')[0]
        return response.firstChild.data
