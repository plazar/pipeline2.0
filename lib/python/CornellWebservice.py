import urllib
import xml.dom.minidom
import config.download

class Client():
    def __getattr__(self, key):
        return lambda **kwargs: self.use_service(key, **kwargs)

    def use_service(self, name, **kwargs):
        """Use the client's web service 'name' with the given keyword arguments.
        
            Inputs:
                name: Name of the web service to use (this is case sensitive).
                Additional keyword arguments are passed to the web service.

            Output:
                response: The response of the webservice, a string.
        """
        params = urllib.urlencode(kwargs)
        url_open = urllib.urlopen("%s/%s?%s" % (config.download.api_service_url, \
                                                name, params))
        response = url_open.read()
        # print response
        return self.parse_response(response)

    def parse_response(self, data):
        dom = xml.dom.minidom.parseString(data)
        response = dom.getElementsByTagName('string')[0]
        return response.firstChild.data
